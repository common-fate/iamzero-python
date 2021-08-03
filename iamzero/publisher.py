from iamzero.logging import configure_root_logger
from iamzero.config import Config
from iamzero.identity import Identity
from typing import Any, List, Tuple
from iamzero.event import Event

from abc import ABC, abstractmethod
import queue
from urllib.parse import urljoin
import gzip
import io
import json
import threading
import requests
import statsd
import time
import boto3
from uuid import uuid4

from iamzero.version import VERSION

logger = configure_root_logger(__name__)


class NoIdentityException(Exception):
    """
    Raised when messages are attempted to be dispatched
    without an associated identity
    """


class Transport(ABC):
    """
    A class which handles the actual dispatch of messages to the IAM Zero
    server.

    We support different modes of transport, such as using a public HTTPS
    endpoint or dispatching events via AWS SQS.
    """

    @abstractmethod
    def send(self, payload: list) -> Tuple[int, Any]:
        """
        Send a message.
        Returns a tuple containing the status code and the response
        """
        pass


class HTTPTransport(Transport):
    def __init__(
        self,
        config: Config,
        gzip_enabled: bool = False,
        gzip_compression_level: int = 1,
        proxies={},
    ) -> None:
        self.config = config
        self.token = config["token"]
        self.url = config["url"]
        self.gzip_enabled = gzip_enabled
        self.gzip_compression_level = gzip_compression_level

        user_agent = "iamzero-python/" + VERSION
        if self.config["user_agent_addition"]:
            user_agent += " " + self.config["user_agent_addition"]

        session = requests.Session()
        session.headers.update({"User-Agent": user_agent})
        if self.gzip_enabled:
            session.headers.update({"Content-Encoding": "gzip"})
        if proxies:
            session.proxies.update(proxies)
        self.session = session

    def send(self, payload: list) -> Tuple[int, Any]:
        url = urljoin(self.url, "api/v1/events/")
        data = json.dumps(payload)
        if self.gzip_enabled:
            # The gzip lib works with file-like objects so we use a buffered byte stream
            stream = io.BytesIO()
            compressor = gzip.GzipFile(
                fileobj=stream, mode="wb", compresslevel=self.gzip_compression_level
            )
            compressor.write(data.encode())
            compressor.close()
            data = stream.getvalue()
            stream.close()
        resp = self.session.post(
            url,
            headers={
                "x-iamzero-token": self.token,
                "Content-Type": "application/json",
            },
            data=data,
            timeout=10.0,
        )
        resp.raise_for_status()
        return (resp.status_code, resp.json())


class SQSTransport(Transport):
    def __init__(
        self,
        config: Config,
    ) -> None:
        self.sqs_queue_url = config["transport_sqs_queue_url"]
        self.token = config["token"]

        user_agent = "iamzero-python/" + VERSION
        if config["user_agent_addition"]:
            user_agent += " " + config["user_agent_addition"]

        self.user_agent = user_agent

        if config["transport_custom_aws_session"] is not None:
            session = config["transport_custom_aws_session"]
        else:
            session = boto3.Session()

        self.sqs = session.client("sqs")

    def send(self, payload: list) -> Tuple[int, Any]:
        entries = []

        for event in payload:
            formatted_event = {
                "Id": str(uuid4()),
                "MessageBody": json.dumps(event),
                "MessageAttributes": {
                    "User-Agent": {
                        "DataType": "String",
                        "StringValue": self.user_agent,
                    },
                },
            }
            if self.token is not None:
                formatted_event["MessageAttributes"]["x-iamzero-token"] = {
                    "DataType": "String",
                    "StringValue": self.token,
                }
            entries.append(formatted_event)

        result = self.sqs.send_message_batch(
            QueueUrl=self.sqs_queue_url, Entries=entries
        )

        status_code = result["ResponseMetadata"]["HTTPStatusCode"]
        return (status_code, result)


class Publisher:
    def __init__(
        self,
        config: Config,
        block_on_send=False,
        block_on_response=False,
        identity=None,
        gzip_enabled=False,
        gzip_compression_level=1,
        proxies={},
    ):
        self.config = config
        self.token = config["token"]
        self.block_on_send = block_on_send
        self.block_on_response = block_on_response
        self.max_batch_size = config["max_batch_size"]
        self.send_frequency = config["send_frequency"]
        self.identity: Identity = identity
        self.transport_type = config["transport"]

        if config["transport"] == "sqs":
            self.transport: Transport = SQSTransport(config=config)
        else:
            # default to HTTP transport
            self.transport: Transport = HTTPTransport(
                config=config,
                gzip_enabled=gzip_enabled,
                gzip_compression_level=gzip_compression_level,
                proxies=proxies,
            )

        if self.identity is None:
            raise Exception("Identity must be provided")

        # pending events queue
        self.pending: queue.Queue[Event] = queue.Queue(maxsize=1000)
        # API responses queue
        self.responses = queue.Queue(maxsize=2000)

        self._sending_thread = None
        self.sd = statsd.StatsClient(prefix="iamzero")

        self.debug = self.config["debug"]

    def log(self, msg, *args, **kwargs):
        if self.debug:
            logger.debug(msg, *args, **kwargs)

    def start(self):
        self._sending_thread = threading.Thread(target=self._sender)
        self._sending_thread.daemon = True
        self._sending_thread.start()
        self.log("started sending thread")

    def send(self, ev: Event):
        """send accepts an event and queues it to be sent"""
        self.sd.gauge("queue_length", self.pending.qsize())
        try:
            if self.block_on_send:
                self.pending.put(ev)
            else:
                self.pending.put_nowait(ev)
            self.sd.incr("messages_queued")
        except queue.Full:
            response = {
                "status_code": 0,
                "duration": 0,
                "metadata": ev.metadata,
                "body": "",
                "error": "event dropped; queue overflow",
            }
            if self.block_on_response:
                self.responses.put(response)
            else:
                try:
                    self.responses.put_nowait(response)
                except queue.Full:
                    # if the response queue is full when trying to add an event
                    # queue is full response, just skip it.
                    pass
            self.sd.incr("queue_overflow")

    def _sender(self):
        """_sender is the control loop that pulls events off the `self.pending`
        queue and submits batches for actual sending."""
        events: List[Event] = []
        last_flush = time.time()

        # wait until identity is established before dispatching
        # events, so that the role can be included in events

        # TODO: how does this work with multiple identities if assume role
        # functionality is used?
        with self.identity.initialized:
            self.identity.initialized.wait(timeout=5.0)

        if self.identity.error is not None:
            self.log(
                "error occurred while fetching identity, err=", self.identity.error
            )

        while True:
            try:
                ev = self.pending.get(timeout=self.send_frequency)
                if ev is None:
                    # signals shutdown
                    self._flush(events)
                    return
                events.append(ev)
                if (
                    len(events) > self.max_batch_size
                    or time.time() - last_flush > self.send_frequency
                ):
                    self._flush(events)
                    events = []
                    last_flush = time.time()
            except queue.Empty:
                self._flush(events)
                events = []
                last_flush = time.time()

    def _flush(self, events: List[Event]):
        if not events:
            return
        self._send_batch(events)

    def _send_batch(self, events: List[Event]):
        """Makes a single batch API request with the given list of events. The
        `destination` argument contains the write key, API host and dataset
        name used to build the request."""
        start = time.time()
        status_code = 0

        if self.identity.role is None:
            err = NoIdentityException(
                "IAM Zero was unable to determine your AWS identity. Please ensure that you are running your application with valid AWS credentials."
            )
            # log as an error in the application we are instrumenting
            if not self.config["quiet"]:
                logger.error(err)
            self._enqueue_errors(status_code, err, start, events)
            return

        try:

            payload = []
            for ev in events:
                event_time = ev.created_at.isoformat()
                if ev.created_at.tzinfo is None:
                    event_time += "Z"
                payload.append(
                    {
                        "time": event_time,
                        "data": ev.data,
                        "identity": {
                            "user": self.identity.user,
                            "role": self.identity.role,
                            "account": self.identity.account,
                        },
                    }
                )

            status_code, response = self.transport.send(payload)

            # log response to the responses queue
            self._enqueue_response(status_code, response, None, start, ev)

        except Exception as e:
            # Catch all exceptions and hand them to the responses queue.
            self._enqueue_errors(status_code, e, start, events)

    def _enqueue_errors(self, status_code, error, start, events):
        for ev in events:
            self.sd.incr("send_errors")
            self._enqueue_response(status_code, "", error, start, ev)

    def _enqueue_response(self, status_code, body, error, start, metadata):
        resp = {
            "status_code": status_code,
            "body": body,
            "error": error,
            "duration": (time.time() - start) * 1000,
            "metadata": metadata,
        }

        if (
            200 <= status_code < 300
            and not self.config["quiet"]
            and self.transport_type == "http"
        ):
            alert_ids = body["alertIDs"]
            # if the transport type is http, we synchronously receive the alert IDs
            # generated by the server in the response and can show a helpful error message to the user.
            if alert_ids is not None:
                for alert_id in alert_ids:
                    logger.info(
                        f"IAM Zero permission recommendations are available at {self.config['url']}/alerts/{alert_id}"
                    )

        self.log("enqueuing response = %s", resp)
        if self.block_on_response:
            self.responses.put(resp)
        else:
            try:
                self.responses.put_nowait(resp)
            except queue.Full:
                pass

    def close(self):
        """call close to send all in-flight requests and shut down the
        senders nicely. Times out after max 20 seconds per sending thread
        plus 10 seconds for the response queue"""
        try:
            self.pending.put(None, True, 10)
        except queue.Full:
            pass
        self._sending_thread.join()
        # signal to the responses queue that nothing more is coming.
        try:
            self.responses.put(None, True, 10)
        except queue.Full:
            pass

    def get_response_queue(self):
        """return the responses queue on to which will be sent the response
        objects from each event send"""
        return self.responses
