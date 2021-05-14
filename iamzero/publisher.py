from iamzero.logging import configure_root_logger
from iamzero.config import Config
from iamzero.identity import Identity
from typing import List
from iamzero.event import Event

import queue
from urllib.parse import urljoin
import gzip
import io
import json
import threading
import requests
import statsd
import time

from iamzero.version import VERSION

logger = configure_root_logger(__name__)


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
        self.gzip_compression_level = gzip_compression_level
        self.gzip_enabled = gzip_enabled
        self.url = config["url"]
        self.identity: Identity = identity

        if self.identity is None:
            raise Exception("Identity must be provided")

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
        #
        # TODO: likely need to handle the case where identity is
        # not loaded properly rather than waiting forever and letting
        # the pending queue fill up.
        # TODO: how does this work with multiple identities if assume role
        # functionality is used?
        with self.identity.initialized:
            self.identity.initialized.wait()

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
        try:
            url = urljoin(self.url, "api/v1/events/")
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
            self.log("firing batch, size = %d", len(payload))
            resp = self.session.post(
                url,
                headers={
                    "x-iamzero-token": self.token,
                    "Content-Type": "application/json",
                },
                data=data,
                timeout=10.0,
            )
            status_code = resp.status_code
            resp.raise_for_status()
            # log response to the responses queue
            self._enqueue_response(status_code, resp.json(), None, start, ev)

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
