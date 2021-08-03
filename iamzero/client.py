from iamzero.logging import configure_root_logger
from iamzero.config import Config
from iamzero.identity import Identity, IdentityFetcher
from iamzero.event import Event

from iamzero.publisher import Publisher

logger = configure_root_logger(__name__)


class Client(object):
    """
    An iamzero client which is used to dispatch authentication errors to an iamzero collector
    """

    def __init__(
        self,
        config: Config,
        block_on_send=False,
        block_on_response=False,
        transmission_impl=None,
    ):
        self.config = config
        self.identity = Identity()
        self.identity_fetcher = IdentityFetcher(
            identity=self.identity, debug=self.config["debug"]
        )
        self.identity_fetcher.start()

        self.publisher = transmission_impl
        if self.publisher is None:
            self.publisher = Publisher(
                config=self.config,
                identity=self.identity,
                block_on_send=block_on_send,
                block_on_response=block_on_response,
            )

        self.publisher.start()
        self._responses = self.publisher.get_response_queue()
        self.block_on_response = block_on_response

        self.log(
            "initialized iamzero client: token=%s, url=%s transport=%s",
            self.config["token"],
            self.config["url"],
            self.config["transport"],
        )
        if self.config["transport"] == "http" and not self.config["token"]:
            self.log("token not set! set the token if you want to send data to iamzero")

        if (
            self.config["transport"] == "sqs"
            and not self.config["transport_sqs_queue_url"]
        ):
            self.log(
                "Warning: transport mode is 'sqs' but the transport_sqs_queue_url setting is not set. You will likely not receive any IAM Zero events."
            )

    def log(self, msg, *args, **kwargs):
        if self.config["debug"]:
            logger.debug(msg, *args, **kwargs)

    def responses(self):
        """Returns a queue from which you can read a record of response info from
        each event sent. Responses will be dicts with the following keys:

        - `status_code` - the HTTP response from the api (eg. 200 or 503)
        - `duration` - how long it took to POST this event to the api, in ms
        - `metadata` - pass through the metadata you added on the initial event
        - `body` - the content returned by API (will be empty on success)
        - `error` - in an error condition, this is filled with the error message

        When the Client's `close` method is called, a None will be inserted on
        the queue, indicating that no further responses will be written.
        """
        return self._responses

    def send(self, event: Event):
        """
        Enqueues the given event to be sent
        """
        if self.publisher is None:
            self.log(
                "tried to send on a closed or uninitialized iamzero client,"
                " event = %s",
                event,
            )
            return

        self.log("send enqueuing event, event = %s", event)
        self.publisher.send(event)

    def close(self):
        """Wait for in-flight events to be transmitted then shut down cleanly.
        Optional (will be called automatically at exit) unless your
        application is consuming from the responses queue and needs to know
        when all responses have been received."""

        if self.publisher:
            self.publisher.close()

        if self.identity_fetcher:
            self.identity_fetcher.close()

        # set to None so that any future sends throw errors
        self.publisher = None
        self.identity_fetcher = None

    def flush(self):
        """Closes and restarts the transmission, sending all events. Use this
        if you want to perform a blocking send of all events in your
        application.
        """
        if self.publisher and isinstance(self.publisher, Publisher):
            self.publisher.close()
            self.publisher.start()
