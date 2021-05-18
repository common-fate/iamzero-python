from iamzero.logging import configure_root_logger
import threading
import queue
from botocore.session import get_session

logger = configure_root_logger(__name__)


class Identity(object):
    """
    Used to store AWS identity
    """

    def __init__(self) -> None:
        self.mutex = threading.RLock()
        self.initialized = threading.Condition(self.mutex)
        self.user = None
        self.role = None
        self.account = None
        self.error = None

    def set(self, user: str = None, role: str = None, account: str = None):
        with self.initialized:
            self.user = user
            self.role = role
            self.account = account
            self.initialized.notify_all()

    def set_error(self, error):
        """
        Marks that an error was received while fetching the AWS identity.
        This marks the Identity object as initialised, unblocking threads that were
        waiting on it.
        """
        with self.initialized:
            self.error = error
            self.initialized.notify_all()


class IdentityRequest(object):
    """
    Stores AWS credentials used to make a request to fetch identity
    using AWS STS get-caller-identity
    """

    def __init__(self, access_key: str, secret_key: str, token: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.token = token


class IdentityFetcher(object):
    """
    Manages a background thread which fetches AWS identity
    using AWS STS get-caller-identity
    """

    def __init__(self, identity=None, debug=False) -> None:
        self.pending: queue.Queue[IdentityRequest] = queue.Queue(5)
        self.debug = debug
        self.identity: Identity = identity

        if self.identity is None:
            raise Exception("Identity must be provided")

    def log(self, msg, *args, **kwargs):
        if self.debug:
            logger.debug(msg, *args, **kwargs)

    def fetch_identity(self, access_key=None, secret_key=None, token=None):
        if access_key is None or secret_key is None:
            self.log("access_key and secret_key must be provided")
            return
        self.log("requesting identity")
        self.pending.put(
            IdentityRequest(access_key=access_key, secret_key=secret_key, token=token)
        )

    def start(self):
        self._sending_thread = threading.Thread(target=self._sender)
        self._sending_thread.daemon = True
        self._sending_thread.start()
        self.log("started identity fetcher thread")

    def _sender(self):
        while True:
            ev = self.pending.get()
            if ev is None:
                return
            try:
                session = get_session()
                sts = session.create_client(
                    "sts",
                    aws_access_key_id=ev.access_key,
                    aws_secret_access_key=ev.secret_key,
                    aws_session_token=ev.token,
                )
                identity = sts.get_caller_identity()
                self.identity.set(
                    user=identity["UserId"],
                    role=identity["Arn"],
                    account=identity["Account"],
                )
            except Exception as e:
                self.identity.set_error(e)

    def close(self):
        """call close to send all in-flight requests and shut down the
        senders nicely. Times out after max 20 seconds per sending thread
        plus 10 seconds for the response queue"""
        try:
            self.pending.put(None, True, 10)
        except queue.Full:
            pass
        self._sending_thread.join()
