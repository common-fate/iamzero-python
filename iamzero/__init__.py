from iamzero.config import Config
from iamzero.event import Event
from iamzero.client import Client
from iamzero.instrumentation import *
import os
import atexit
from typing import Dict, Optional, TYPE_CHECKING, Type

if TYPE_CHECKING:
    import boto3

# we store the iamzero client as a global to avoid initialising
# multiple clients
_IAMZERO_CLIENT: Optional[Client] = None
_INITPID = None

WARNED_UNINITIALIZED = False


def init(
    token: str = None,
    record: bool = None,
    url: str = None,
    debug: bool = None,
    quiet: bool = None,
    transport: str = None,
    transport_custom_aws_session: Type["boto3.Session"] = None,
    transport_sqs_queue_url: str = None,
):
    global _IAMZERO_CLIENT
    global _INITPID

    pid = os.getpid()

    # require that the IAM Zero identity fetcher is defined
    # if it is not defined, the library may not have been fully set up
    # and we should initialise it again.
    # This has occurred in AWS Lambda environments, resulting in the error:
    # `'NoneType' object has no attribute 'fetch_identity'` being thrown upon initialisation.
    if _IAMZERO_CLIENT and _IAMZERO_CLIENT.identity_fetcher is not None:
        if pid == _INITPID:
            _IAMZERO_CLIENT.log("iamzero is already initialised, skipping init")
            return
        else:
            _IAMZERO_CLIENT.log(
                f"iamzero already initialised, but process ID has changed (previously {_INITPID}, now {pid}"
            )
            _IAMZERO_CLIENT.close()

    config = Config(
        token=token,
        record=record,
        url=url,
        debug=debug,
        quiet=quiet,
        transport=transport,
        transport_custom_aws_session=transport_custom_aws_session,
        transport_sqs_queue_url=transport_sqs_queue_url,
    )
    _IAMZERO_CLIENT = Client(config=config)
    _INITPID = pid


def get_client():
    return _IAMZERO_CLIENT


def send_event(data: Dict):
    event = Event(data=data)
    client = get_client()
    if client:
        client.send(event)


def fetch_identity(access_key=None, secret_key=None, token=None):
    client = get_client()
    if client and client.identity_fetcher:
        client.identity_fetcher.fetch_identity(
            access_key=access_key, secret_key=secret_key, token=token
        )


def get_responses_queue():
    client = get_client()
    if client:
        return client.responses()


def _flush():
    """
    Allows iamzero to be used in shorter Python scripts which exit immediately.
    We block the main thread until any pending messages have been flushed.
    """
    client = get_client()
    if client:
        client.close()


atexit.register(_flush)
