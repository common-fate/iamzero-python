import iamzero
from wrapt import wrap_function_wrapper


def wrapped_api_call(wrapped, instance, args, kwargs):
    try:
        client = iamzero.get_client()
        result = wrapped(*args, **kwargs)

        if client.config["record"]:
            # add action to queue to be dispatched
            data = {
                "type": "awsAction",
                "service": instance._service_model.service_name,
                "region": instance.meta.region_name,
                "operation": args[0],
                "parameters": args[1],
            }
            iamzero.send_event(data=data)

        return result
    except Exception as e:
        # add error to queue to be dispatched
        exception_message = None
        exception_code = None

        if hasattr(e, "response"):
            exception_message = e.response.get("Error", {}).get("Message")
            exception_code = e.response.get("Error", {}).get("Code")

        data = {
            "type": "awsError",
            "service": instance._service_model.service_name,
            "region": instance.meta.region_name,
            "operation": args[0],
            "parameters": args[1],
            "exceptionMessage": exception_message,
            "exceptionCode": exception_code,
        }
        iamzero.send_event(data=data)
        raise


def wrapped_client_creator(wrapped, instance, args, kwargs):
    # By default, botocore does not store the role ARN of the session.
    # We need this so that iamzero can tell **which** role or user
    # had the permissions error, so we know what to create
    # least-privilege recommendations for.
    #
    # We instrument the client creator method to capture the AWS credentials.
    # These are used to make a call to the AWS STS get-caller-identity endpoint
    # in a background thread, to identify the role ARN of the session.
    #
    # These credentials are only used to call AWS resources and aren't dispatched
    # to the iamzero collector.
    credentials = kwargs["credentials"]
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    token = credentials.token

    service_name = kwargs["service_name"]

    # don't fetch identity for the sts service, as iamzero uses a botocore
    # client under the hood to fetch the identity. This would cause an infinite
    # instrumentation loop.
    if service_name != "sts":
        iamzero.fetch_identity(
            access_key=access_key, secret_key=secret_key, token=token
        )

    return wrapped(*args, **kwargs)


wrap_function_wrapper("botocore.client", "BaseClient._make_api_call", wrapped_api_call)

wrap_function_wrapper(
    "botocore.client", "ClientCreator.create_client", wrapped_client_creator
)
