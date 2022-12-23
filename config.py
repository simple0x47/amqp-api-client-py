import os
import sys
import json

AMQP_API_CONNECTION_URI_ENV = 'AMQP_API_CONNECTION_URI'


def get_amqp_connection_uri() -> str:
    amqp_api_uri = os.environ.get(AMQP_API_CONNECTION_URI_ENV)
    if amqp_api_uri is None:
        print("error: missing 'AMQP_API_CONNECTION_URI' environment variable")
        exit(1)

    return amqp_api_uri


def get_token() -> str:
    arguments_count = len(sys.argv)

    if arguments_count != 2:
        print("error: expected token")
        exit(1)

    token_response = json.loads(sys.argv[1])

    if "access_token" not in token_response:
        print("error: missing 'access_token' key from token response")
        exit(1)

    return token_response["access_token"]
