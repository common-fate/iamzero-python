import logging
import os

from configparser import Error as ConfigError, RawConfigParser
from typing import Any, Mapping, Optional


LOGGER = logging.getLogger(__name__)

CONFIG_DEFAULT_VALUES = {
    "url": "https://app.iamzero.dev",
    # supported transports: "http", "sqs"
    "transport": "http",
    # a custom botocore session to use for the transport
    # (e.g. if you are assuming a different profile to dispatch events via SQS)
    "transport_custom_aws_session": None,
    "transport_sqs_queue_url": None,
    "record": False,
    "debug": False,
    "token": None,
    "quiet": False,
    "max_batch_size": 100,
    "send_frequency": 0.25,
    "user_agent_addition": "",
}


HOME_FILE_PATH = os.path.expanduser("~/.iamzero.ini")


class Config(object):
    """
    Loads config with the following priorities:

    1. keyword arguments that Config was called with (for example - `Config(url="test")` will always set the url variable to test)
    2. env variables (IAMZERO_ + the name of the config variable - eg IAMZERO_URL)
    2. config file (by default ~/.iamzero.ini, or whatever the value of IAMZERO_CONFIG_FILE env var is)
    3. Default values as per CONFIG_DEFAULT_VALUE
    """

    FILE_ENV_VAR = "IAMZERO_CONFIG_FILE"
    HOME_FILE_PATH = HOME_FILE_PATH

    def __init__(self, **kwargs):
        self._init_args = kwargs

        self.config = {}  # type: (Mapping[str, Any])

        self.default_values = CONFIG_DEFAULT_VALUES

        self.loaders = [
            self.load_from_default_values,
            self.load_from_file,
            self.load_from_env,
            self.load_from_init_args,
        ]

        self.config_path = None
        self.load()

    def load(self):
        """Call each loader and update the config variable at the end"""
        base_config = {}

        for loader in self.loaders:
            loaded = loader()

            if loaded:
                base_config.update(loaded)

        self.config = base_config

    def load_from_default_values(self):
        """Returns default values"""
        return self.default_values

    def load_from_init_args(self):
        """Replaces config with the kwargs that __init__ was called with"""
        config_dict = {}
        for key, val in self._init_args.items():
            if val is not None:
                config_dict[key] = val

        return config_dict

    def load_from_file(self):
        """
        Load from config file
        """

        file_path = (
            self.config_path
            or self._file_path_from_env()
            or self._file_path_from_home()
        )

        if not file_path:
            return {}

        config = RawConfigParser()

        try:
            config.read(file_path)

            config_dict = {}
            for option in config.options("iamzero"):
                config_dict[option] = self._coerce_value(
                    option, config.get("iamzero", option)
                )

            return config_dict
        except ConfigError:
            LOGGER.debug("Error parsing config file %s", file_path)
            return {}

    def load_from_env(self):
        """Load configuration from os environment variables, variables
        must be prefixed with IAMZERO_ to be detected.
        """
        env_config = {}
        for env_var, value in os.environ.items():
            if env_var.startswith("IAMZERO_"):
                key = env_var[8:].lower()
                coerced_value = self._coerce_value(key, value)
                env_config[key] = coerced_value
        return env_config

    def _coerce_value(self, name, value):  # type: (str, Any) -> Any
        default_value = self.default_values.get(name)
        # best effort conversion to boolean
        if isinstance(default_value, bool):
            value = value.lower().strip() in ("1", "true", "yes", "y")
        elif isinstance(default_value, (int, float)):
            try:
                value = type(default_value)(value)
            except ValueError:
                LOGGER.error(
                    "Invalid config value for %s, using default value", name.lower()
                )
                value = default_value
        return value

    def _file_path_from_env(self):
        """Return file path if os environement was set and file exists"""
        path = os.getenv(self.FILE_ENV_VAR, default=None)

        if path and os.path.isfile(path):
            return path

    def _file_path_from_home(self):
        """Return file path if file exists in home directory"""
        if self.HOME_FILE_PATH and os.path.isfile(self.HOME_FILE_PATH):
            return self.HOME_FILE_PATH

    def __getitem__(self, name):  # type: (str) -> Any
        return self.config[name]
