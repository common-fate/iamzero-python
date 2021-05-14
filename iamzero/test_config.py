from .config import Config


def test_loading_config_through_kwargs():
    config = Config(debug=True)
    assert config["debug"] == True
