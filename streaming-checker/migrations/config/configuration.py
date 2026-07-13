import os

from typing import Any
from migrations.config.environment import Environment


def get_config() -> Any:
    """
    :return:
    """
    env = os.getenv("MIGRATOR_ENVIRONMENT")

    config = None
    if env == Environment.LOCAL:
        from migrations.config.local.config import Config
        config = Config()
    else:
        raise Exception("No environment provided -> export MIGRATOR_ENVIRONMENT=(local/development/production), "
                        "provided: {}".format(env))

    alert_level_code: str = os.getenv("ALERT_LEVEL_CODE")
    if alert_level_code is not None:
        config.ALERT_LEVEL_CODE = alert_level_code

    return config
