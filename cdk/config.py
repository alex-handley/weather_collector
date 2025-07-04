import aws_cdk as cdk
from aws_cdk import aws_events as events
import os
from typing import Dict, Any


class BaseConfig:
    def __init__(self):
        self.account = os.environ.get('CDK_DEPLOY_ACCOUNT', os.environ.get('CDK_DEFAULT_ACCOUNT'))
        self.region = os.environ.get('CDK_DEPLOY_REGION', 'us-west-2')
        self.env_name = os.environ.get('CDK_ENV', 'development')
        self.application_name = "WeatherCollector"
        self.schedule = None  # default, can be overridden

    def get_tags(self) -> Dict[str, str]:
        return {
            "Application": "weather-collector",
            "Environment": self.env_name,
        }

    def get_config(self) -> Dict[str, Any]:
        return {
            "name": self.env_name,
            "cdk_env": self.cdk_env,
            "tags": self.get_tags(),
            "schedule": self.schedule,
        }


class DevConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.schedule = None  # no schedule in dev


class ProdConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        # 5 AM PST = 13:00 UTC
        self.schedule = events.Schedule.cron(minute="0", hour="13")


def get_environment():
    env_name = os.environ.get("CDK_ENV", "development")
    config_cls = ProdConfig if env_name == "production" else DevConfig

    return config_cls()
