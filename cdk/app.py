import aws_cdk as cdk
from stacks.data_pipeline_stack import CollectorStack
from config import get_environment

app = cdk.App()
config = get_environment()

cdk_env = cdk.Environment(account=config.account, region=config.region)

CollectorStack(app, "WeatherCollectorStack",
                  env=cdk_env,
                  config=config)

app.synth()
