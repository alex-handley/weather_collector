import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    IgnoreMode,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    aws_iam as iam,
    aws_glue as glue,
    aws_logs as logs,
    aws_ssm as ssm,
)
from constructs import Construct
from config import BaseConfig


class CollectorStack(Stack):
    def __init__(self, scope: Construct, id: str, config: BaseConfig, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config

        data_bucket = self.create_s3_bucket()
        lambda_role = self.create_lambda_role(data_bucket)
        lambda_fn = self.create_lambda_function(data_bucket, lambda_role)
        self.schedule_lambda(lambda_fn)
        self.create_glue_databases()

    def create_s3_bucket(self) -> s3.Bucket:
        return s3.Bucket(self, "CollectorBucket")

    def create_lambda_role(self, bucket: s3.Bucket) -> iam.Role:
        role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        bucket.grant_read_write(role)

        # Add permissions for Docker Lambda to access ECR and execute
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonEC2ContainerRegistryReadOnly"
            )
        )

        # Add SSM access in case Chrome dependencies are stored there
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMReadOnlyAccess")
        )

        return role

    def create_lambda_function(
        self, bucket: s3.Bucket, role: iam.Role
    ) -> _lambda.Function:
        log_group = logs.LogGroup(
            self,
            "LambdaLogGroup",
            log_group_name=f"/aws/lambda/DailyDataFunction",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        locations = ssm.StringParameter.value_for_string_parameter(
            self, "/forecast_collector/locations"
        )
        models = ssm.StringParameter.value_for_string_parameter(
            self, "/forecast_collector/models"
        )
        forecasts_url = ssm.StringParameter.value_for_string_parameter(
            self, "/forecast_collector/forecasts_url"
        )

        lambda_fn = _lambda.DockerImageFunction(
            self,
            "DailyDataFunction",
            code=_lambda.DockerImageCode.from_image_asset(
                ".", ignore_mode=IgnoreMode.DOCKER
            ),
            timeout=Duration.minutes(10),
            role=role,
            environment={
                "BUCKET": bucket.bucket_name,
                "LOCATIONS": locations,
                "MODELS": models,
                "FORECASTS_URL": forecasts_url,
            },
            memory_size=3008,
            architecture=_lambda.Architecture.X86_64,  # Ensure compatibility with Chrome
        )

        log_group.grant_write(lambda_fn)
        return lambda_fn

    def schedule_lambda(self, lambda_fn: _lambda.Function) -> None:
        # 5 AM PT summer / 4 AM PT winter
        morning_rule = events.Rule(
            self,
            "MorningScheduleRule",
            schedule=events.Schedule.cron(minute="0", hour="13"),
        )
        morning_rule.add_target(targets.LambdaFunction(lambda_fn))

        # 6 PM PT summer / 5 PM PT winter
        evening_rule = events.Rule(
            self,
            "EveningScheduleRule",
            schedule=events.Schedule.cron(minute="0", hour="2"),
        )
        evening_rule.add_target(targets.LambdaFunction(lambda_fn))

    def create_glue_databases(self) -> None:
        glue.CfnDatabase(
            self,
            "WeatherCollectorRawDatabase",
            catalog_id=self.account,
            database_input={
                "name": "weather_collector_raw",
                "description": "Raw data from weather collector",
            },
        )

        glue.CfnDatabase(
            self,
            "WeatherCollectorStdDatabase",
            catalog_id=self.account,
            database_input={
                "name": "weather_collector_standard",
                "description": "Standard Zone for weather collector",
            },
        )
