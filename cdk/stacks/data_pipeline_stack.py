import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    aws_iam as iam,
    aws_glue as glue,
    aws_athena as athena,
    aws_logs as logs,
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
        glue_db = self.create_glue_database()
        self.create_athena_table(glue_db, data_bucket)

    def create_s3_bucket(self) -> s3.Bucket:
        return s3.Bucket(self, "RawDataBucket")

    def create_lambda_role(self, bucket: s3.Bucket) -> iam.Role:
        role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        bucket.grant_read_write(role)

        # Add permissions for Docker Lambda to access ECR and execute
        role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEC2ContainerRegistryReadOnly"))

        # Add SSM access in case Chrome dependencies are stored there
        role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMReadOnlyAccess"))

        return role

    def create_lambda_function(self, bucket: s3.Bucket, role: iam.Role) -> _lambda.Function:
        log_group = logs.LogGroup(
            self, "LambdaLogGroup",
            log_group_name=f"/aws/lambda/DailyDataFunction",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        lambda_fn = _lambda.DockerImageFunction(
            self, "DailyDataFunction",
            code=_lambda.DockerImageCode.from_image_asset("."),
            timeout=Duration.minutes(10),
            role=role,
            environment={
                "BUCKET": bucket.bucket_name
            },
            memory_size=1024,
            architecture=_lambda.Architecture.X86_64  # Ensure compatibility with Chrome
        )

        log_group.grant_write(lambda_fn)
        return lambda_fn

    def schedule_lambda(self, lambda_fn: _lambda.Function) -> None:
        rule = events.Rule(
            self, "DailyScheduleRule",
            schedule=events.Schedule.cron(minute="0", hour="13")
        )
        rule.add_target(targets.LambdaFunction(lambda_fn))

    def create_glue_database(self) -> glue.CfnDatabase:
        return glue.CfnDatabase(
            self, "AthenaDatabase",
            catalog_id=self.account,
            database_input={
                "name": "daily_data"
            }
        )

    def create_athena_table(self, glue_db: glue.CfnDatabase, bucket: s3.Bucket) -> None:
        athena.CfnNamedQuery(
            self, "CreateAthenaTable",
            database="daily_data",
            query_string=f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS daily_data.daily_table (
                    id INT,
                    value STRING
                )
                PARTITIONED BY (date STRING)
                STORED AS PARQUET
                LOCATION 's3://{bucket.bucket_name}/forecasts/'
            """,
            name="CreateDailyDataTable",
            description="Create Athena table for daily data",
            work_group="primary"
        )
