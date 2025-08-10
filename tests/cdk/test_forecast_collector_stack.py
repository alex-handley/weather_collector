import pytest
from aws_cdk import App, Environment
from aws_cdk.assertions import Template, Match

from stacks.forecast_collector_stack import CollectorStack


class DummyConfig:
    """Minimal config object just to satisfy the constructor."""

    name = "test"
    env_name = "test"

    def get_tags(self):
        return {"Application": "weather-collector", "Environment": "test"}

    def get_config(self):
        return {"name": "test", "env": {"name": "test"}, "tags": self.get_tags()}


@pytest.fixture
def app():
    return App()


@pytest.fixture
def stack(app, monkeypatch):
    """
    Build the stack with SSM lookups monkeypatched so synthesis is deterministic.
    """
    # Patch the SSM parameter lookups to fixed strings
    import aws_cdk.aws_ssm as ssm

    def fake_value_for_string_parameter(scope, parameter_name, **kwargs):
        if parameter_name == "/forecast_collector/locations":
            return "sky_pilot,wedge"
        if parameter_name == "/forecast_collector/models":
            return "nam,icon"
        if parameter_name == "/forecast_collector/forecasts_url":
            return "https://example.com/forecasts"
        # Default fallback in case other parameters appear later
        return f"FAKE::{parameter_name}"

    monkeypatch.setattr(
        ssm.StringParameter,
        "value_for_string_parameter",
        staticmethod(fake_value_for_string_parameter),
    )

    env = Environment(account="111111111111", region="us-west-2")
    return CollectorStack(app, "CollectorStackTest", config=DummyConfig(), env=env)


@pytest.fixture
def template(stack):
    return Template.from_stack(stack)


# --- Tests --------------------------------------------------------------------


def test_bucket_created(template: Template):
    template.resource_count_is("AWS::S3::Bucket", 1)


def test_iam_role_and_policies(template: Template):
    # Role exists with Lambda service principal
    print(template.to_json())
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Action": "sts:AssumeRole",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                            }
                        )
                    ]
                )
            }
        },
    )


def test_log_group_properties(template: Template):
    # Retention is ONE_WEEK
    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        {"LogGroupName": "/aws/lambda/DailyDataFunction", "RetentionInDays": 7},
    )


def test_lambda_is_docker_image_with_env(template: Template):
    # PackageType Image, x86_64 arch, memory/timeout, and env vars present
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "PackageType": "Image",
            "Architectures": ["x86_64"],
            "MemorySize": 3008,
            "Timeout": 600,
            "Environment": {
                "Variables": {
                    "BUCKET": Match.any_value(),
                    "LOCATIONS": "sky_pilot,wedge",
                    "MODELS": "nam,icon",
                    "FORECASTS_URL": "https://example.com/forecasts",
                }
            },
        },
    )


def test_eventbridge_rules_and_targets(template: Template):
    # Morning rule 13:00 UTC -> cron(0 13 * * ? *)
    template.has_resource_properties(
        "AWS::Events::Rule",
        {
            "ScheduleExpression": "cron(0 13 * * ? *)",
            "Targets": Match.array_with(
                [
                    Match.object_like(
                        {
                            "Arn": {
                                "Fn::GetAtt": [
                                    Match.string_like_regexp("^DailyDataFunction.*"),
                                    "Arn",
                                ]
                            }
                        }
                    )
                ]
            ),
        },
    )

    # Evening rule 02:00 UTC -> cron(0 2 * * ? *)
    template.has_resource_properties(
        "AWS::Events::Rule",
        {
            "ScheduleExpression": "cron(0 2 * * ? *)",
            "Targets": Match.array_with(
                [
                    Match.object_like(
                        {
                            "Arn": {
                                "Fn::GetAtt": [
                                    Match.string_like_regexp("^DailyDataFunction.*"),
                                    "Arn",
                                ]
                            }
                        }
                    )
                ]
            ),
        },
    )


def test_glue_databases_created(template: Template):
    template.has_resource_properties(
        "AWS::Glue::Database", {"DatabaseInput": {"Name": "weather_collector_raw"}}
    )
    template.has_resource_properties(
        "AWS::Glue::Database", {"DatabaseInput": {"Name": "weather_collector_standard"}}
    )


def test_resource_counts(template: Template):
    # Sanity check on counts
    template.resource_count_is("AWS::Lambda::Function", 1)
    template.resource_count_is("AWS::Events::Rule", 2)
    template.resource_count_is("AWS::Logs::LogGroup", 1)
    template.resource_count_is("AWS::Glue::Database", 2)
    template.resource_count_is("AWS::IAM::Role", 1)
