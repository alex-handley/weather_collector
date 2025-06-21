import aws_cdk as cdk
from stacks.data_pipeline_stack import DataPipelineStack

app = cdk.App()
DataPipelineStack(app, "DataPipelineStack")

app.synth()
