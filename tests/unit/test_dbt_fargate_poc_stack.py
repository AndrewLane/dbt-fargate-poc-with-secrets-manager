import aws_cdk as core
import aws_cdk.assertions as assertions

from dbt_fargate_poc.dbt_fargate_poc_stack import DbtFargatePocStack

# example tests. To run these tests, uncomment this file along with the example
# resource in dbt_fargate_poc/dbt_fargate_poc_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DbtFargatePocStack(app, "dbt-fargate-poc")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
