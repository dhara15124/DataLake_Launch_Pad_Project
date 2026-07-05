"""
generate_cloudformation_template.py

Triggered by EventBridge when a .sql file is placed under outputs/ in aps-group-cfn-bucket.
Reads the SQL file, parses dataset_name and job_name, generates a CloudFormation template,
saves it to aps-group-cfn-bucket/cloudformation/, and deploys it via CloudFormation API.
"""

import json
import re
import boto3

CFN_BUCKET      = "aps-group-cfn-bucket"
CFN_PREFIX      = "cloudformation"
GLUE_SCRIPT_LOC = "s3://aps-group-rawzone-bucket/scripts/ingestion.py"
GLUE_ROLE_ARN   = "arn:aws:iam::{}:role/{}-glue-role"


def parse_sql(sql: str) -> dict:
    dataset_match = re.search(r"INSERT INTO dataset.*?VALUES\s*\(\s*'([^']+)'", sql, re.DOTALL)
    job_match     = re.search(r"INSERT INTO job.*?VALUES\s*\(\s*'([^']+)'", sql, re.DOTALL)

    dataset_name = dataset_match.group(1) if dataset_match else "unknown"
    job_name     = job_match.group(1)     if job_match     else f"{dataset_name.title()} MySQL Ingestion Job"

    return {"dataset_name": dataset_name, "job_name": job_name}


def build_template(dataset_name: str, job_name: str, glue_role_arn: str) -> dict:
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": f"Glue job and triggers for {dataset_name}",
        "Resources": {
            "IngestionJob": {
                "Type": "AWS::Glue::Job",
                "Properties": {
                    "Name": job_name,
                    "Role": glue_role_arn,
                    "Command": {
                        "Name": "glueetl",
                        "ScriptLocation": GLUE_SCRIPT_LOC,
                        "PythonVersion": "3"
                    },
                    "GlueVersion": "4.0",
                    "NumberOfWorkers": 2,
                    "WorkerType": "G.1X",
                    "DefaultArguments": {
                        "--job_name": job_name,
                        "--metadata_secret_name": "metadata",
                        "--region": {"Ref": "AWS::Region"},
                        "--additional-python-modules": "pymysql"
                    }
                }
            },
            "OnDemandTrigger": {
                "Type": "AWS::Glue::Trigger",
                "Properties": {
                    "Name": f"{dataset_name}-ingestion-ondemand-trigger",
                    "Type": "ON_DEMAND",
                    "Actions": [{"JobName": {"Ref": "IngestionJob"}}]
                }
            },
            "ScheduledTrigger": {
                "Type": "AWS::Glue::Trigger",
                "Properties": {
                    "Name": f"{dataset_name}-ingestion-scheduled-trigger",
                    "Type": "SCHEDULED",
                    "Schedule": "cron(0 2 * * ? *)",
                    "StartOnCreation": True,
                    "Actions": [
                        {
                            "JobName": {"Ref": "IngestionJob"},
                            "Arguments": {
                                "--job_name": job_name,
                                "--metadata_secret_name": "metadata",
                                "--region": {"Ref": "AWS::Region"}
                            }
                        }
                    ]
                }
            }
        }
    }


def save_template_to_s3(template: dict, dataset_name: str) -> str:
    s3  = boto3.client("s3")
    key = f"{CFN_PREFIX}/{dataset_name}-stack.json"
    s3.put_object(
        Bucket=CFN_BUCKET,
        Key=key,
        Body=json.dumps(template, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    return key


def deploy_stack(template: dict, dataset_name: str, template_s3_key: str):
    cfn        = boto3.client("cloudformation")
    stack_name = f"{dataset_name}-ingestion-stack"
    template_url = f"https://{CFN_BUCKET}.s3.amazonaws.com/{template_s3_key}"

    existing_stacks = cfn.list_stacks(
        StackStatusFilter=["CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]
    )
    stack_names = [s["StackName"] for s in existing_stacks["StackSummaries"]]

    if stack_name in stack_names:
        cfn.update_stack(
            StackName=stack_name,
            TemplateURL=template_url,
            Capabilities=["CAPABILITY_NAMED_IAM"]
        )
    else:
        cfn.create_stack(
            StackName=stack_name,
            TemplateURL=template_url,
            Capabilities=["CAPABILITY_NAMED_IAM"],
            OnFailure="ROLLBACK"
        )


def lambda_handler(event, context):
    s3 = boto3.client("s3")

    if "detail" in event:
        bucket = event["detail"]["bucket"]["name"]
        key    = event["detail"]["object"]["key"]
    else:
        bucket = CFN_BUCKET
        key    = event["s3_key"]

    response = s3.get_object(Bucket=bucket, Key=key)
    sql      = response["Body"].read().decode("utf-8")

    parsed       = parse_sql(sql)
    dataset_name = parsed["dataset_name"]
    job_name     = parsed["job_name"]

    # get glue role arn from existing stack output or use a known role name
    sts           = boto3.client("sts")
    account_id    = sts.get_caller_identity()["Account"]
    glue_role_arn = f"arn:aws:iam::{account_id}:role/your-stack-name-glue-role"

    template        = build_template(dataset_name, job_name, glue_role_arn)
    template_s3_key = save_template_to_s3(template, dataset_name)
    deploy_stack(template, dataset_name, template_s3_key)

    return {
        "statusCode": 200,
        "message": f"Stack deployed for dataset '{dataset_name}'",
        "template_s3_key": f"s3://{CFN_BUCKET}/{template_s3_key}"
    }
