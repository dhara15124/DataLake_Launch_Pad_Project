"""
generate_metadata_inserts.py

Triggered by S3 event when a JSON file is placed under inputs/ in aps-group-cfn-bucket.
Reads the input JSON from S3, generates INSERT SQL, saves SQL back to S3 outputs/,
and executes it against the metadata RDS MySQL database.

Input JSON (4 fields only):
{
    "dataset_name":  "media",
    "database_name": "media_db",
    "schema_name":   "media_db",
    "tables":        ["media_content", "media_reviews"]
}
"""

import json
import os
import boto3
import pymysql
import urllib.parse

S3_BUCKET         = "ingestion-rawzone-bucket"
CONNECTION_TYPE   = "rds_mysql"
CFN_BUCKET        = "aps-group-cfn-bucket"
METADATA_SECRET   = "metadata"
METADATA_DATABASE = "datalake_metadata"


def get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def build_sql(dataset_name, database_name, schema_name, tables) -> str:
    secret_key_name = f"{dataset_name}-secret"
    job_name        = f"{dataset_name.title()} MySQL Ingestion Job"
    job_description = f"Ingests {dataset_name} tables from MySQL RDS to S3 LandingZone"

    dataset_id_sel  = f"(SELECT dataset_id  FROM dataset          WHERE dataset_name  = '{dataset_name}')"
    database_id_sel = f"(SELECT database_id FROM dataset_database WHERE database_name = '{database_name}')"
    schema_id_sel   = f"(SELECT schema_id   FROM dataset_schema   WHERE schema_name   = '{schema_name}')"
    job_id_sel      = f"(SELECT job_id      FROM job              WHERE job_name      = '{job_name}')"
    job_step_id_sel = f"(SELECT job_step_id FROM job_step         WHERE job_id = {job_id_sel} AND step_sequence = 1)"

    lines = []

    lines.append("-- dataset")
    lines.append("INSERT INTO dataset (dataset_name, dataset_description) VALUES")
    lines.append(f"('{dataset_name}', '{dataset_name.title()} source system');\n")

    lines.append("-- dataset_database")
    lines.append("INSERT INTO dataset_database (dataset_id, database_name, connection_type, secret_key_name) VALUES")
    lines.append(f"({dataset_id_sel}, '{database_name}', '{CONNECTION_TYPE}', '{secret_key_name}');\n")

    lines.append("-- dataset_schema")
    lines.append("INSERT INTO dataset_schema (database_id, schema_name) VALUES")
    lines.append(f"({database_id_sel}, '{schema_name}');\n")

    lines.append("-- dataset_entity")
    entity_rows = []
    for table in tables:
        entity_rows.append(f"({schema_id_sel}, '{table}', 'MySQL source - {table}', NULL, NULL)")
    for table in tables:
        entity_rows.append(f"({schema_id_sel}, '{table}', 'LandingZone dest - {table}', '{S3_BUCKET}', 'source/')")
    lines.append("INSERT INTO dataset_entity (schema_id, entity_name, entity_description, s3_bucket, s3_prefix) VALUES")
    lines.append(",\n".join(entity_rows) + ";\n")

    lines.append("-- job")
    lines.append("INSERT INTO job (job_name, job_description) VALUES")
    lines.append(f"('{job_name}', '{job_description}');\n")

    lines.append("-- job_step")
    lines.append("INSERT INTO job_step (job_id, step_sequence, step_name, source_connection_type, dest_connection_type, run_crawler, continue_on_error) VALUES")
    lines.append(f"({job_id_sel}, 1, 'MySQL to LandingZone', '{CONNECTION_TYPE}', 's3_parquet', 0, 1);\n")

    lines.append("-- source_to_destination_mapping")
    mapping_rows = []
    for table in tables:
        src_sel  = f"(SELECT entity_id FROM dataset_entity WHERE schema_id = {schema_id_sel} AND entity_name = '{table}' AND s3_bucket IS NULL)"
        dest_sel = f"(SELECT entity_id FROM dataset_entity WHERE schema_id = {schema_id_sel} AND entity_name = '{table}' AND s3_bucket IS NOT NULL)"
        mapping_rows.append(f"({job_step_id_sel}, {src_sel}, {dest_sel})")
        #mapping_rows.append(f"({job_step_id_sel}, {src_sel}, {dest_sel})   -- {table} MySQL → {table} LandingZone")
    lines.append("INSERT INTO source_to_destination_mapping (job_step_id, source_entity_id, dest_entity_id) VALUES")
    lines.append(",\n".join(mapping_rows) + ";")

    return "\n".join(lines)


def save_sql_to_s3(sql: str, input_key: str):
    s3 = boto3.client("s3")
    filename    = os.path.basename(input_key).replace(".json", ".sql")
    output_key  = f"outputs/{filename}"
    s3.put_object(Bucket=CFN_BUCKET, Key=output_key, Body=sql.encode("utf-8"))
    return output_key


def execute_sql(sql: str):
    creds = get_secret(METADATA_SECRET)
    conn  = pymysql.connect(
        host=creds["host"],
        port=int(creds["port"]),
        user=creds["username"],
        password=creds["password"],
        database=METADATA_DATABASE,
        autocommit=False
    )
    print("connected to db")
    try:
        cursor = conn.cursor()

        # Remove SQL comment lines
        clean_sql = "\n".join(
            line for line in sql.splitlines()
            if not line.strip().startswith("--")
        )

        for i, statement in enumerate(clean_sql.split(";"), start=1):
            stmt = statement.strip()

            if stmt:
                print("=" * 80)
                print(f"Executing Statement {i}")
                print(stmt)
                print("=" * 80)

                rows = cursor.execute(stmt)

                print(f"Rows affected: {rows}")
                print(f"Statement {i} executed successfully.\n")

        conn.commit()
        print("Transaction committed successfully.")

        cursor.close()

    except Exception as e:
        print(f"Database execution failed: {str(e)}")
        print("Rolling back transaction...")
        conn.rollback()
        raise

    finally:
        print("Closing database connection.")
        conn.close()


def lambda_handler(event, context):
    s3 = boto3.client("s3")

    # Support both EventBridge S3 event and direct invocation with s3_key
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"],encoding="utf-8")

    response    = s3.get_object(Bucket=bucket, Key=key)
    input_data  = json.loads(response["Body"].read())
    print(json.dumps(input_data, indent=2, default=str))

    dataset_name  = input_data["dataset_name"]
    database_name = input_data["database_name"]
    schema_name   = input_data["schema_name"]
    tables        = input_data["tables"]

    sql        = build_sql(dataset_name, database_name, schema_name, tables)
    output_key = save_sql_to_s3(sql, key)
    print(f"SQL saved to s3://{CFN_BUCKET}/{output_key}")
    execute_sql(sql)

    return {
        "statusCode": 200,
        "output_s3_key": output_key,
        "message": f"SQL executed and saved to s3://{CFN_BUCKET}/{output_key}"
    }
