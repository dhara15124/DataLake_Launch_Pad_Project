"""
media_ingestion_job.py

AWS Glue Job Entry Point.

Flow:
  1. Receive job_name from Glue trigger argument
  2. Connect to datalake_metadata MySQL (via Secrets Manager)
  3. Call sp_get_job_step_mappings(job_name) to get all source→dest mappings
  4. For each mapping: connect to source MySQL, extract table, write S3 Parquet
  5. Run Glue crawler to create/update Athena table
"""

import sys
import json
import logging
import traceback
import time
import boto3
import pymysql
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext


# -------------------------------------------------------
# Logging setup
# -------------------------------------------------------
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("media_ingestion_job")
logger.setLevel(logging.INFO)


# -------------------------------------------------------
# 1. Get secret from Secrets Manager
# -------------------------------------------------------
def get_secret(secret_client, secret_name: str) -> dict:
    response = secret_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


# -------------------------------------------------------
# 2. Connect to datalake_metadata MySQL
# -------------------------------------------------------
def get_metadata_connection(secret_client, metadata_secret_name: str):
    creds = get_secret(secret_client, metadata_secret_name)
    return pymysql.connect(
        host=creds["host"],
        port=int(creds["port"]),
        user=creds["username"],
        password=creds["password"],
        database="datalake_metadata"
    )


# -------------------------------------------------------
# 3. Fetch job step mappings from metadata SP
#    Returns list of rows:
#    (mapping_id, dataset_name, source_schema, source_entity,
#     dest_s3_bucket, dest_s3_prefix,
#     source_secret_key_name, source_database_name,
#     glue_database_name, crawler_name, run_crawler)
# -------------------------------------------------------
def get_job_step_mappings(metadata_conn, job_name: str) -> list:
    cursor = metadata_conn.cursor()
    cursor.callproc("sp_get_job_step_mappings", [job_name])
    rows = []
    for result in cursor.stored_results():
        rows = result.fetchall()
    cursor.close()
    return rows


# -------------------------------------------------------
# 4. Extract table from MySQL via pymysql, write to S3 Parquet via PySpark
# -------------------------------------------------------
def extract_and_load(spark, secret_client, secret_name: str, database_name: str,
                     schema_name: str, table_name: str, s3_path: str, logger) -> int:
    creds = get_secret(secret_client, secret_name)
    conn = pymysql.connect(
        host=creds["host"],
        port=int(creds["port"]),
        user=creds["username"],
        password=creds["password"],
        database=database_name
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM `{schema}`.`{table}`".format(schema=schema_name, table=table_name))
    rows = cursor.fetchall()
    columns = [col[0].lower() for col in cursor.description]
    cursor.close()
    conn.close()

    row_count = len(rows)
    logger.info("Extracted {} rows from {}.{}".format(row_count, schema_name, table_name))

    df = spark.createDataFrame(rows, schema=columns)
    df.write.mode("overwrite").parquet(s3_path)
    logger.info("Written Parquet to: {}".format(s3_path))
    return row_count


# -------------------------------------------------------
# 6. Create and run Glue crawler → creates Athena table
# -------------------------------------------------------
def run_crawler(glue_client, crawler_name: str, glue_database: str, s3_path: str, crawler_role: str, logger):
    # Delete existing crawler if present
    try:
        glue_client.delete_crawler(Name=crawler_name)
        logger.info("Deleted existing crawler: {}".format(crawler_name))
        time.sleep(5)
    except glue_client.exceptions.EntityNotFoundException:
        pass

    # Create crawler
    glue_client.create_crawler(
        Name=crawler_name,
        Role=crawler_role,
        DatabaseName=glue_database,
        Targets={"S3Targets": [{"Path": s3_path}]},
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE"
        }
    )
    logger.info("Created crawler: {}".format(crawler_name))

    # Start crawler and wait
    glue_client.start_crawler(Name=crawler_name)
    logger.info("Started crawler: {}".format(crawler_name))

    max_wait = 600  # 10 minutes
    elapsed  = 0
    while elapsed < max_wait:
        response = glue_client.get_crawler(Name=crawler_name)
        state = response["Crawler"]["State"]
        if state == "READY":
            logger.info("Crawler completed: {}".format(crawler_name))
            return
        logger.info("Crawler state: {} — waiting...".format(state))
        time.sleep(30)
        elapsed += 30

    raise Exception("Crawler timed out after {}s: {}".format(max_wait, crawler_name))


# -------------------------------------------------------
# 7. Main
# -------------------------------------------------------
def main():
    args = getResolvedOptions(sys.argv, [
        "job_name",
        "metadata_secret_name",
        "crawler_role",
        "region"
    ])

    job_name        = args["job_name"]
    metadata_secret = args["metadata_secret_name"]
    crawler_role    = args["crawler_role"]
    region          = args["region"]

    sc           = SparkContext()
    glue_context = GlueContext(sc)
    spark        = glue_context.spark_session

    secret_client = boto3.client("secretsmanager", region_name=region)
    glue_client   = boto3.client("glue",           region_name=region)

    logger.info("Job started: {}".format(job_name))

    # Connect to metadata DB
    metadata_conn = get_metadata_connection(secret_client, metadata_secret)

    # Get all source→dest mappings for this job
    mappings = get_job_step_mappings(metadata_conn, job_name)
    if not mappings:
        raise Exception("No active mappings found for job: {}".format(job_name))

    logger.info("Found {} mapping(s) for job: {}".format(len(mappings), job_name))

    failed = []

    for row in mappings:
        (
            mapping_id,
            dataset_name,
            source_schema_name,
            source_entity_name,
            dest_s3_bucket,
            dest_s3_prefix,
            source_secret_key_name,
            source_database_name,
            glue_database_name,
            crawler_name,
            run_crawler_flag
        ) = row

        full_prefix = "{base}{dataset}/{schema}/{entity}".format(
            base=dest_s3_prefix.rstrip("/") + "/",
            dataset=dataset_name,
            schema=source_schema_name,
            entity=source_entity_name
        )
        s3_path = "s3://{bucket}/{prefix}/".format(
            bucket=dest_s3_bucket,
            prefix=full_prefix.strip("/")
        )

        try:
            logger.info("Processing: {}.{}".format(source_schema_name, source_entity_name))

            # Extract via PySpark JDBC and write Parquet
            row_count = extract_and_load(
                spark, secret_client, source_secret_key_name, source_database_name,
                source_schema_name, source_entity_name, s3_path, logger
            )

            logger.info("Completed {}.{} | Rows: {}".format(source_schema_name, source_entity_name, row_count))

            # Run crawler if flagged
            if run_crawler_flag == 1:
                run_crawler(glue_client, crawler_name, glue_database_name, s3_path, crawler_role, logger)

        except Exception:
            error = str(traceback.format_exc())
            logger.error("Failed {}.{}: {}".format(source_schema_name, source_entity_name, error))
            failed.append("{}.{}".format(source_schema_name, source_entity_name))

    metadata_conn.close()

    if failed:
        raise Exception("Job completed with failures: {}".format(", ".join(failed)))

    logger.info("Job completed successfully: {}".format(job_name))


if __name__ == "__main__":
    main()
