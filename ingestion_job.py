"""
media_ingestion_job.py

AWS Glue Job Entry Point.

Flow:
  1. Receive job_name from Glue trigger argument
  2. Connect to datalake_metadata MySQL (via Secrets Manager)
  3. Call sp_get_job_step_mappings(job_name) to get all source->dest mappings
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
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from botocore.exceptions import ClientError, EndpointConnectionError


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
    logger.info("Fetching secret: {}".format(secret_name))
    try:
        response = secret_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        logger.info("Successfully fetched secret: {}".format(secret_name))
        return secret
    except Exception as e:
        logger.error("Failed to fetch secret '{}': {}".format(secret_name, str(e)))
        raise


# -------------------------------------------------------
# 2. Connect to datalake_metadata MySQL
# -------------------------------------------------------
def get_metadata_connection(secret_client, metadata_secret_name: str):
    logger.info("Connecting to metadata database using secret: {}".format(metadata_secret_name))
    try:
        creds = get_secret(secret_client, metadata_secret_name)
        conn = pymysql.connect(
            host=creds["host"],
            port=int(creds["port"]),
            user=creds["username"],
            password=creds["password"],
            database="datalake_metadata"
        )
        logger.info("Successfully connected to metadata database: {}".format(creds["host"]))
        return conn
    except pymysql.MySQLError as e:
        logger.error("MySQL connection failed for metadata DB: {}".format(str(e)))
        raise
    except Exception as e:
        logger.error("Unexpected error connecting to metadata DB: {}".format(str(e)))
        raise


# -------------------------------------------------------
# 3. Fetch job step mappings from metadata SP
#    Returns list of rows:
#    (mapping_id, dataset_name, source_schema, source_entity,
#     dest_s3_bucket, dest_s3_prefix,
#     source_secret_key_name, source_database_name,
#     glue_database_name, crawler_name, run_crawler)
# -------------------------------------------------------
def get_job_step_mappings(metadata_conn, job_name: str) -> list:
    logger.info("Calling SP sp_get_job_step_mappings for job: {}".format(job_name))
    try:
        cursor = metadata_conn.cursor()
        cursor.callproc("sp_get_job_step_mappings", [job_name])
        rows = cursor.fetchall()
        cursor.close()
        logger.info("SP returned {} mapping(s) for job: {}".format(len(rows), job_name))
        return rows
    except pymysql.MySQLError as e:
        logger.error("Failed to call SP sp_get_job_step_mappings: {}".format(str(e)))
        raise
    except Exception as e:
        logger.error("Unexpected error fetching job mappings: {}".format(str(e)))
        raise


# -------------------------------------------------------
# 4. Extract table from MySQL via pymysql, write to S3 Parquet via PySpark
# -------------------------------------------------------
def extract_and_load(spark, secret_client, secret_name: str, database_name: str,
                     schema_name: str, table_name: str, s3_path: str) -> int:
    logger.info("Connecting to source database '{}' using secret: {}".format(database_name, secret_name))
    try:
        creds = get_secret(secret_client, secret_name)
        conn = pymysql.connect(
            host=creds["host"],
            port=int(creds["port"]),
            user=creds["username"],
            password=creds["password"],
            database=database_name
        )
        logger.info("Successfully connected to source database: {}".format(creds["host"]))
    except pymysql.MySQLError as e:
        logger.error("Failed to connect to source database '{}': {}".format(database_name, str(e)))
        raise
    except Exception as e:
        logger.error("Unexpected error connecting to source database '{}': {}".format(database_name, str(e)))
        raise

    try:
        query = "SELECT * FROM `{schema}`.`{table}`".format(schema=schema_name, table=table_name)
        logger.info("Executing query: {}".format(query))
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        cursor.close()
        conn.close()
        row_count = len(rows)
        logger.info("Extracted {} rows from {}.{}".format(row_count, schema_name, table_name))
    except pymysql.MySQLError as e:
        logger.error("Failed to extract data from {}.{}: {}".format(schema_name, table_name, str(e)))
        raise
    except Exception as e:
        logger.error("Unexpected error during data extraction from {}.{}: {}".format(schema_name, table_name, str(e)))
        raise

    try:
        logger.info("Creating Spark DataFrame with {} rows and columns: {}".format(row_count, columns))
        df = spark.createDataFrame(rows, schema=columns)
        logger.info("Writing Parquet to: {}".format(s3_path))
        df.write.mode("overwrite").parquet(s3_path)
        logger.info("Successfully written Parquet to: {}".format(s3_path))
        return row_count
    except Exception as e:
        logger.error("Failed to write Parquet to '{}': {}".format(s3_path, str(e)))
        raise


# -------------------------------------------------------
# 5. Create and run Glue crawler -> creates Athena table
# -------------------------------------------------------
def run_crawler(glue_client, crawler_name: str, glue_database: str, s3_path: str, crawler_role: str):
    logger.info("Starting crawler process for: {}".format(crawler_name))

    try:
        glue_client.get_crawler(Name=crawler_name)
        logger.info("Crawler already exists, skipping create: {}".format(crawler_name))
    except glue_client.exceptions.EntityNotFoundException:
        logger.info("Crawler not found, creating: {}".format(crawler_name))
        try:
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
            logger.info("Created crawler: {} | Database: {} | S3 Path: {}".format(crawler_name, glue_database, s3_path))
        except Exception as e:
            logger.error("Failed to create crawler '{}': {}".format(crawler_name, str(e)))
            raise

    try:
        glue_client.start_crawler(Name=crawler_name)
        logger.info("Started crawler: {}".format(crawler_name))
    except glue_client.exceptions.CrawlerRunningException:
        logger.info("Crawler already running: {}".format(crawler_name))
    except Exception as e:
        logger.error("Failed to start crawler '{}': {}".format(crawler_name, str(e)))
        raise

    max_wait = 600
    elapsed  = 0
    while elapsed < max_wait:
        try:
            state = glue_client.get_crawler(Name=crawler_name)["Crawler"]["State"]
            logger.info("Crawler '{}' state: {} | Elapsed: {}s".format(crawler_name, state, elapsed))
            if state == "READY":
                logger.info("Crawler completed successfully: {}".format(crawler_name))
                return
            time.sleep(30)
            elapsed += 30
        except Exception as e:
            logger.error("Error polling crawler '{}' state: {}".format(crawler_name, str(e)))
            raise

    raise Exception("Crawler '{}' timed out after {}s".format(crawler_name, max_wait))


# -------------------------------------------------------
# 6. Grant Lake Formation permissions on table
# -------------------------------------------------------
def grant_table_permissions(lf_client, database_name: str, table_name: str, iam_user_arn: str):
    logger.info("Granting LakeFormation permissions on {}.{} to {}".format(database_name, table_name, iam_user_arn))
    try:
        lf_client.grant_permissions(
            Principal= {"DataLakePrincipalIdentifier": 'arn:aws:iam::216812304371:user/Dhara'},
            Resource={"Table": {"DatabaseName": database_name, "Name": table_name}},
            Permissions=["SELECT", "DESCRIBE"],
            PermissionsWithGrantOption=[]
        )
        logger.info("Granted permissions on {}.{} to {}".format(database_name, table_name, iam_user_arn))
    except Exception as e:
        logger.error("Failed to grant LakeFormation permissions: {}".format(str(e)))
        raise


# -------------------------------------------------------
# 7. Main
# -------------------------------------------------------
def main():
    logger.info("Initialising Glue job...")

    try:
        args = getResolvedOptions(sys.argv, [
            "job_name",
            "metadata_secret_name",
            "crawler_role",
            "region"
        ])
    except Exception as e:
        logger.error("Failed to resolve Glue job arguments: {}".format(str(e)))
        raise

    job_name        = args["job_name"]
    metadata_secret = args["metadata_secret_name"]
    crawler_role    = args["crawler_role"]
    region          = args["region"]

    logger.info("Job arguments resolved | job_name: {} | region: {}".format(job_name, region))

    try:
        sc           = SparkContext()
        glue_context = GlueContext(sc)
        spark        = glue_context.spark_session
        logger.info("SparkContext and GlueContext initialised successfully")
    except Exception as e:
        logger.error("Failed to initialise SparkContext: {}".format(str(e)))
        raise

    secret_client = boto3.client("secretsmanager",  region_name=region)
    glue_client   = boto3.client("glue",            region_name=region)
    lf_client     = boto3.client("lakeformation",   region_name=region)

    logger.info("Job started: {}".format(job_name))

    try:
        metadata_conn = get_metadata_connection(secret_client, metadata_secret)
    except Exception as e:
        logger.error("Cannot proceed — metadata DB connection failed: {}".format(str(e)))
        raise

    try:
        mappings = get_job_step_mappings(metadata_conn, job_name)
    except Exception as e:
        metadata_conn.close()
        logger.error("Cannot proceed — failed to fetch job mappings: {}".format(str(e)))
        raise

    if not mappings:
        metadata_conn.close()
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

        logger.info("Processing mapping_id: {} | {}.{} -> {}".format(
            mapping_id, source_schema_name, source_entity_name, s3_path))

        try:
            row_count = extract_and_load(
                spark, secret_client, source_secret_key_name, source_database_name,
                source_schema_name, source_entity_name, s3_path
            )
            logger.info("Completed mapping_id: {} | {}.{} | Rows written: {}".format(
                mapping_id, source_schema_name, source_entity_name, row_count))

            if run_crawler_flag == 1:
                run_crawler(glue_client, crawler_name, glue_database_name, s3_path, crawler_role)
            else:
                logger.info("Crawler skipped for mapping_id: {} (run_crawler=0)".format(mapping_id))

            grant_table_permissions(
                lf_client, glue_database_name, source_entity_name,
                "arn:aws:iam::216812304371:user/Dhara"
            )

        except Exception:
            error = traceback.format_exc()
            logger.error("Failed mapping_id: {} | {}.{} | Error: {}".format(
                mapping_id, source_schema_name, source_entity_name, error))
            failed.append("{}.{}".format(source_schema_name, source_entity_name))

    metadata_conn.close()
    logger.info("Metadata connection closed")

    if failed:
        raise Exception("Job '{}' completed with failures: {}".format(job_name, ", ".join(failed)))

    logger.info("Job completed successfully: {}".format(job_name))


if __name__ == "__main__":
    main()
