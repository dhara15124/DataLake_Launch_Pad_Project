"""
generate_metadata_inserts.py

Input event (4 fields only):
{
    "dataset_name":  "media",
    "database_name": "media_db",
    "schema_name":   "media_db",
    "tables":        ["media_content", "media_reviews"]
}
"""

S3_BUCKET       = "ingestion-rawzone-group"
CONNECTION_TYPE = "rds_mysql"


def lambda_handler(event, context):
    dataset_name  = event["dataset_name"]
    database_name = event["database_name"]
    schema_name   = event["schema_name"]
    tables        = event["tables"]

    secret_key_name = f"{dataset_name}-secret"
    job_name        = f"{dataset_name.title()} MySQL Ingestion Job"
    job_description = f"Ingests {dataset_name} tables from MySQL RDS to S3 LandingZone"

    # Reusable SELECT subqueries for FK lookups
    dataset_id_sel  = f"(SELECT dataset_id  FROM dataset         WHERE dataset_name   = '{dataset_name}')"
    database_id_sel = f"(SELECT database_id FROM dataset_database WHERE database_name  = '{database_name}')"
    schema_id_sel   = f"(SELECT schema_id   FROM dataset_schema  WHERE schema_name    = '{schema_name}')"
    job_id_sel      = f"(SELECT job_id      FROM job             WHERE job_name       = '{job_name}')"
    job_step_id_sel = f"(SELECT job_step_id FROM job_step        WHERE job_id = {job_id_sel} AND step_sequence = 1)"

    lines = []

    # -- dataset
    lines.append("-- dataset")
    lines.append("INSERT INTO dataset (dataset_name, dataset_description) VALUES")
    lines.append(f"('{dataset_name}', '{dataset_name.title()} source system');\n")

    # -- dataset_database
    lines.append("-- dataset_database")
    lines.append("INSERT INTO dataset_database (dataset_id, database_name, connection_type, secret_key_name) VALUES")
    lines.append(f"({dataset_id_sel}, '{database_name}', '{CONNECTION_TYPE}', '{secret_key_name}');\n")

    # -- dataset_schema
    lines.append("-- dataset_schema")
    lines.append("INSERT INTO dataset_schema (database_id, schema_name) VALUES")
    lines.append(f"({database_id_sel}, '{schema_name}');\n")

    # -- dataset_entity (source rows first, then dest rows)
    lines.append("-- dataset_entity")
    entity_rows = []
    for table in tables:
        entity_rows.append(f"({schema_id_sel}, '{table}', 'MySQL source - {table}', NULL, NULL)")
    for table in tables:
        entity_rows.append(f"({schema_id_sel}, '{table}', 'LandingZone dest - {table}', '{S3_BUCKET}', 'source/')")
    lines.append("INSERT INTO dataset_entity (schema_id, entity_name, entity_description, s3_bucket, s3_prefix) VALUES")
    lines.append(",\n".join(entity_rows) + ";\n")

    # -- job
    lines.append("-- job")
    lines.append("INSERT INTO job (job_name, job_description) VALUES")
    lines.append(f"('{job_name}', '{job_description}');\n")

    # -- job_step
    lines.append("-- job_step")
    lines.append("INSERT INTO job_step (job_id, step_sequence, step_name, source_connection_type, dest_connection_type, run_crawler, continue_on_error) VALUES")
    lines.append(f"({job_id_sel}, 1, 'MySQL to LandingZone', '{CONNECTION_TYPE}', 's3_parquet', 0, 1);\n")

    # -- source_to_destination_mapping
    lines.append("-- source_to_destination_mapping")
    mapping_rows = []
    for table in tables:
        src_sel  = f"(SELECT entity_id FROM dataset_entity WHERE schema_id = {schema_id_sel} AND entity_name = '{table}' AND s3_bucket IS NULL)"
        dest_sel = f"(SELECT entity_id FROM dataset_entity WHERE schema_id = {schema_id_sel} AND entity_name = '{table}' AND s3_bucket IS NOT NULL)"
        mapping_rows.append(f"({job_step_id_sel}, {src_sel}, {dest_sel})   -- {table} MySQL → {table} LandingZone")
    lines.append("INSERT INTO source_to_destination_mapping (job_step_id, source_entity_id, dest_entity_id) VALUES")
    lines.append(",\n".join(mapping_rows) + ";")

    return {
        "statusCode": 200,
        "body": "\n".join(lines)
    }
