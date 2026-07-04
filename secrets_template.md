# ============================================================
# FILE: secrets_template.md
# PURPOSE: AWS Secrets Manager secret JSON templates
#
# HOW TO USE:
#   1. Replace all <placeholder> values with real values
#   2. Go to AWS Secrets Manager → Store a new secret
#   3. Choose "Other type of secret"
#   4. Paste the JSON below as key/value pairs
#   5. Name the secret exactly as shown
#
# OR use AWS CLI commands provided at the bottom
# ============================================================


# ------------------------------------------------------------
# SECRET 1: Metadata Database
# Secret Name : metadata
# Used by     : --metadata_secret_name in Glue job
# Connects to : datalake_metadata MySQL RDS
# ------------------------------------------------------------

{
  "host":     "<metadata-rds-endpoint>.rds.amazonaws.com",
  "port":     "3306",
  "username": "<metadata-db-username>",
  "password": "<metadata-db-password>",
  "dbname":   "datalake_metadata"
}


# ------------------------------------------------------------
# SECRET 2: Source Database
# Secret Name : media-secret
# Used by     : dataset_database.secret_key_name in metadata
# Connects to : media_db MySQL RDS (source data)
# ------------------------------------------------------------

{
  "host":     "<source-rds-endpoint>.rds.amazonaws.com",
  "port":     "3306",
  "username": "<source-db-username>",
  "password": "<source-db-password>",
  "dbname":   "media_db"
}


# ============================================================
# AWS CLI COMMANDS
# Run these after replacing placeholder values
# ============================================================

# Secret 1 - Metadata
aws secretsmanager create-secret \
  --name media-metadata-secret \
  --region eu-west-1 \
  --secret-string '{
    "host":     "<metadata-rds-endpoint>.rds.amazonaws.com",
    "port":     "3306",
    "username": "<metadata-db-username>",
    "password": "<metadata-db-password>",
    "dbname":   "datalake_metadata"
  }'

# Secret 2 - Source
aws secretsmanager create-secret \
  --name media-mysql-secret \
  --region eu-west-1 \
  --secret-string '{
    "host":     "<source-rds-endpoint>.rds.amazonaws.com",
    "port":     "3306",
    "username": "<source-db-username>",
    "password": "<source-db-password>",
    "dbname":   "media_db"
  }'


# ============================================================
# GLUE JOB PARAMETERS (for reference)
# Set these in Glue job → Job details → Job parameters
# ============================================================

# Key                       Value
# --job_name                Media MySQL Ingestion Job
# --metadata_secret_name    media-metadata-secret
# --crawler_role            arn:aws:iam::<account-id>:role/<glue-crawler-role>
# --region                  eu-west-1
