import json
import boto3
import streamlit as st

BUCKET = "aps-group-cfn-bucket"

s3 = boto3.client("s3")

st.title("Dataset Metadata Input")

dataset_name  = st.text_input("Dataset Name", placeholder="e.g. radio")
database_name = st.text_input("Database Name", placeholder="e.g. radio_db")
schema_name   = st.text_input("Schema Name", placeholder="e.g. radio_db")
tables        = st.text_area("Tables (one per line)", placeholder="radio_content\nradio_reviews")

if st.button("Submit"):
    if not all([dataset_name, database_name, schema_name, tables]):
        st.error("All fields are required")
    else:
        payload = {
            "dataset_name":  dataset_name.strip(),
            "database_name": database_name.strip(),
            "schema_name":   schema_name.strip(),
            "tables":        [t.strip() for t in tables.strip().splitlines() if t.strip()]
        }
        key = f"inputs/{dataset_name.strip()}.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        st.success(f"Saved to s3://{BUCKET}/{key}")
        st.json(payload)
