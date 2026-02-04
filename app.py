import streamlit as st
from google.cloud import bigquery
import pandas as pd

# 1️⃣ BigQuery 테이블 목록 가져오는 함수 (필수)
@st.cache_data(show_spinner=False)
def get_bq_tables(project_id, dataset_id):
    client = bigquery.Client(project=project_id)

    query = f"""
    SELECT table_name, table_type
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    ORDER BY table_type, table_name
    """

    return client.query(query).to_dataframe()

# 2️⃣ UI
st.title("BQ 구조 확인 (임시)")

if st.checkbox("📌 BigQuery 테이블 구조 확인"):
    df_tables = get_bq_tables(
        project_id="your-project-id",
        dataset_id="your-dataset-id"
    )
    st.dataframe(df_tables, use_container_width=True)
