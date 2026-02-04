import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.title("BQ 구조 확인 (임시)")

@st.cache_data
def get_bq_tables(project_id, dataset_id):
    client = bigquery.Client(project=project_id)

    query = f"""
    SELECT
      table_name,
      creation_time,
      row_count,
      size_bytes
    FROM `{project_id}.{dataset_id}.__TABLES__`
    ORDER BY table_name
    """

    return client.query(query).to_dataframe()

if st.checkbox("📌 BigQuery 테이블 구조 확인"):
    with st.spinner("BigQuery 조회 중..."):
        df_tables = get_bq_tables(
            project_id="sidiz-458301",
            dataset_id="analytics_487246344"
        )

    st.success("조회 완료")
    st.dataframe(df_tables, use_container_width=True)
