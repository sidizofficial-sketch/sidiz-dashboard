import streamlit as st
from google.cloud import bigquery

st.title("BigQuery 연결 테스트")

st.write("1️⃣ BigQuery Client 생성 중...")

try:
    client = bigquery.Client()
    st.success("✅ BigQuery Client 생성 성공")
except Exception as e:
    st.error("❌ Client 생성 실패")
    st.exception(e)
    st.stop()

st.write("2️⃣ 단순 쿼리 실행 중...")

QUERY = "SELECT 1 AS test_col"

try:
    df = client.query(QUERY).to_dataframe()
    st.success("✅ 쿼리 실행 성공")
    st.dataframe(df)
except Exception as e:
    st.error("❌ 쿼리 실행 실패")
    st.exception(e)
