import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("BigQuery 연결 테스트")

# 1️⃣ Secrets 확인 (디버깅용, 나중에 삭제 가능)
st.write("Secrets keys:", list(st.secrets.keys()))
st.write("Project ID:", st.secrets["gcp_service_account"]["project_id"])

# 2️⃣ Credentials 생성
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# 3️⃣ BigQuery Client 생성 (중요!)
client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id
)

st.success("✅ BigQuery Client 생성 성공")

# 4️⃣ 실제 쿼리 테스트
df = client.query("SELECT 1 AS ok").to_dataframe()
st.dataframe(df)
