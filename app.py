import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime
import re
import plotly.express as px
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Intelligence", page_icon="🪑", layout="wide")

# 2. 보안 및 모델 설정
try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    
    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        target_model = next((m for m in models if "1.5-flash" in m), models[0])
        model = genai.GenerativeModel(target_model)

    project_id = info['project_id']
    dataset_id = "analytics_487246344"
    table_path = f"`{project_id}.{dataset_id}.events_*`"

    INSTRUCTION = f"""
    당신은 SIDIZ의 데이터 분석 전문가입니다. 사용자의 질문에 대해 다음 가이드를 준수하세요.
    1. 반드시 ```sql ... ``` 블록에 BigQuery SQL을 포함하세요. 테이블은 {table_path}를 사용하세요.
    2. 결과 데이터에 age, gender, source, revenue, quantity 정보가 포함되도록 쿼리하세요.
    3. 제품 필터링 시 UNNEST(items)를 사용하고 LIKE 연산자를 활용하세요.
    4. 분석 결과에 대해 비즈니스 인사이트 요약을 제공하세요.
    """

except Exception as e:
    st.error(f"초기 설정 오류: {e}")
    st.stop()

# 3. UI 구성
st.title("🪑 SIDIZ AI Intelligence Dashboard")
st.caption("실시간 GA4 데이터 기반 통합 인텔리전스 리포트")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 4. 분석 실행 로직
if prompt := st.chat_input("질문을 입력하세요 (예: T50 구매자 특징과 유입 경로 분석해줘)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("AI 분석 엔진 가동 중..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                
                # 인사이트 요약 출력
                st.markdown("### 💡 AI 인사이트 요약")
                insight_text = re.sub(r"```sql.*?```", "", answer, flags=re.DOTALL)
                st.info(insight_text)

                # SQL 추출 및 실행
                sql_match = re.search(r"```sql\s*(.*?)\s*```", answer, re.
