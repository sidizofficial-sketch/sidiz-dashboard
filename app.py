import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import re
import plotly.express as px
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Dashboard", page_icon="🪑", layout="wide")

# 2. 보안 및 모델 설정
try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    
    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        model_list = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        target = next((m for m in model_list if "1.5-flash" in m), model_list[0])
        model = genai.GenerativeModel(target)

    project_id = info['project_id']
    dataset_id = "analytics_487246344"
    table_path = f"`{project_id}.{dataset_id}.events_*`"

    INSTRUCTION = f"""
    당신은 SIDIZ의 데이터 전문가입니다.
    1. SQL은 ```sql ... ``` 블록에 작성하고 테이블은 {table_path}를 사용하세요.
    2. 결과에 age, gender, source, revenue, quantity가 포함되게 하세요.
    3. 상품 필터링은 UNNEST(items)를 사용하세요.
    4. (1)데모그래픽 (2)유입경로 (3)성과 (4)행태 (5)전환율 5대 지표를 분석하세요.
    """
except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 3. UI 구성
st.title("🪑 SIDIZ AI Intelligence Dashboard")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 4. 분석 실행 로직
if prompt := st.chat_input("질문을 입력하세요 (예: T50 분석해줘)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("AI 엔진 분석 중..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                
                # 인사이트 섹션
                st.markdown("### 💡 AI 인사이트 요약")
                insight = re.sub(r"```sql.*?```", "", answer, flags=re.DOTALL)
                st.info(insight)

                # SQL 추출 및 시각화
