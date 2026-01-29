import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime
import re
import plotly.express as px
import plotly.graph_objects as go # 고급 시각화용 추가

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
    # 에러 방지를 위한 풀네임 테이블 경로 정의
    table_path = f"`{project_id}.{dataset_id}.events_*`"

    # AI에게 분석 명세서 기반 페르소나 부여 (5대 지표 포함)
    INSTRUCTION = f"""
    당신은 SIDIZ의 데이터 분석 전문가입니다. 사용자의 제품 질문에 대해 다음 가이드를 준수하세요.
    1. 반드시 ```sql ... ``` 블록에 BigQuery SQL을 포함하세요. 테이블은 반드시 {table_path}를 사용하세요.
    2. SQL 작성 시 인구통계(age, gender), 유입경로(source/medium), 매출/수량, 서비스 이용 행태(이벤트명)를 모두 포함하도록 쿼리하세요.
    3. 결과 데이터를 기반으로 비즈니스 인사이트를 3줄 요약하세요.
    4. 상품 필터링 시 UNNEST(items)를 사용하고 LIKE 연산자로 제품명을 찾으세요.
    """

except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 3. UI 구성
st.title("🪑 SIDIZ AI Data Dashboard")
st.caption("SIDIZ GA4 빅데이터를 기반으로 실시간 인텔리전스 리포트를 생성합니다.")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 4. 실행 로직
if prompt := st.chat_input("질문을 입력하세요 (예: T50 구매자들의 특징과 유입 경로 분석해줘)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("빅데이터 분석 엔진 가동 중..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                
                # 인사이트 요약 출력
                st.markdown("### 💡 AI 인사이트 요약")
