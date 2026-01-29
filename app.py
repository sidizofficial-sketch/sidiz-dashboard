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
    당신은 SIDIZ 데이터 분석 전문가입니다.
    1. SQL은 반드시 ```sql ... ``` 블록에 작성하고 테이블은 {table_path}를 사용하세요.
    2. 결과 데이터에 age, gender, source, revenue, quantity가 포함되게 하세요.
    3. 상품 필터링 시 UNNEST(items)를 사용하세요.
    4. 분석 결과에 (1)인구통계 (2)유입경로 (3)성과 (4)행태 (5)전환율 요약을 포함하세요.
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
if prompt := st.chat_input("질문을 입력하세요 (예: T50 구매자 특징 알려줘)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("AI가 데이터를 분석 중입니다..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                
                # 1. 인사이트 요약 출력
                st.markdown("### 💡 AI 인사이트 요약")
                insight_text = re.sub(r"```sql.*?```", "", answer, flags=re.DOTALL)
                st.info(insight_text)

                # 2. SQL 추출 및 실행
                sql_match = re.search(r"```sql\s*(.*?)\s*```", answer, re.DOTALL)
                
                if sql_match:
                    query = sql_match.group(1).strip()
                    df = client.query(query).to_dataframe()
                    
                    if not df.empty:
                        st.divider()
                        st.subheader("📊 실시간 분석 대시보드")
                        
                        # 3. KPI 카드 (성과 지표)
                        m1, m2, m3 = st.columns(3)
                        with m1: st.metric("총 분석 모수", f"{len(df):,}건")
                        with m2: 
                            avg_rev = df['revenue'].mean() if 'revenue' in df.columns else 0
                            st.metric("평균 구매액", f
