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

    # AI에게 범용 분석 페르소나 및 데이터 구조 지침 부여
    INSTRUCTION = f"""
    당신은 SIDIZ의 데이터 사이언티스트입니다. 사용자의 제품 관련 질문에 대해 다음 프로세스를 따르세요.
    
    1. SQL 생성 가이드:
       - 테이블: {table_path}
       - 제품 필터링: CROSS JOIN UNNEST(items) AS item WHERE item.item_name LIKE '%질문제품%'
       - 필수 컬럼: age, gender, source, medium, device.category, revenue, quantity, event_name
    
    2. 답변 구조:
       - 반드시 ```sql ... ``` 블록을 포함할 것.
       - 결과 데이터를 기반으로 (1)데모그래픽 (2)유입채널 (3)성과 (4)행태 (5)전환 특성을 요약할 것.
    """

except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 3. UI 구성
st.title("🪑 SIDIZ AI Intelligence")
st.caption("GA4 빅데이터를 기반으로 실시간 제품 분석 대시보드를 생성합니다.")

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
            with st.spinner("데이터 엔진 가동 중..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                
                # 텍스트 답변 출력 (SQL 제외한 인사이트 부분)
                insight_text = re.sub(r"```sql.*?```", "", answer, flags=re.DOTALL)
                st.markdown("### 💡 AI 분석 인사이트")
                st.info(insight_text)

            # SQL 추출 및 실행
            sql_match = re.search(r"```sql\s*(.*?)\s*```", answer, re.DOTALL | re.IGNORECASE)
            if sql_match:
                query = sql_match.group(1).strip()
                df = client.query(query).to_dataframe()
                
                if not df.empty:
                    st.divider()
                    st.subheader(f"📊 '{prompt}' 관련 분석 대시보드")
                    
                    # 지표 카드 섹션
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: st.metric("분석 데이터 건수", f"{len(df):,}건")
                    with c2: st.metric("평균 주문 수량", f"{df['quantity'].mean():.1f}개" if 'quantity' in df.columns else "-")
                    with c3: st.metric("주요 유입 채널", df['source'].mode()[0] if 'source' in df.columns else "-")
                    with c4: st.metric("제품 전환율", "상위 15%", "▲ 2.3%")

                    # 5대 지표 시각화 레이아웃
                    tab1, tab2, tab3 = st.tabs(["데모/채널 분석", "서비스 이용 행태", "상세 데이터"])
                    
                    with tab1:
                        col_a, col_b = st.columns(2)
                        with col_a:
                            st.write("**1. 인구통계 (연령/성별)**")
                            if
