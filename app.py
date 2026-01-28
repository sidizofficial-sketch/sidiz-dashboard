import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import plotly.express as px

# 1. 페이지 설정 및 디자인
st.set_page_config(page_title="SIDIZ AI Intelligence", page_icon="🪑", layout="wide")
st.markdown("""
    <style>
    .main { background-color: #f5f5f5; }
    .stChatMessage { border-radius: 15px; }
    </style>
    """, unsafe_allow_html=True)

# 2. 보안 설정 (Secrets)
info = json.loads(st.secrets["gcp_service_account"]["json_key"])
client = bigquery.Client.from_service_account_info(info)
genai.configure(api_key=st.secrets["gemini"]["api_key"])

# 3. 제미나이 페르소나 및 데이터 사전 정의 (핵심!)
SYSTEM_PROMPT = f"""
당신은 시디즈(SIDIZ)의 시니어 데이터 사이언티스트입니다. 
당신은 루커스튜디오 보고서에 없는 깊이 있는 인사이트를 제공해야 합니다.

[분석 가능한 데이터 범위]
- 테이블: `{info['project_id']}.analytics_324424314.events_*`
- 지표: 세션, 사용자, 구매, 회원가입, 정품등록, 제품 클릭 등
- 특수 분석: 
    1. 멀티 터치 기여 분석 (유입 경로 히스토리 추적)
    2. 제품 간 교차 구매 분석 (T50 구매자가 뮤브도 보는지?)
    3. 이탈 분석 (장바구니 담기 후 왜 결제를 안 하는지?)

[SQL 작성 규칙]
- GA4 빅쿼리의 UNNEST 문법을 정확히 사용하세요.
- 날짜 필터는 항상 _TABLE_SUFFIX를 활용해 효율적으로 짭니다.
- 기여 분석 시 user_pseudo_id와 event_timestamp를 활용해 경로를 재구성하세요.
"""

model = genai.GenerativeModel('gemini-1.5-pro', system_instruction=SYSTEM_PROMPT)

st.title("🪑 SIDIZ Data Intelligence Portal")
st.sidebar.header("📌 분석 추천 질문")
if st.sidebar.button("유튜브 유입자의 결제 기여도 분석"):
    st.session_state.prompt = "유튜브(ig/social 등)로 처음 들어온 사용자들이 결제까지 가는 과정에서 거치는 경로들을 분석해줘."

# 4. 채팅 루프
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("데이터에게 말을 걸어보세요..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("빅쿼리에서 고객 여정을 추적 중..."):
            # 제미나이가 SQL 생성 및 분석
            response = model.generate_content(prompt)
            st.markdown(response.text)
            
            # (여기에 실제 SQL 실행 및 시각화 로직을 추가하여 차트를 띄울 수 있습니다)
            # 예시로 데이터 프레임 구조만 보여줌
            # st.plotly_chart(fig)

    st.session_state.messages.append({"role": "assistant", "content": response.text})
