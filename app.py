import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime

# 1. 페이지 설정 및 디자인
st.set_page_config(page_title="SIDIZ AI Intelligence", page_icon="🪑", layout="wide")
st.markdown("""
    <style>
    .main { background-color: #f5f5f5; }
    .stChatMessage { border-radius: 15px; }
    </style>
    """, unsafe_allow_html=True)

# 2. 보안 설정 및 데이터 준비
try:
    # Secrets 읽기
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info)
    
    # Gemini API 설정 (최신 모델 gemini-2.0-flash 적용)
    if "gemini" in st.secrets and "api_key" in st.secrets["gemini"]:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        model = genai.GenerativeModel('models/gemini-2.0-flash')
        st.sidebar.success("✅ Gemini 2.0 엔진 연결 완료", icon="🚀")
    else:
        st.sidebar.error("❌ API 키를 Secrets에서 찾을 수 없습니다.", icon="🚨")
        st.stop()

    # 날짜 자동 계산 (분석 기간 설정)
    today = datetime.date.today().strftime('%Y%m%d')
    three_months_ago = (datetime.date.today() - datetime.timedelta(days=90)).strftime('%Y%m%d')

    # 3. 데이터 분석 지침 (시스템 프롬프트 역할)
    INSTRUCTION = f"""
    당신은 시디즈(SIDIZ)의 시니어 데이터 사이언티스트입니다.
    사용자의 질문을 분석하여 Google BigQuery SQL을 생성하고 인사이트를 설명하세요.

    [데이터셋 정보]
    - 프로젝트 ID: `{info['project_id']}`
    - 데이터셋: `analytics_324424314`
    - 테이블: `events_*` (GA4 데이터)
    - 오늘 날짜: {today}

    [SQL 작성 필수 규칙]
    1. 날짜 필터링: 반드시 `_TABLE_SUFFIX BETWEEN '{three_months_ago}' AND '{today}'`를 사용하세요.
    2. 주단위 분석: `DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)`를 활용하세요.
    3. 매출 계산: 'purchase' 이벤트의 'value' 파라미터(int_value 또는 double_value)를 합산하세요.
    4. 결과물: 생성한 SQL 쿼리문과 함께, 데이터가 시사하는 점을 한글로 친절하게 설명하세요.
    """

except Exception as e:
    st.error(f"초기 설정 중 오류가 발생했습니다: {e}", icon="🔥")
    st.stop()

# 4. UI 구성
st.title("🪑 SIDIZ Data Intelligence Portal")
st.caption("BigQuery 기반 실시간 고객 여정 및 매출 분석 AI")

if "messages" not in st.session_state:
    st.session_state.messages = []

# 대화 로그 출력
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 사용자 질문 입력 및 처리
if prompt := st.chat_input("데이터에게 궁금한 점을 물어보세요..."):
    # 사용자 메시지 기록
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 비서 답변 생성
    with st.chat_message("assistant"):
