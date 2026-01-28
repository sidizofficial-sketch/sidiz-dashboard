import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime

# app.py 15라인 근처에 추가해 보세요
if "gemini" in st.secrets:
    st.sidebar.success("Gemini API 키 로드 완료!")
else:
    st.sidebar.error("Gemini API 키를 찾을 수 없습니다.")

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Intelligence", page_icon="🪑", layout="wide")

# 2. 보안 설정 및 데이터 준비
try:
    # Secrets 읽기
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info)
    genai.configure(api_key=st.secrets["gemini"]["api_key"])

    # 날짜 자동 계산
    today = datetime.date.today().strftime('%Y%m%d')
    three_months_ago = (datetime.date.today() - datetime.timedelta(days=90)).strftime('%Y%m%d')

    # 3. 제미나이 페르소나 설정 (시스템 프롬프트)
    SYSTEM_PROMPT = f"""
    당신은 시디즈(SIDIZ)의 시니어 데이터 사이언티스트입니다.
    사용자의 질문을 분석하여 Google BigQuery SQL을 생성하고 분석 결과를 설명하세요.

    [데이터셋 정보]
    - 프로젝트 ID: `{info['project_id']}`
    - 데이터셋: `analytics_324424314`
    - 테이블: `events_*`

    [SQL 작성 필수 규칙]
    1. 날짜 필터링: 반드시 `_TABLE_SUFFIX`를 사용하세요. 
       - 오늘: {today}, 3개월 전: {three_months_ago}
       - 예: `_TABLE_SUFFIX BETWEEN '{three_months_ago}' AND '{today}'`
    2. 주단위(Weekly) 분석: `DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)`를 사용하세요.
    3. 매출: 'purchase' 이벤트의 'value' 파라미터를 합산하세요.
    4. 결과는 항상 SQL 쿼리와 함께 한글 설명을 제공하세요.
    """

    model = genai.GenerativeModel('gemini-1.5-pro', system_instruction=SYSTEM_PROMPT)

except Exception as e:
    st.error(f"설정 중 오류가 발생했습니다: {e}")
    st.stop()

# 4. UI 구성
st.title("🪑 SIDIZ Data Intelligence Portal")

if "messages" not in st.session_state:
    st.session_state.messages = []

# 기존 대화 출력
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 사용자 입력 처리
if prompt := st.chat_input("데이터에게 말을 걸어보세요..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("빅쿼리에서 데이터를 분석 중..."):
            try:
                # 제미나이 답변 생성
                response = model.generate_content(prompt)
                st.markdown(response.text)
                st.session_state.messages.append({"role": "assistant", "content": response.text})
            except Exception as e:
                st.error(f"분석 중 오류가 발생했습니다: {e}")
