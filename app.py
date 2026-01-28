import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Intelligence", page_icon="🪑", layout="wide")

# 2. 보안 설정 및 데이터 준비
try:
    # Secrets 읽기
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info)
    
    # Gemini API 설정
    if "gemini" in st.secrets and "api_key" in st.secrets["gemini"]:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        st.sidebar.success("✅ Gemini API 연결 준비 완료")
    else:
        st.sidebar.error("❌ Gemini API 키를 Secrets에서 찾을 수 없습니다.")
        st.stop()

    # 날짜 자동 계산
    today = datetime.date.today().strftime('%Y%m%d')
    three_months_ago = (datetime.date.today() - datetime.timedelta(days=90)).strftime('%Y%m%d')

    # 3. 제미나이 지침 정의 (합치기용)
    INSTRUCTION = f"""
    당신은 시디즈(SIDIZ)의 시니어 데이터 사이언티스트입니다.
    아래 규칙을 바탕으로 SQL을 생성하고 분석 결과를 설명하세요.

    [데이터셋 정보]
    - 프로젝트 ID: `{info['project_id']}`
    - 데이터셋: `analytics_324424314`
    - 테이블: `events_*`
    - 오늘 날짜: {today}

    [SQL 규칙]
    1. 날짜 필터링: 반드시 `_TABLE_SUFFIX BETWEEN '{three_months_ago}' AND '{today}'` 형태를 사용하세요.
    2. 결과는 반드시 SQL 쿼리와 함께 한글 분석 내용을 포함하세요.
    """

    # [중요] 모델명을 풀네임으로 변경하여 404 에러 방지
    model = genai.GenerativeModel('models/gemini-1.5-flash')

except Exception as e:
    st.error(f"설정 중 오류가 발생했습니다: {e}")
    st.stop()

# 4. UI 구성
st.title("🪑 SIDIZ Data Intelligence Portal")

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
        with st.spinner("빅쿼리 분석 엔진 가동 중..."):
            try:
                # 합쳐진 프롬프트로 전달
                combined_prompt = f"{INSTRUCTION}\n\n사용자 질문: {prompt}"
                response = model.generate_content(combined_prompt)
                
                st.markdown(response.text)
                st.session_state.messages.append({"role": "assistant", "content": response.text})
            except Exception as e:
                # 여기서도 에러가 나면 모델 리스트를 출력해버립니다 (디버깅용)
                st.error(f"모델 연결 오류. 다른 모델명을 시도해야 합니다: {e}")
