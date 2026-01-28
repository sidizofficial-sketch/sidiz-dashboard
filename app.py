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
    
    # Gemini API 설정 (최신 모델명 적용)
    if "gemini" in st.secrets and "api_key" in st.secrets["gemini"]:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        model = genai.GenerativeModel('models/gemini-2.0-flash')
        st.sidebar.success("✅ Gemini 2.0 엔진 연결 완료", icon="🚀")
    else:
        st.sidebar.error("❌ API 키를 확인해주세요.", icon="🚨")
        st.stop()

    # 날짜 자동 계산
    today = datetime.date.today().strftime('%Y%m%d')
    three_months_ago = (datetime.date.today() - datetime.timedelta(days=90)).strftime('%Y%m%d')

    # 3. 데이터 분석 지침
    INSTRUCTION = f"""
    당신은 시디즈(SIDIZ)의 데이터 전문가입니다. SQL을 생성하고 분석하세요.
    - 프로젝트 ID: `{info['project_id']}`
    - 데이터셋: `analytics_324424314`
    - 테이블: `events_*`
    - 오늘 날짜: {today}
    """

except Exception as e:
    st.error(f"초기 설정 오류: {e}", icon="🔥")
    st.stop()

# 4. UI 구성
st.title("🪑 SIDIZ Data Intelligence Portal")

if "messages" not in st.session_state:
    st.session_state.messages = []

# 대화 로그 출력
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 5. 사용자 입력 처리
if prompt := st.chat_input("데이터에게 궁금한 점을 물어보세요..."):
    # 사용자 메시지 표시
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 어시스턴트 답변 생성 (이 부분의 들여쓰기를 확인하세요!)
    with st.chat_message("assistant"):
        with st.spinner("분석 중..."):
            try:
                # 지침과 질문 결합
                full_query = f"{INSTRUCTION}\n\n사용자 질문: {prompt}"
                response = model.generate_content(full_query)
                
                # 답변 출력 및 저장
                answer = response.text
                st.markdown(answer)
                st.session_state.messages.append({"role": "assistant", "content": answer})
                
            except Exception as e:
                st.error(f"분석 중 오류 발생: {e}", icon="🚨")
