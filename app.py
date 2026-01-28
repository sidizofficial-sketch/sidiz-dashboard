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
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info)
    
    if "gemini" in st.secrets and "api_key" in st.secrets["gemini"]:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        # 여기서 모델을 'gemini-1.0-pro'로 시도합니다. (가장 호환성이 높음)
        model = genai.GenerativeModel('gemini-1.0-pro')
        st.sidebar.success("✅ Gemini API 연결 준비 완료")
    else:
        st.sidebar.error("❌ API 키를 확인해주세요.")
        st.stop()

    today = datetime.date.today().strftime('%Y%m%d')
    three_months_ago = (datetime.date.today() - datetime.timedelta(days=90)).strftime('%Y%m%d')

    INSTRUCTION = f"""
    당신은 시디즈(SIDIZ)의 데이터 전문가입니다. SQL을 생성하고 분석하세요.
    - 프로젝트: `{info['project_id']}`, 데이터셋: `analytics_324424314`
    - 오늘: {today}, 기간: {three_months_ago} ~ {today}
    """

except Exception as e:
    st.error(f"설정 중 오류: {e}")
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
        with st.spinner("빅쿼리 분석 중..."):
            try:
                # 1.0-pro 모델은 시스템 지침을 메시지 형태로 합쳐서 보내는 게 가장 안전합니다.
                full_prompt = f"{INSTRUCTION}\n\n질문: {prompt}"
                response = model.generate_content(full_prompt)
                
                if response.text:
                    st.markdown(response.text)
                    st.session_state.messages.append({"role": "assistant", "content": response.text})
            except Exception as e:
                # 마지막 보루: 여기서도 404가 나면 현재 사용 가능한 모델 리스트를 화면에 뿌립니다.
                st.error(f"오류 발생: {e}")
                st.info("현재 계정에서 사용 가능한 모델 리스트를 확인합니다...")
                models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                st.write("사용 가능한 모델:", models)
