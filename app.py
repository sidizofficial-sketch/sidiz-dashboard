import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import json, datetime

# 1. 초기 설정
st.set_page_config(page_title="SIDIZ AI", page_icon="🪑", layout="wide")

try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info)
    
    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        st.sidebar.success("✅ 엔진 연결 완료")
    
    today = datetime.date.today().strftime('%Y%m%d')
    INSTRUCTION = f"당신은 시디즈 데이터 전문가입니다. 프로젝트:{info['project_id']}, 데이터셋:analytics_324424314를 기반으로 SQL과 분석을 제공하세요. 오늘:{today}"

except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 2. UI 및 대화
st.title("🪑 SIDIZ Data Intelligence")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 3. 입력창 (이 부분이 잘리면 안 됩니다!)
if prompt := st.chat_input("데이터에게 물어보세요"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            response = model.generate_content(f"{INSTRUCTION}\n 질문: {prompt}")
            answer = response.text
            st.markdown(answer)
            st.session_state.messages.append({"role": "assistant", "content": answer})
        except Exception as e:
            st.error(f"오류: {e}")
