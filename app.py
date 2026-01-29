import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import json, datetime

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI", page_icon="🪑")

# 2. 보안 및 모델 설정
try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info)
    
    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        
        # [해결 핵심] 404 에러 방지를 위해 경로 없이 이름만 전달
        # 만약 이래도 안되면 'gemini-pro'로 바꿔서 모델 존재 여부부터 확인해야 합니다.
        model = genai.GenerativeModel('gemini-1.5-flash')
        st.sidebar.success("✅ 엔진 연결 완료")

    today = datetime.date.today().strftime('%Y%m%d')
    INSTRUCTION = f"당신은 시디즈 데이터 분석가입니다. 프로젝트:{info['project_id']}, 데이터셋:analytics_324424314 정보를 바탕으로 SQL과 분석을 제공하세요. 오늘:{today}"

except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 3. UI 구성
st.title("🪑 SIDIZ Data Intelligence")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 4. 질문 처리
if prompt := st.chat_input("데이터에게 궁금한 점을 물어보세요"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            # 404 에러를 피하기 위한 가장 단순한 호출
            response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
            if response:
                st.markdown(response.text)
                st.session_state.messages.append({"role": "assistant", "content": response.text})
        except Exception as e:
            st.error(f"분석 중 오류 발생: {e}")
