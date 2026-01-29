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
        # [수정] 모델 이름에서 경로를 완전히 빼고 이름만 전달합니다.
        model = genai.GenerativeModel('gemini-1.5-flash')
        st.sidebar.success("✅ 엔진 연결 완료")

    today = datetime.date.today().strftime('%Y%m%d')
    INSTRUCTION = f"당신은 시디즈 데이터 분석가입니다. 프로젝트:{info['project_id']}, 데이터셋:analytics_324424314 정보를 바탕으로 SQL과 한글 분석을 제공하세요. 오늘날짜:{today}"

except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 3. UI
st.title("🪑 SIDIZ AI Intelligence")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 4. 질문 처리 (가장 안전한 호출 방식)
if prompt := st.chat_input("데이터에게 궁금한 점을 물어보세요"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("분석 중..."):
            try:
                # generate_content 호출 시 모델명을 다시 확인하지 않도록 함
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                if response:
                    st.markdown(response.text)
                    st.session_state.messages.append({"role": "assistant", "content": response.text})
            except Exception as e:
                # 만약 여기서 또 404가 뜨면 API 키 자체의 권한 문제입니다.
                st.error(f"분석 중 오류 발생: {e}")
