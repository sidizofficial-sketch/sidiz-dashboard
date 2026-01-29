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
        
        # [핵심 수정] 경로 없이 이름만 사용하되, 
        # API가 모델을 못 찾을 경우를 대비해 가장 기본 모델인 'gemini-1.5-flash'를 사용합니다.
        # 만약 이래도 404가 뜨면 API 키의 권한 문제입니다.
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
            # 404 에러 방지를 위한 가장 단순한 호출 방식
            response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
            if response:
                st.markdown(response.text)
                st.session_state.messages.append({"role": "assistant", "content": response.text})
        except Exception as e:
            # 에러 메시지를 더 구체적으로 파악하기 위한 처리
            st.error(f"분석 중 오류 발생: {e}")
            if "404" in str(e):
                st.info("💡 팁: API 키를 발급받은 Google AI Studio에서 'Gemini 1.5 Flash' 모델이 목록에 있는지 확인해보세요.")
