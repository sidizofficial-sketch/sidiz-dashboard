import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import json, datetime

st.set_page_config(page_title="SIDIZ AI", page_icon="🪑")

try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info)
    
    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        
        # [우회 전략] 사용 가능한 모델 리스트에서 gemini-1.5-flash를 직접 찾습니다.
        available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        
        # 리스트에 있는 이름 중 가장 적합한 것을 골라 강제 할당
        target_model = 'models/gemini-1.5-flash' 
        if target_model not in available_models:
             # 만약 없으면 리스트의 첫 번째 모델이라도 사용 (비상용)
             target_model = available_models[0] if available_models else 'gemini-pro'

        model = genai.GenerativeModel(target_model)
        st.sidebar.success(f"✅ 엔진 연결 완료: {target_model}")

    today = datetime.date.today().strftime('%Y%m%d')
    INSTRUCTION = f"시디즈 데이터 분석가로서 프로젝트:{info['project_id']}, 데이터셋:analytics_324424314를 기반으로 SQL과 분석을 제공하세요. 오늘:{today}"

except Exception as e:
    st.error(f"초기 설정 오류: {e}")
    st.stop()

st.title("🪑 SIDIZ AI Intelligence")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

if prompt := st.chat_input("데이터에게 궁금한 점을 물어보세요"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            # 안전한 호출을 위해 모델 객체에서 직접 생성
            response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
            st.markdown(response.text)
            st.session_state.messages.append({"role": "assistant", "content": response.text})
        except Exception as e:
            st.error(f"분석 중 오류 발생: {e}")
            # 사용 가능한 모델 목록을 화면에 출력하여 디버깅
            st.info(f"사용 가능 모델 목록: {available_models}")
