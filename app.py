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
        
        # [해결책] 404 방지를 위해 '-latest'를 붙이거나 가장 표준적인 이름을 사용합니다.
        # 그래도 404가 뜨면 'gemini-1.5-pro-latest'로 시도해보세요.
        model = genai.GenerativeModel('gemini-1.5-flash-latest') 
        st.sidebar.success("✅ 시디즈 분석 엔진 연결 완료", icon="🚀")
    else:
        st.sidebar.error("❌ API 키를 확인해주세요.", icon="🚨")
        st.stop()

    today = datetime.date.today().strftime('%Y%m%d')

    # 3. 데이터 분석 지침
    INSTRUCTION = """
    당신은 시디즈(SIDIZ)의 데이터 분석 전문가입니다. 
    GA4 BigQuery 데이터를 기반으로 답변하세요.
    - 프로젝트 ID: """ + str(info['project_id']) + """
    - 데이터셋: analytics_324424314
    - 테이블: events_*
    - 오늘 날짜: """ + today + """
    """

except Exception as e:
    st.error(f"초기 설정 오류: {e}", icon="🔥")
    st.stop()

# 4. UI 구성
st.title("🪑 SIDIZ Data Intelligence Portal")
st.markdown("---")

# 세션 상태 초기화 (이 부분이 정확해야 대화창이 유지됩니다)
if "messages" not in st.session_state:
    st.session_state.messages = []

# 기존 대화 표시
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 5. 사용자 입력 처리
# if prompt가 코드 가장 바깥쪽(왼쪽 벽)에 붙어 있어야 대화창이 사라지지 않습니다.
prompt = st.chat_input("데이터에게 궁금한 점을 물어보세요...")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("빅쿼리 데이터를 분석하고 있습니다..."):
            try:
                full_query = INSTRUCTION + "\n\n사용자 질문: " + prompt
                response = model.generate_content(full_query)
                
                if response and response.text:
                    answer = response.text
                    st.markdown(answer)
                    st.session_state.messages.append({"role": "assistant", "content": answer})
                else:
                    st.error("AI가 답변을 생성하지 못했습니다. 다시 시도해주세요.")
                
            except Exception as e:
                error_str = str(e)
                if "404" in error_str:
                    st.error("🚨 여전히 모델을 찾을 수 없습니다. API 키의 플랜(무료/유료)이나 리전을 확인해야 할 수도 있습니다.", icon="🔍")
                elif "429" in error_str:
                    st.error("⏳ 할당량 초과: 1분 뒤 재시도", icon="⚠️")
                else:
                    st.error(f"오
