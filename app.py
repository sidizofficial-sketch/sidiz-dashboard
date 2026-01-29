import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime
import time  # 시간 지연을 위해 추가

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
        
        # [수정] 가장 안정적인 1.5 Flash 모델로 명확히 지정
        model = genai.GenerativeModel('gemini-1.5-flash') 
        st.sidebar.success("✅ 시디즈 분석 엔진 연결 완료", icon="🚀")
    else:
        st.sidebar.error("❌ API 키를 확인해주세요.", icon="🚨")
        st.stop()

    # 날짜 자동 계산
    today = datetime.date.today().strftime('%Y%m%d')

    # 3. 데이터 분석 지침
    INSTRUCTION = f"""
    당신은 시디즈(SIDIZ)의 데이터 전문가입니다. 
    - 프로젝트 ID: `{info['project_id']}`
    - 데이터셋: `analytics_324424314`
    - 테이블: `events_*`
    - 오늘 날짜: {today}
    
    [규칙] 사용자의 질문에 대해 빅쿼리 SQL을 작성하고 결과를 한글로 친절하게 설명하세요.
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
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("AI가 데이터를 분석 중입니다..."):
            try:
                # [강화] 지침과 질문 결합
                full_query = f"{INSTRUCTION}\n\n사용자 질문: {prompt}"
                
                # API 호출 (재시도 로직 포함)
                response = model.generate_content(full_query)
                
                if response:
                    answer = response.text
                    st.markdown(answer)
                    st.session_state.messages.append({"role": "assistant", "content": answer})
                
            except Exception as e:
                if "429" in str(e):
                    # 429 에러 발생 시 사용자에게 더 친절한 가이드 제공
                    st.warning("⚠️ 현재 구글 서버의 무료 할당량이 꽉 찼습니다.", icon="⏳")
                    st.info("💡 **해결 방법:** 1분 뒤에 다시 질문하거나, AI Studio에서 '결제(Billing)'를 등록하면 즉시 해결됩니다.")
                else:
                    st.error(f"분석 중 오류 발생: {e}", icon="🚨")
