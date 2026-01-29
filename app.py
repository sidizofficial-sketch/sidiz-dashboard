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
        # 가장 안정적인 1.5 Flash 모델 사용
        model = genai.GenerativeModel('gemini-1.5-flash') 
        st.sidebar.success("✅ 시디즈 분석 엔진 연결 완료", icon="🚀")
    else:
        st.sidebar.error("❌ API 키를 확인해주세요.", icon="🚨")
        st.stop()

    # 날짜 자동 계산
    today = datetime.date.today().strftime('%Y%m%d')

    # 3. 데이터 분석 지침 (Syntax Error 방지를 위해 구조 변경)
    # 중괄호 에러를 피하기 위해 f-string 대신 일반 문자열로 작성합니다.
    INSTRUCTION = """
    당신은 대한민국 대표 의자 브랜드 '시디즈(SIDIZ)'의 데이터 분석 전문가입니다. 
    사용자의 질문에 대해 Google Analytics 4(GA4) BigQuery 데이터를 기반으로 답변하세요.
    
    [환경 정보]
    - 프로젝트 ID: """ + str(info['project_id']) + """
    - 데이터셋: analytics_324424314
    - 테이블: events_*
    - 오늘 날짜: """ + today + """
    
    [답변 규칙]
    1. 사용자의 질문을 분석하기 위한 SQL 쿼리를 생성하세요.
    2. 생성된 쿼리의 의미를 한글로 설명하세요.
    3. 결과 데이터를 해석하여 비즈니스 인사이트를 제공하세요.
    4. 친절하고 전문적인 어조를 유지하세요.
    """

except Exception as e:
    st.error(f"초기 설정 오류: {e}", icon="🔥")
    st.stop()

# 4. UI 구성
st.title("🪑 SIDIZ Data Intelligence Portal")
st.markdown("---")

if "messages" not in st.session_state:
    st.session_
