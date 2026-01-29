import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime
import time

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
        
        # 가장 안정적인 1.5 Flash 모델 사용 (경로 없이 입력)
        model = genai.GenerativeModel('gemini-1.5-flash') 
        st.sidebar.success("✅ 시디즈 분석 엔진 연결 완료", icon="🚀")
    else:
        st.sidebar.error("❌ API 키를 확인해주세요.", icon="🚨")
        st.stop()

    # 날짜 자동 계산
    today = datetime.date.today().strftime('%Y%m%d')

    # 3. 데이터 분석 지침 (프롬프트 엔지니어링)
    # 아래 문자열이 정확히 따옴표 3개로 닫혀야 SyntaxError가 나지 않습니다.
    INSTRUCTION = f"""
    당신은 대한민국 대표 의자 브랜드 '시디즈(SIDIZ)'의 데이터 분석 전문가입니다. 
    사용자의 질문에 대해 Google Analytics 4(GA4) BigQuery 데이터를 기반으로 답변하세요.
    
    [환경 정보]
    - 프로젝트 ID: {info['project_id']}
    - 데이터셋: analytics_324424314
    - 테이블: events_*
    - 오늘 날짜: {today}
