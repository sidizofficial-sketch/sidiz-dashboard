import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import re
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import datetime, timedelta

# --------------------------------------------------
# 1. 페이지 설정
# --------------------------------------------------
st.set_page_config(page_title="SIDIZ AI Dashboard", page_icon="🪑", layout="wide")
st.title("🪑 SIDIZ AI Intelligence Dashboard")

# --------------------------------------------------
# 2. 보안 및 클라이언트 설정
# --------------------------------------------------
try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")

    genai.configure(api_key=st.secrets["gemini"]["api_key"])
    model = genai.GenerativeModel("models/gemini-1.5-flash")

    naver_client_id = st.secrets.get("naver", {}).get("client_id")
    naver_client_secret = st.secrets.get("naver", {}).get("client_secret")

    project_id = info["project_id"]
    dataset_id = "analytics_487246344"
    table_path = f"{project_id}.{dataset_id}.events_*"

except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# --------------------------------------------------
# 3. 네이버 검색 트렌드 함수
# --------------------------------------------------
def get_naver_search_trend(keywords, start_date, end_date, time_unit="date"):
    if not naver_client_id or not naver_client_secret:
        return None, "네이버 API 키 없음"

    url = "https://openapi.naver.com/v1/datalab/search"
    headers = {
        "X-Naver-Client-Id": naver_client_id,
        "X-Naver-Client-Secret": naver_client_secret,
        "Content-Type": "application/json"
    }

    body = {
        "startDate": start_date,
        "endDate": end_date_
