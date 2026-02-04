import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Intelligence Dashboard", layout="wide")

# 2. BigQuery 클라이언트
@st.cache_resource
def get_bq_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# 3. 데이터 추출 함수
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None, None
    
    # 시간 단위별 레이블 설정 (SQL 내에서 가독성을 위해 미리 문자열 생성)
    if time_unit == "일별":
        group_sql = "CAST(date AS STRING)"
    elif time_unit == "주별":
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, WEEK) AS STRING), ' ~ ', CAST(LAST_DAY(date, WEEK) AS STRING))"
    else: # 월별
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, MONTH) AS STRING), ' ~ ', CAST(LAST_DAY(date, MONTH) AS STRING))"

    # 1. 요약 데이터용 쿼리 (Current vs Previous)
    # f-string 내부에 중괄호가 겹치지 않도록 주의하여 작성
    summary_query = f"""
    WITH raw_data AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as session_num,
        event_name,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{min(start_c,
