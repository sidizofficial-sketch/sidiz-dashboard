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
def get_dashboard_data(curr_range, comp_range, time_unit):
    if client is None: return None, None
    
    start_c, end_c = curr_range
    start_p, end_p = comp_range
    
    # 시간 단위별 레이블 설정
    if time_unit == "일별":
        group_sql = "CAST(date AS STRING)"
    elif time_unit == "주별":
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, WEEK) AS STRING), ' ~ ', CAST(LAST_DAY(date, WEEK) AS STRING))"
    else: # 월별
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, MONTH) AS STRING), ' ~ ', CAST(LAST_DAY(date, MONTH) AS STRING))"

    # 전체 쿼리 (통합)
    main_query = f"""
    WITH raw_data AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as session_num,
        event_name,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{min(start_c, start_p).strftime('%Y%m%d')}' AND '{max(end_c, end_p).strftime('%Y%m%d')}'
    ),
    summary AS (
      SELECT 
        CASE 
            WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' 
            WHEN date BETWEEN '{start_p}' AND '{end_p}' THEN 'Previous' 
        END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN session_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(purchase_revenue) as revenue,
        COUNTIF(event_name = 'page_view') as pvs
      FROM raw_data
      WHERE session_id IS NOT NULL
      GROUP BY 1
    ),
    timeseries AS (
      SELECT 
        {group_sql} as period_label,
        SUM(purchase_revenue) as revenue,
        COUNTIF(event_name = 'purchase') as orders,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions
      FROM raw_data
      WHERE date BETWEEN '{start_c}' AND '{end_c}'
