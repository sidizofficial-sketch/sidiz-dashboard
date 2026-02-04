import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Dashboard", layout="wide")

# 2. BigQuery 클라이언트
@st.cache_resource
def get_bq_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except Exception as e:
        st.error(f"BQ 인증 실패: {e}")
        return None

client = get_bq_client()

# 3. 데이터 추출 함수 (전기 대비 비교 로직 포함)
def get_kpi_data(start_date, end_date):
    if client is None: return None
    
    # 분석 기간(Current) 및 전기 기간(Previous) 계산
    days_diff = (end_date - start_date).days + 1
    prev_start = start_date - timedelta(days=days_diff)
    prev_end = start_date - timedelta(days=1)
    
    query = f"""
    WITH raw_data AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as session_num,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') as eng_time,
        event_name,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{prev_start.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}'
    ),
    period_agg AS (
      SELECT 
        CASE WHEN date BETWEEN '{start_date}' AND '{end_date}' THEN 'Current' ELSE 'Previous' END as period,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN session_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions,
        COUNTIF(event_name = 'page_view') as pvs,
        COUNTIF(event_name = 'sign_up') as sign_ups,
        -- 정품등록 이벤트 (시디즈 커스텀 이벤트명 'product_registration' 가정, 확인 필요)
        COUNTIF(event_name = 'product_registration') as registrations,
        COUNTIF(event_name = 'purchase') as purchase,
        SUM(purchase_revenue) as revenue,
        COUNT(DISTINCT CASE WHEN eng_time IS NULL OR eng_time < 1000 THEN CONCAT(user_pseudo_id, CAST(session_id AS STRING)) END) as bounce_sessions
      FROM raw_data
      GROUP BY 1
    )
    SELECT * FROM period_agg
    """
    return client.query(query).to_dataframe()

# 4. 메인 UI
st.title("🪑 SIDIZ 성과 분석 대시보드")

# 사이드바
st.sidebar.header("🗓️ 기간 설정")
date_range = st.sidebar.date_input("분석 기간", [datetime.now() - timedelta(days=7), datetime.now() - timedelta(days=1)])

if len(date_range) == 2:
    start_date, end_date = date_range
    df = get_kpi_data(start_date, end_date)
    
    if not df.empty:
        # 데이터 분리 (현재 vs 전기)
        curr = df[df['period'] == 'Current'].iloc[0] if not df[df['period'] == 'Current'].empty else pd.Series(0, index=df.columns)
        prev = df[df['period'] == 'Previous'].iloc[0] if not df[df['period'] == 'Previous'].empty else pd.Series(0, index=df.columns)

        def get_delta(c_val, p_val):
            return f"{((c_val - p_val) / p_val * 100):.1f}%" if p_val > 0 else "0%"

        # 섹션 1: 유입 및 전환 (Metrics with Arrows)
        st.subheader("🚀 주요 전환 지표 (전기 대비)")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("회원가입 수", f"{int(curr['sign_ups']):,}", get_delta(curr['sign_ups'], prev['sign_ups']))
        m2.metric("정품등록 수", f"{int(curr['registrations']):,}", get_delta(curr['registrations'], prev['registrations']))
        m3.metric("구매 수", f"{int(curr['purchase']):,}", get_delta(curr['purchase'], prev['purchase']))
        m4.metric("매출액", f"₩{int(curr['revenue']):,}", get_delta(curr['revenue'], prev['revenue']))

        # 섹션 2: 트래픽 지표
        st.markdown("---")
        st.subheader("📈 트래픽 및 행동 지표")
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("세션 수", f"{int(curr['sessions']):,}", get_delta(curr['sessions'], prev['sessions']))
        
        # 이탈률 계산
        curr_br = (curr['bounce_sessions']/curr['sessions']*100) if curr['sessions'] > 0 else 0
        prev_br = (prev['bounce_sessions']/prev['sessions']*100) if prev['sessions'] > 0 else 0
        # 이탈률은 낮을수록 좋으므로 delta_color="inverse" 사용 가능 (여기선 기본형)
        t2.metric("이탈률", f"{curr_br:.1f}%", f"{(curr_br - prev_br):+.1f}%p", delta_color="inverse")
        
        # 재방문율 계산
        curr_rr = ((curr['users'] - curr['new_users'])/curr['users']*100) if curr['users'] > 0 else 0
        prev_rr = ((prev['users'] - prev['new_users'])/prev['users']*100) if prev['users'] > 0 else 0
        t3.metric("재방문율", f"{curr_rr:.1f}%", f"{(curr_rr - prev_rr):+.1f}%p")
        
        t4.metric("페이지뷰", f"{int(curr['pvs']):,}", get_delta(curr['pvs'], prev['pvs']))

    else:
        st.warning("데이터를 불러올 수 없습니다.")
