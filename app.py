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
def get_kpi_data(current_range, compare_range, time_unit):
    if client is None: return None, None
    
    start_c, end_c = current_range
    start_p, end_p = compare_range
    
    # 시간 단위별 날짜 포맷 및 기간 레이블 설정
    if time_unit == "일별":
        group_sql = "PARSE_DATE('%Y%m%d', event_date)"
    elif time_unit == "주별":
        group_sql = "CONCAT(CAST(DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK) AS STRING), ' ~ ', CAST(LAST_DAY(PARSE_DATE('%Y%m%d', event_date), WEEK) AS STRING))"
    else: # 월별
        group_sql = "CONCAT(CAST(DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH) AS STRING), ' ~ ', CAST(LAST_DAY(PARSE_DATE('%Y%m%d', event_date), MONTH) AS STRING))"

    query = f"""
    WITH raw_data AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        {group_sql} as period_label,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as session_num,
        event_name,
        ecommerce.purchase_revenue,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') as tid
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
        COUNTIF(event_name = 'page_view') as pvs,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(purchase_revenue) as revenue
      FROM raw_data
      WHERE session_id IS NOT NULL
      GROUP BY 1
    ),
    timeseries AS (
      SELECT 
        period_label,
        SUM(purchase_revenue) as revenue,
        COUNTIF(event_name = 'purchase') as orders,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions
      FROM raw_data
      WHERE date BETWEEN '{start_c}' AND '{end_c}'
      GROUP BY 1 ORDER BY 1
    )
    SELECT * FROM summary
    """
    
    try:
        summary_df = client.query(query).to_dataframe()
        ts_df = client.query(query.split("timeseries AS (")[1].split(")")[0]).to_dataframe()
        return summary_df, ts_df
    except Exception as e:
        st.error(f"데이터 쿼리 에러: {e}")
        return None, None

# 4. UI 구성
st.title("🪑 SIDIZ 실시간 KPI 대시보드")

with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_range = st.date_input("분석 기간 선택", [datetime.now() - timedelta(days=8), datetime.now() - timedelta(days=1)])
    comp_range = st.date_input("비교 기간 선택", [datetime.now() - timedelta(days=16), datetime.now() - timedelta(days=9)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_range) == 2 and len(comp_range) == 2:
    summary_df, ts_df = get_kpi_data(curr_range, comp_range, time_unit)
    
    if summary_df is not None and not summary_df.empty:
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0] if 'Current' in summary_df['type'].values else pd.Series(0, index=summary_df.columns)
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else pd.Series(0, index=summary_df.columns)

        def calc_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        # 섹션 1: 사용자 지표
        st.subheader("👥 사용자 및 방문 지표")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("활성 사용자", f"{int(curr['users']):,}", calc_delta(curr['users'], prev['users']))
        c2.metric("신규 사용자", f"{int(curr['new_users']):,}", calc_delta(curr['new_users'], prev['new_users']))
        
        curr_nv = (curr['new_users']/curr['users']*100) if curr['users']>0 else 0
        prev_nv = (prev['new_users']/prev['users']*100) if prev['users']>0 else 0
        c3.metric("신규 방문율", f"{curr_nv:.1f}%", f"{(curr_nv-prev_nv):+.1f}%p")
        
        curr_rv = ((curr['users']-curr['new_users'])/curr['users']*100) if curr['users']>0 else 0
        prev_rv = ((prev['users']-prev['new_users'])/prev['users']*100) if prev['users']>0 else 0
        c4.metric("재방문율", f"{curr_rv:.1f}%", f"{(curr_rv-prev_rv):+.1f}%p")

        # 섹션 2: 트래픽 및 구매 (추가 지표 포함)
        st.markdown("---")
        st.subheader("💰 트래픽 및 구매 성과 (주문/전환/객단가)")
        c5, c6, c7, c8 = st.columns(4)
        c5.metric("세션 수", f"{int(curr['sessions']):,}", calc_delta(curr['sessions'], prev['sessions']))
        c6.metric("주문수", f"{int(curr['orders']):,}", calc_delta(curr['orders'], prev['orders']))
        
        curr_cr = (curr['orders']/curr['sessions']*100) if curr['sessions']>0 else 0
        prev_cr = (prev['orders']/prev['sessions']*100) if prev['sessions']>0 else 0
        c7.metric("구매전환율(CVR)", f"{curr_cr:.2f}%", f"{(curr_cr-prev_cr):+.2f}%p")
        
        curr_aov = (curr['revenue']/curr['orders']) if curr['orders']>0 else 0
        prev_aov = (prev['revenue']/prev['orders']) if prev['orders']>0 else 0
        c8.metric("평균 객단가(AOV)", f"₩{int(curr_aov):,}", calc_delta(curr_aov, prev_aov))
        
        st.metric("총 매출액", f"₩{int(curr['revenue']):,}", calc_delta(curr['revenue'], prev['revenue']))

        # 섹션 3: 추이 분석 그래프
        if ts_df is not None and not ts_df.empty:
            st.markdown("---")
            st.subheader(f"📊 {time_unit} 추이 분석 (매출액 / 주문수 / 세션)")
            
            fig = go.Figure()
            # 매출액 (Bar)
            fig.add_trace(go.Bar(x=ts_df['period_label'], y=ts_df['revenue'], name='매출액', marker_color='#2ca02c', yaxis='y1'))
            # 주문수 (Line)
            fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['orders'], name='주문수', line=dict(color='#FF4B4B', width=3), yaxis='y2'))
            # 세션 (Line)
            fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['sessions'], name='세션 수', line=dict(color='#1f77b4', width=2, dash='dot'), yaxis='y2'))

            fig.update_layout(
                yaxis=dict(title="매출액 (원)", side="left", showgrid=False, tickformat=","),
                yaxis2=dict(title="주문/세션 (건)", side="right", overlaying="y", showgrid=True, tickformat=","),
                hovermode="x unified",
                template="plotly_white",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            # K 표기 제거 (전체 숫자 표기)
            fig.update_yaxes(tickformat=",d") 
            
            st.plotly_chart(fig, use_container_width=True)
