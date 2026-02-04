import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Dashboard", page_icon="🪑", layout="wide")

# 2. BigQuery 클라이언트 (인증 및 클라이언트 생성)
@st.cache_resource
def get_bq_client():
    try:
        # Streamlit Secrets 사용
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# 3. 데이터 추출 및 분석 함수
def get_dashboard_data(start_date, end_date, time_unit):
    if client is None: return None, None
    
    # 분석 기간(Current) 및 전기 기간(Previous) 계산
    days_diff = (end_date - start_date).days + 1
    prev_start = start_date - timedelta(days=days_diff)
    prev_end = start_date - timedelta(days=1)
    
    # 시간 단위별 그룹화 SQL (시각화용)
    if time_unit == "일별":
        group_sql = "PARSE_DATE('%Y%m%d', event_date)"
    elif time_unit == "주별":
        group_sql = "LAST_DAY(PARSE_DATE('%Y%m%d', event_date), WEEK)"
    else: # 월별
        group_sql = "LAST_DAY(PARSE_DATE('%Y%m%d', event_date), MONTH)"

    query = f"""
    WITH raw_data AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        {group_sql} as period,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as session_num,
        event_name,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{prev_start.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}'
    ),
    summary AS (
      SELECT 
        CASE WHEN date BETWEEN '{start_date}' AND '{end_date}' THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN session_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions,
        COUNTIF(event_name = 'page_view') as pvs,
        COUNTIF(event_name = 'sign_up') as sign_ups,
        COUNTIF(event_name = 'purchase') as purchase,
        SUM(purchase_revenue) as revenue
      FROM raw_data
      WHERE session_id IS NOT NULL
      GROUP BY 1
    ),
    timeseries AS (
      SELECT 
        period,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions
      FROM raw_data
      WHERE date BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY 1
      ORDER BY 1
    )
    SELECT * FROM summary
    """
    
    try:
        # 요약 데이터 가져오기
        summary_df = client.query(query).to_dataframe()
        
        # 시계열 데이터 가져오기 (위 WITH 절의 timeseries 부분만 별도 실행)
        ts_query = f"""
        WITH ts_raw AS (
            SELECT {group_sql} as period, user_pseudo_id, 
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id
            FROM `sidiz-458301.analytics_487246344.events_*`
            WHERE _TABLE_SUFFIX BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}'
        )
        SELECT period, COUNT(DISTINCT user_pseudo_id) as users, COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions
        FROM ts_raw GROUP BY 1 ORDER BY 1
        """
        ts_df = client.query(ts_query).to_dataframe()
        return summary_df, ts_df
    except Exception as e:
        st.error(f"데이터 로드 에러: {e}")
        return None, None

# 4. 메인 화면 구성
st.title("🪑 SIDIZ 실시간 KPI 대시보드")

# 사이드바 (기간 및 단위 선택 기능 보강)
with st.sidebar:
    st.header("⚙️ 분석 설정")
    date_range = st.date_input("분석 기간 선택", [datetime.now() - timedelta(days=8), datetime.now() - timedelta(days=1)])
    
    time_unit = st.selectbox("분석 단위 (차트)", ["일별", "주별", "월별"], index=0)
    
    if len(date_range) == 2:
        diff = (date_range[1] - date_range[0]).days + 1
        st.info(f"💡 비교 기간: 그 직전 {diff}일이 자동 선택됩니다.")

# 5. 메인 로직 실행
if len(date_range) == 2:
    start_date, end_date = date_range
    summary_df, ts_df = get_dashboard_data(start_date, end_date, time_unit)
    
    if summary_df is not None and not summary_df.empty:
        # 데이터 매핑 (Current / Previous)
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0] if 'Current' in summary_df['type'].values else pd.Series(0, index=summary_df.columns)
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else pd.Series(0, index=summary_df.columns)

        # 신규 방문율 계산 (%)
        curr_nv_rate = (curr['new_users'] / curr['users'] * 100) if curr['users'] > 0 else 0
        prev_nv_rate = (prev['new_users'] / prev['users'] * 100) if prev['users'] > 0 else 0
        
        # 재방문율 계산 (%)
        curr_rv_rate = ((curr['users'] - curr['new_users']) / curr['users'] * 100) if curr['users'] > 0 else 0
        prev_rv_rate = ((prev['users'] - prev['new_users']) / prev['users'] * 100) if prev['users'] > 0 else 0

        def calc_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        # KPI 섹션 1: 사용자 및 방문
        st.subheader("👥 사용자 및 방문 지표 (전기 대비)")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("활성 사용자", f"{int(curr['users']):,}", calc_delta(curr['users'], prev['users']))
        col2.metric("신규 사용자", f"{int(curr['new_users']):,}", calc_delta(curr['new_users'], prev['new_users']))
        col3.metric("신규 방문율", f"{curr_nv_rate:.1f}%", f"{(curr_nv_rate - prev_nv_rate):+.1f}%p")
        col4.metric("재방문율", f"{curr_rv_rate:.1f}%", f"{(curr_rv_rate - prev_rv_rate):+.1f}%p")

        # KPI 섹션 2: 트래픽 및 성과
        st.markdown("---")
        st.subheader("💰 트래픽 및 구매 성과")
        col5, col6, col7, col8 = st.columns(4)
        col5.metric("세션 수", f"{int(curr['sessions']):,}", calc_delta(curr['sessions'], prev['sessions']))
        col6.metric("페이지뷰", f"{int(curr['pvs']):,}", calc_delta(curr['pvs'], prev['pvs']))
        col7.metric("회원가입 수", f"{int(curr['sign_ups']):,}", calc_delta(curr['sign_ups'], prev['sign_ups']))
        col8.metric("매출액", f"₩{int(curr['revenue']):,}", calc_delta(curr['revenue'], prev['revenue']))

        # 6. 차트 섹션
        if ts_df is not None and not ts_df.empty:
            st.markdown("---")
            st.subheader(f"📈 {time_unit} 추이 분석 (활성 사용자 & 세션)")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=ts_df['period'], y=ts_df['users'], name='활성 사용자', line=dict(color='#FF4B4B', width=3)))
            fig.add_trace(go.Bar(x=ts_df['period'], y=ts_df['sessions'], name='세션 수', opacity=0.3, marker_color='gray'))
            fig.update_layout(
                hovermode="x unified",
                template="plotly_white",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("선택한 기간에 데이터가 없습니다.")
else:
    st.info("왼쪽 사이드바에서 날짜 범위를 선택해주세요.")
