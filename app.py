import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Intelligence Dashboard", layout="wide")

# 상단 안내 (정합성 관련 클레임 방지용)
st.caption("🌍 기준 타임존: Asia/Seoul | 🧩 데이터 기준: BQ Canonical (읽기 전용)")

if "gemini" in st.secrets and "gemini_api_key" in st.secrets["gemini"]:
    genai.configure(api_key=st.secrets["gemini"]["gemini_api_key"])
    model = genai.GenerativeModel('gemini-1.5-flash')
    HAS_GEMINI = True
else:
    HAS_GEMINI = False

@st.cache_resource
def get_bq_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# -------------------------------------------------
# 2. 데이터 추출 함수
# -------------------------------------------------
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None, None, None
    
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p, e_p = start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d')

    if time_unit == "일별": group_sql = "PARSE_DATE('%Y%m%d', event_date)"
    elif time_unit == "주별": group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)"
    else: group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)"

    # SQL ① 핵심 KPI (페이지뷰, 회원가입 포함)
    query = f"""
    WITH base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id, event_name, ecommerce.purchase_revenue,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    )
    SELECT 
        CASE WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions,
        COUNTIF(event_name = 'page_view') as pageviews,
        COUNTIF(event_name = 'sign_up') as sign_ups,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(IFNULL(purchase_revenue, 0)) as revenue,
        COUNTIF(event_name = 'purchase' AND purchase_revenue >= 1500000) as bulk_orders,
        SUM(CASE WHEN event_name = 'purchase' AND purchase_revenue >= 1500000 THEN purchase_revenue ELSE 0 END) as bulk_revenue
    FROM base GROUP BY 1 HAVING type IS NOT NULL
    """

    ts_query = f"""
    SELECT 
        CAST({group_sql} AS STRING) as period_label, 
        SUM(IFNULL(ecommerce.purchase_revenue, 0)) as revenue,
        COUNTIF(event_name = 'purchase' AND ecommerce.purchase_revenue >= 1500000) as bulk_orders
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY 1
    """

    source_query = f"""
    SELECT 
        CASE 
            WHEN traffic_source.source='google' AND traffic_source.medium='cpc' THEN 'Google Ads'
            WHEN traffic_source.source='naver' AND traffic_source.medium='cpc' THEN 'Naver Ads'
            WHEN traffic_source.source='meta' THEN 'Meta Ads'
            WHEN traffic_source.medium='organic' THEN 'Organic'
            ELSE 'Others'
        END AS channel_group,
        SUM(IFNULL(ecommerce.purchase_revenue, 0)) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY revenue DESC
    """

    try:
        return client.query(query).to_dataframe(), client.query(ts_query).to_dataframe(), client.query(source_query).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ 쿼리 오류: {e}")
        return None, None, None

# -------------------------------------------------
# 3. 메인 UI 및 출력
# -------------------------------------------------
st.title("🪑 SIDIZ Intelligence Dashboard")

today = datetime.now()
with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("기준 기간", [today - timedelta(days=7), timedelta(days=1) + (today - timedelta(days=2))])
    comp_date = st.date_input("비교 기간", [today - timedelta(days=14), today - timedelta(days=8)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_date) == 2 and len(comp_date) == 2:
    summary_df, ts_df, source_df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], time_unit)
    
    if summary_df is not None and not summary_df.empty:
        # --- [데이터 계산 영역: NameError 방지] ---
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else curr

        def get_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        # 비율 지표 사전 계산
        c_nv = (curr['new_users'] / curr['users'] * 100) if curr['users'] > 0 else 0
        p_nv = (prev['new_users'] / prev['users'] * 100) if prev['users'] > 0 else 0
        c_cvr = (curr['orders'] / curr['sessions'] * 100) if curr['sessions'] > 0 else 0
        p_cvr = (prev['orders'] / prev['sessions'] * 100) if prev['sessions'] > 0 else 0
        c_aov = (curr['revenue'] / curr['orders']) if curr['orders'] > 0 else 0
        p_aov = (prev['revenue'] / prev['orders']) if prev['orders'] > 0 else 0

        # --- [UI 출력 영역] ---
        st.subheader("🎯 핵심 성과 요약")
        r1 = st.columns(5)
        r2 = st.columns(5)

        # Row 1: 유입/활동
        r1[0].metric("활성 사용자", f"{int(curr['users']):,}", get_delta(curr['users'], prev['users']))
        r1[1].metric("세션 수", f"{int(curr['sessions']):,}", get_delta(curr['sessions'], prev['sessions']))
        r1[2].metric("페이지뷰(PV)", f"{int(curr['pageviews']):,}", get_delta(curr['pageviews'], prev['pageviews']))
        r1[3].metric("신규 사용자", f"{int(curr['new_users']):,}", get_delta(curr['new_users'], prev['new_users']))
        r1[4].metric("신규 방문율", f"{c_nv:.1f}%", f"{c_nv-p_nv:+.1f}%p")

        # Row 2: 전환/수익성
        r2[0].metric("회원가입 수", f"{int(curr['sign_ups']):,}", get_delta(curr['sign_ups'], prev['sign_ups']))
        r2[1].metric("주문 수", f"{int(curr['orders']):,}", get_delta(curr['orders'], prev['orders']))
        r2[2].metric("구매전환율", f"{c_cvr:.2f}%", f"{c_cvr-p_cvr:+.2f}%p")
        r2[3].metric("총 매출액", f"₩{int(curr['revenue']):,}", get_delta(curr['revenue'], prev['revenue']))
        r2[4].metric("평균 객단가(AOV)", f"₩{int(c_aov):,}", get_delta(c_aov, p_aov))

        # [대량 구매 세그먼트]
        st.markdown("---")
        st.subheader("📦 대량 구매 세그먼트 (150만 원↑)")
        b1, b2, b3 = st.columns(3)
        bulk_ratio = (curr['bulk_revenue'] / curr['revenue'] * 100) if curr['revenue'] > 0 else 0
        b1.metric("대량 주문 건수", f"{int(curr['bulk_orders'])}건", f"{int(curr['bulk_orders'] - prev['bulk_orders']):+}건")
        b2.metric("대량 구매 매출", f"₩{int(curr['bulk_revenue']):,}", get_delta(curr['bulk_revenue'], prev['bulk_revenue']))
        b3.metric("대량 구매 매출 비중", f"{bulk_ratio:.1f}%")

        # [매출 추이 차트]
        st.markdown("---")
        st.subheader(f"📊 {time_unit} 매출 추이")
        fig = go.Figure()
        fig.add_bar(x=ts_df['period_label'], y=ts_df['revenue'], name="전체 매출", marker_color='#2ca02c')
        fig.add_scatter(x=ts_df['period_label'], y=ts_df['bulk_orders'], name="대량 주문수", yaxis="y2", line=dict(color='#FF4B4B'))
        fig.update_layout(yaxis2=dict(overlaying="y", side="right"), template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

else:
    st.info("💡 사이드바에서 분석 기간을 선택해주세요.")
