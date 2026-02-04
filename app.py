import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Intelligence Dashboard", layout="wide")

# Gemini 설정 (Secrets에 gemini_api_key가 등록되어 있어야 함)
if "gemini_api_key" in st.secrets:
    genai.configure(api_key=st.secrets["gemini_api_key"])
    model = genai.GenerativeModel('gemini-1.5-flash')
    HAS_GEMINI = True
else:
    HAS_GEMINI = False

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

# 3. 데이터 추출 함수 (유입 소스 분석 추가)
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None, None, None
    
    if time_unit == "일별":
        group_sql = "CAST(date AS STRING)"
    elif time_unit == "주별":
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, WEEK) AS STRING), ' ~ ', CAST(LAST_DAY(date, WEEK) AS STRING))"
    else: 
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, MONTH) AS STRING), ' ~ ', CAST(LAST_DAY(date, MONTH) AS STRING))"

    # KPI 쿼리
    summary_query = f"""
    WITH raw_data AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as session_num,
        event_name,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{min(start_c, start_p).strftime('%Y%m%d')}' AND '{max(end_c, end_p).strftime('%Y%m%d')}'
    )
    SELECT 
        CASE 
            WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' 
            WHEN date BETWEEN '{start_p}' AND '{end_p}' THEN 'Previous' 
        END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN session_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(purchase_revenue) as revenue
    FROM raw_data WHERE session_id IS NOT NULL GROUP BY 1 HAVING type IS NOT NULL
    """

    # 시계열 쿼리
    ts_query = f"""
    SELECT {group_sql} as period_label, SUM(ecommerce.purchase_revenue) as revenue,
    COUNTIF(event_name = 'purchase') as orders,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    GROUP BY 1 ORDER BY 1
    """

    # AI를 위한 매체 분석 쿼리 추가
    source_query = f"""
    SELECT traffic_source.source, COUNTIF(event_name = 'purchase') as orders, SUM(ecommerce.purchase_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    GROUP BY 1 ORDER BY revenue DESC LIMIT 5
    """
    
    try:
        summary_df = client.query(summary_query).to_dataframe()
        ts_df = client.query(ts_query).to_dataframe()
        source_df = client.query(source_query).to_dataframe()
        return summary_df, ts_df, source_df
    except Exception as e:
        st.error(f"⚠️ 데이터 쿼리 오류: {e}")
        return None, None, None

# 4. 메인 UI
st.title("🪑 SIDIZ AI Intelligence Dashboard")

with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간", [datetime.now() - timedelta(days=8), datetime.now() - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [datetime.now() - timedelta(days=16), datetime.now() - timedelta(days=9)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_date) == 2 and len(comp_date) == 2:
    summary_df, ts_df, source_df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], time_unit)
    
    if summary_df is not None and not summary_df.empty:
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else curr

        # [섹션 1: 핵심 성과 요약]
        def calc_delta(c, p): return f"{((c - p) / p * 100):+.1f}%" if p > 0 else "0%"
        st.subheader("🎯 핵심 성과 요약")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("활성 사용자", f"{int(curr['users']):,}", calc_delta(curr['users'], prev['users']))
        c2.metric("신규 방문율", f"{(curr['new_users']/curr['users']*100 if curr['users']>0 else 0):.1f}%")
        c3.metric("총 매출액", f"₩{int(curr['revenue'] or 0):,}", calc_delta(curr['revenue'], prev['revenue']))
        c4.metric("구매전환율(CVR)", f"{(curr['orders']/curr['sessions']*100 if curr['sessions']>0 else 0):.2f}%")

        # [섹션 2: 추이 분석]
        st.markdown("---")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=ts_df['period_label'], y=ts_df['revenue'], name='매출액', marker_color='#2ca02c'))
        fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['sessions'], name='세션', yaxis='y2', line=dict(color='#1f77b4')))
        fig.update_layout(yaxis2=dict(overlaying='y', side='right'), template="plotly_white", hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # [섹션 3: 🤖 AI 데이터 인사이트 요약]
        st.markdown("---")
        st.subheader("🤖 이 기간의 AI 분석 리포트")
        
        if HAS_GEMINI:
            with st.spinner("AI가 빅쿼리 데이터를 심층 분석하고 있습니다..."):
                # AI에게 줄 데이터 컨텍스트 생성
                context = f"""
                분석 데이터 요약:
                - 매출: {int(curr['revenue']):,}원 (전기대비 {calc_delta(curr['revenue'], prev['revenue'])})
                - 주문건수: {int(curr['orders'])}건
                - 전환율: {(curr['orders']/curr['sessions']*100):.2f}%
                - 상위 유입 채널: {', '.join(source_df['source'].tolist())}
                """
                
                prompt = f"""
                당신은 시디즈의 시니어 데이터 분석가입니다. 
                아래 데이터를 바탕으로 현재 비즈니스 상황을 3줄로 요약하고, 
                매출을 높이기 위한 가장 시급한 전략 1가지를 제안해주세요.
                데이터: {context}
                """
                try:
                    ai_res = model.generate_content(prompt).text
                    st.info(ai_res)
                except:
                    st.warning("AI 분석 도중 오류가 발생했습니다.")
        else:
            st.warning("Gemini API 키가 설정되지 않았습니다.")
