import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정 및 API 키
st.set_page_config(page_title="SIDIZ Advanced Analytics", layout="wide")

# Secrets에 등록된 경우에만 Gemini 활성화
if "gemini_api_key" in st.secrets:
    genai.configure(api_key=st.secrets["gemini_api_key"])

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

# 3. [보정된 함수] 데이터 추출 (KPI, 매체별, 상품 고유코드 분석 통합)
def get_all_dashboard_data(start_c, end_c, start_p, end_p, time_unit, item_identifiers):
    if client is None: return None, None, None, None
    
    # 시간 단위별 레이블
    if time_unit == "일별":
        group_sql = "CAST(date AS STRING)"
    elif time_unit == "주별":
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, WEEK) AS STRING), ' ~ ', CAST(LAST_DAY(date, WEEK) AS STRING))"
    else: 
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, MONTH) AS STRING), ' ~ ', CAST(LAST_DAY(date, MONTH) AS STRING))"

    # 상품 식별자 처리 (IN 절용)
    id_list = [x.strip() for x in item_identifiers.split(',')]
    formatted_ids = ", ".join([f"'{i}'" for i in id_list])

    # [A] 메인 KPI 쿼리
    kpi_query = f"""
    WITH raw AS (
      SELECT PARSE_DATE('%Y%m%d', event_date) as date, user_pseudo_id, event_name, ecommerce.purchase_revenue,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as sid,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as s_num
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{min(start_c, start_p).strftime('%Y%m%d')}' AND '{max(end_c, end_p).strftime('%Y%m%d')}'
    )
    SELECT 
        CASE WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(purchase_revenue) as revenue
    FROM raw WHERE sid IS NOT NULL GROUP BY 1 HAVING type IS NOT NULL
    """

    # [B] 매체별 성과 쿼리
    source_query = f"""
    SELECT traffic_source.source, traffic_source.medium,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))) as sessions,
    COUNTIF(event_name = 'purchase') as orders,
    SUM(ecommerce.purchase_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    GROUP BY 1, 2 ORDER BY revenue DESC LIMIT 5
    """

    # [C] 고유 상품 정밀 분석 쿼리
    product_query = f"""
    SELECT item.item_id, item.item_name,
    COUNTIF(event_name = 'view_item') as views,
    COUNTIF(event_name = 'purchase') as orders,
    SUM(item.item_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
    WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    AND (item.item_id IN ({formatted_ids}) OR item.item_name IN ({formatted_ids}))
    GROUP BY 1, 2 ORDER BY revenue DESC
    """

    # [D] 시계열 쿼리
    ts_query = f"""
    SELECT {group_sql} as period_label, SUM(ecommerce.purchase_revenue) as revenue, COUNTIF(event_name = 'purchase') as orders,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))) as sessions
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    GROUP BY 1 ORDER BY 1
    """

    try:
        kpi_df = client.query(kpi_query).to_dataframe()
        source_df = client.query(source_query).to_dataframe()
        prod_df = client.query(product_query).to_dataframe()
        ts_df = client.query(ts_query).to_dataframe()
        return kpi_df, source_df, prod_df, ts_df
    except Exception as e:
        st.error(f"쿼리 실패: {e}")
        return None, None, None, None

# 4. Gemini 인사이트 함수
def get_ai_insight(curr):
    if "gemini_api_key" not in st.secrets: return "API 키를 설정해주세요."
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"시디즈 매출 {curr['revenue']:,}원, 주문 {curr['orders']:,}건입니다. 성과를 분석하고 짧은 전략을 제안해줘."
        return model.generate_content(prompt).text
    except: return "인사이트를 생성할 수 없습니다."

# 5. UI 구성
st.title("🪑 SIDIZ AI Intelligence Dashboard")

with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_d = st.date_input("분석 기간", [datetime.now()-timedelta(days=8), datetime.now()-timedelta(days=1)])
    comp_d = st.date_input("비교 기간", [datetime.now()-timedelta(days=16), datetime.now()-timedelta(days=9)])
    time_unit = st.selectbox("추이 단위", ["일별", "주별", "월별"])
    st.markdown("---")
    item_input = st.text_area("고유 상품코드/명 (쉼표 구분)", value="T500HLDA, TN500HLDA")

# 6. 메인 로직 출력
if len(curr_d) == 2 and len(comp_d) == 2:
    kpi_df, source_df, prod_df, ts_df = get_all_dashboard_data(curr_d[0], curr_d[1], comp_d[0], comp_d[1], time_unit, item_input)
    
    if kpi_df is not None and not kpi_df.empty:
        curr = kpi_df[kpi_df['type']=='Current'].iloc[0]
        prev = kpi_df[kpi_df['type']=='Previous'].iloc[0] if 'Previous' in kpi_df['type'].values else curr

        # [섹션 1: AI 인사이트]
        st.info(f"🤖 **AI 분석:** {get_ai_insight(curr)}")

        # [섹션 2: 주요 KPI]
        st.subheader("🎯 핵심 성과 (전기 대비)")
        k1, k2, k3, k4 = st.columns(4)
        def delta(c, p): return f"{((c-p)/p*100):+.1f}%" if p > 0 else "0%"
        k1.metric("매출액", f"₩{int(curr['revenue']):,}", delta(curr['revenue'], prev['revenue']))
        k2.metric("주문수", f"{int(curr['orders']):,}", delta(curr['orders'], prev['orders']))
        k3.metric("세션", f"{int(curr['sessions']):,}", delta(curr['sessions'], prev['sessions']))
        k4.metric("신규방문율", f"{(curr['new_users']/curr['users']*100):.1f}%")

        # [섹션 3: 상품 및 매체 분석]
        st.markdown("---")
        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("📍 고유 상품별 성과")
            st.dataframe(prod_df, use_container_width=True)
        with col_right:
            st.subheader("🌐 주요 유입 매체")
            st.dataframe(source_df, use_container_width=True)

        # [섹션 4: 추이 그래프]
        st.markdown("---")
        st.subheader(f"📊 {time_unit} 매출 및 세션 추이")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=ts_df['period_label'], y=ts_df['revenue'], name='매출', marker_color='#2ca02c'))
        fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['sessions'], name='세션', yaxis='y2', line=dict(color='#1f77b4')))
        fig.update_layout(yaxis2=dict(overlaying='y', side='right'), hovermode="x unified", template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)
