import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai
import re

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Analytics Intelligence", layout="wide")

if "gemini_api_key" in st.secrets:
    genai.configure(api_key=st.secrets["gemini_api_key"])
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

# 상품명 정제 함수
def clean_product_name(name):
    if not name or name == '(not set)': return ""
    name = name.replace('全選項', '풀옵션').replace('空中', '에어')
    for char in [' - ', ' / ', ' (', '[']:
        if char in name:
            name = name.split(char)[0]
    return name.strip()

# 3. [에러 수정] 데이터 추출 함수 (서브쿼리 최적화)
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    
    # 공통 데이터 추출 쿼리 (서브쿼리 중복 방지 위해 UNNEST 최적화)
    def build_base_query(s, e):
        return f"""
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id,
            event_name,
            ecommerce.purchase_revenue as revenue,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s}' AND '{e}'
        """

    # KPI 쿼리
    current_sql = f"""
    WITH base AS ({build_base_query(s_c, e_c)})
    SELECT 
        'Current' as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(revenue) as revenue
    FROM base WHERE sid IS NOT NULL
    """
    
    if start_p:
        prev_sql = f"""
        UNION ALL
        SELECT 'Previous' as type, COUNT(DISTINCT user_pseudo_id), COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END),
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))), COUNTIF(event_name = 'purchase'), SUM(revenue)
        FROM ({build_base_query(start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d'))}) WHERE sid IS NOT NULL
        """
        current_sql += prev_sql

    # 시계열 쿼리 (에러 방지를 위해 단순화)
    if time_unit == "일별": group_sql = "date"
    elif time_unit == "주별": group_sql = "DATE_TRUNC(date, WEEK)"
    else: group_sql = "DATE_TRUNC(date, MONTH)"

    ts_query = f"""
    WITH base AS ({build_base_query(s_c, e_c)})
    SELECT CAST({group_sql} AS STRING) as label, SUM(revenue) as revenue, 
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions
    FROM base WHERE sid IS NOT NULL GROUP BY 1 ORDER BY 1
    """

    # 매체 쿼리
    source_query = f"""
    SELECT traffic_source.source, COUNTIF(event_name='purchase') as orders, SUM(ecommerce.purchase_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY revenue DESC LIMIT 5
    """

    try:
        return client.query(current_sql).to_dataframe(), client.query(ts_query).to_dataframe(), client.query(source_query).to_dataframe()
    except Exception as e:
        st.error(f"❌ BigQuery 실행 에러: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

@st.cache_data(ttl=3600)
def get_master_item_list():
    query = "SELECT DISTINCT item_name FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item WHERE item_name IS NOT NULL"
    df = client.query(query).to_dataframe()
    df['clean_name'] = df['item_name'].apply(clean_product_name)
    return df[df['clean_name']!=""].drop_duplicates().sort_values('clean_name')

# 4. 사이드바 및 UI (이전과 동일)
with st.sidebar:
    st.header("📅 기간 설정")
    yesterday = datetime.now() - timedelta(days=1)
    curr_d = st.date_input("분석 기간", [yesterday - timedelta(days=6), yesterday])
    use_compare = st.checkbox("비교 기간 사용", value=True)
    comp_d = [yesterday - timedelta(days=13), yesterday - timedelta(days=7)]
    if use_compare:
        comp_d = st.date_input("비교 기간", comp_d)
    time_unit = st.selectbox("추이 단위", ["일별", "주별", "월별"])
    st.markdown("---")
    st.header("🔍 제품 필터 (Tab 2)")
    m_list = get_master_item_list()
    search_kw = st.text_input("상품명 검색", value="T50")
    filtered = m_list[m_list['clean_name'].str.contains(search_kw, case=False)]
    selected_names = st.multiselect("분석 상품 선택", options=filtered['clean_name'].unique())

# 5. 메인 화면 출력
tab1, tab2 = st.tabs(["📊 핵심 KPI 현황", "🪑 제품 상세 분석"])

if len(curr_d) == 2:
    kpi_df, ts_df, source_df = get_dashboard_data(curr_d[0], curr_d[1], comp_d[0] if use_compare else None, comp_d[1] if use_compare else None, time_unit)

    with tab1:
        if not kpi_df.empty:
            curr = kpi_df[kpi_df['type']=='Current'].iloc[0]
            prev = kpi_df[kpi_df['type']=='Previous'].iloc[0] if len(kpi_df) > 1 else curr
            
            # AI 분석 및 KPI 카드 렌더링 (기존 로직과 동일)
            st.subheader("🎯 핵심 성과 (전기 대비)")
            def d(c, p): return f"{((c-p)/p*100):+.1f}%" if use_compare and p > 0 else None
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("매출액", f"₩{int(curr['revenue'] or 0):,}", d(curr['revenue'], prev['revenue']))
            c2.metric("주문수", f"{int(curr['orders'] or 0):,}", d(curr['orders'], prev['orders']))
            c3.metric("세션수", f"{int(curr['sessions'] or 0):,}", d(curr['sessions'], prev['sessions']))
            c4.metric("신규 방문율", f"{(curr['new_users']/curr['users']*100 if curr['users'] > 0 else 0):.1f}%")
            
            st.markdown("---")
            # 그래프 출력
            fig = go.Figure()
            fig.add_trace(go.Bar(x=ts_df['label'], y=ts_df['revenue'], name='매출', marker_color='#2ca02c'))
            fig.add_trace(go.Scatter(x=ts_df['label'], y=ts_df['sessions'], name='세션', yaxis='y2', line=dict(color='#1f77b4')))
            fig.update_layout(yaxis2=dict(overlaying='y', side='right'), template="plotly_white", hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        if selected_names:
            conditions = " OR ".join([f"item_name LIKE '{n}%'" for n in selected_names])
            p_query = f"""
                SELECT item_name, COUNTIF(event_name='view_item') as views, COUNTIF(event_name='purchase') as orders, SUM(item_revenue) as revenue
                FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
                WHERE _TABLE_SUFFIX BETWEEN '{curr_d[0].strftime('%Y%m%d')}' AND '{curr_d[1].strftime('%Y%m%d')}'
                AND ({conditions}) GROUP BY 1
            """
            res_df = client.query(p_query).to_dataframe()
            res_df['item_name'] = res_df['item_name'].apply(clean_product_name)
            final_df = res_df.groupby('item_name').sum().reset_index()
            st.dataframe(final_df.style.format({'revenue': '₩{:,.0f}'}), use_container_width=True)
