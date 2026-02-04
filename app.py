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

# [기능] 상품명 정제 (한자 및 옵션 제거)
def clean_product_name(name):
    if not name or name == '(not set)': return ""
    # 한자 정제 (필요시)
    name = name.replace('全選項', '풀옵션').replace('空中', '에어')
    # 옵션 구분자 제거
    for char in [' - ', ' / ', ' (', '[']:
        if char in name:
            name = name.split(char)[0]
    return name.strip()

# 3. 데이터 추출 함수 (KPI, 매체, 시계열)
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p = start_p.strftime('%Y%m%d') if start_p else None
    e_p = end_p.strftime('%Y%m%d') if end_p else None

    # KPI 쿼리 (신규사용자, 세션, 주문, 매출)
    def kpi_sql(s, e, label):
        return f"""
        SELECT 
            '{label}' as type,
            COUNT(DISTINCT user_pseudo_id) as users,
            COUNT(DISTINCT CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) = 1 THEN user_pseudo_id END) as new_users,
            COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions,
            COUNTIF(event_name = 'purchase') as orders,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s}' AND '{e}'
        """
    
    full_kpi_query = kpi_sql(s_c, e_c, 'Current')
    if s_p:
        full_kpi_query += f" UNION ALL {kpi_sql(s_p, e_p, 'Previous')}"

    # 시계열 쿼리
    if time_unit == "일별": group_sql = "date"
    elif time_unit == "주별": group_sql = "DATE_TRUNC(date, WEEK)"
    else: group_sql = "DATE_TRUNC(date, MONTH)"

    ts_query = f"""
    SELECT CAST({group_sql} AS STRING) as label, SUM(ecommerce.purchase_revenue) as revenue, 
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY 1
    """

    # 매체 쿼리
    source_query = f"""
    SELECT traffic_source.source, COUNTIF(event_name='purchase') as orders, SUM(ecommerce.purchase_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY revenue DESC LIMIT 5
    """

    return client.query(full_kpi_query).to_dataframe(), client.query(ts_query).to_dataframe(), client.query(source_query).to_dataframe()

@st.cache_data(ttl=3600)
def get_master_item_list():
    query = "SELECT DISTINCT item_name FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item WHERE item_name IS NOT NULL"
    df = client.query(query).to_dataframe()
    df['clean_name'] = df['item_name'].apply(clean_product_name)
    return df[df['clean_name']!=""].drop_duplicates().sort_values('clean_name')

# 4. 사이드바 설정
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
    st.header("🔍 제품 검색 필터 (Tab 2)")
    m_list = get_master_item_list()
    search_kw = st.text_input("상품명 검색", value="T50")
    filtered = m_list[m_list['clean_name'].str.contains(search_kw, case=False)]
    selected_names = st.multiselect("분석 상품 선택", options=filtered['clean_name'].unique())

# 5. 메인 로직
tab1, tab2 = st.tabs(["📊 핵심 KPI 현황", "🪑 제품 상세 분석"])

if len(curr_d) == 2:
    kpi_df, ts_df, source_df = get_dashboard_data(curr_d[0], curr_d[1], comp_d[0] if use_compare else None, comp_d[1] if use_compare else None, time_unit)

    with tab1:
        if not kpi_df.empty:
            curr = kpi_df[kpi_df['type']=='Current'].iloc[0]
            prev = kpi_df[kpi_df['type']=='Previous'].iloc[0] if len(kpi_df) > 1 else curr

            # AI 분석
            if HAS_GEMINI:
                model = genai.GenerativeModel('gemini-1.5-flash')
                insight = model.generate_content(f"시디즈 매출 {curr['revenue']:,}원 성과를 분석해줘").text
                st.info(f"🤖 AI 분석: {insight}")

            st.subheader("🎯 핵심 성과 (전기 대비)")
            def d(c, p): return f"{((c-p)/p*100):+.1f}%" if use_compare and p > 0 else None
            
            # KPI 행 1
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("매출액", f"₩{int(curr['revenue']):,}", d(curr['revenue'], prev['revenue']))
            c2.metric("주문수", f"{int(curr['orders']):,}", d(curr['orders'], prev['orders']))
            c3.metric("세션수", f"{int(curr['sessions']):,}", d(curr['sessions'], prev['sessions']))
            c4.metric("신규 방문율", f"{(curr['new_users']/curr['users']*100):.1f}%")

            # KPI 행 2 (개선 포인트: 전환율 및 객단가 추가)
            st.markdown("---")
            c5, c6, c7, c8 = st.columns(4)
            curr_cvr = (curr['orders']/curr['sessions']*100) if curr['sessions']>0 else 0
            prev_cvr = (prev['orders']/prev['sessions']*100) if prev['sessions']>0 else 0
            c5.metric("구매전환율(CVR)", f"{curr_cvr:.2f}%", d(curr_cvr, prev_cvr))
            
            curr_aov = (curr['revenue']/curr['orders']) if curr['orders']>0 else 0
            prev_aov = (prev['revenue']/prev['orders']) if prev['orders']>0 else 0
            c6.metric("평균객단가(AOV)", f"₩{int(curr_aov):,}", d(curr_aov, prev_aov))
            c7.metric("활성 사용자(AU)", f"{int(curr['users']):,}")
            c8.metric("인당 세션수", f"{(curr['sessions']/curr['users'] if curr['users']>0 else 0):.1f}")

            # 그래프 및 매체
            st.markdown("---")
            col_g, col_s = st.columns([2, 1])
            with col_g:
                st.subheader("매출 및 세션 추이")
                fig = go.Figure()
                fig.add_trace(go.Bar(x=ts_df['label'], y=ts_df['revenue'], name='매출', marker_color='#2ca02c'))
                fig.add_trace(go.Scatter(x=ts_df['label'], y=ts_df['sessions'], name='세션', yaxis='y2', line=dict(color='#1f77b4')))
                fig.update_layout(yaxis2=dict(overlaying='y', side='right'), template="plotly_white", hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)
            with col_s:
                st.subheader("유입 매체 Top 5")
                st.table(source_df)

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
            
            st.subheader(f"🔍 선택 상품 통합 분석 ({len(selected_names)}종)")
            st.dataframe(final_df.style.format({'revenue': '₩{:,.0f}'}), use_container_width=True)
            
            # 상품간 매출 비중 차트 추가
            fig_p = go.Figure(data=[go.Pie(labels=final_df['item_name'], values=final_df['revenue'], hole=.3)])
            st.plotly_chart(fig_p)
        else:
            st.info("왼쪽 사이드바에서 분석할 상품을 선택해 주세요.")
