import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Analytics", layout="wide")

# Gemini 설정 (Secrets에 키가 있는 경우만)
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

# 3. 데이터 추출 함수 (KPI, 매체, 시계열)
def get_combined_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None
    
    # 시간 단위 레이블
    if time_unit == "일별": group_sql = "CAST(date AS STRING)"
    elif time_unit == "주별": group_sql = "CONCAT(CAST(DATE_TRUNC(date, WEEK) AS STRING), ' ~ ', CAST(LAST_DAY(date, WEEK) AS STRING))"
    else: group_sql = "CONCAT(CAST(DATE_TRUNC(date, MONTH) AS STRING), ' ~ ', CAST(LAST_DAY(date, MONTH) AS STRING))"

    # 비교 기간 유무에 따른 날짜 설정
    min_date = start_p if start_p else start_c
    max_date = end_c

    query = f"""
    WITH raw AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date, user_pseudo_id, event_name, ecommerce.purchase_revenue,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{min_date.strftime('%Y%m%d')}' AND '{max_date.strftime('%Y%m%d')}'
    ),
    summary AS (
      SELECT 
        CASE 
          WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' 
          {f"WHEN date BETWEEN '{start_p}' AND '{end_p}' THEN 'Previous'" if start_p else ""}
        END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(purchase_revenue) as revenue
      FROM raw WHERE sid IS NOT NULL GROUP BY 1 HAVING type IS NOT NULL
    ),
    ts AS (
      SELECT {group_sql} as period_label, SUM(purchase_revenue) as revenue, COUNTIF(event_name = 'purchase') as orders,
      COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions
      FROM raw WHERE date BETWEEN '{start_c}' AND '{end_c}' GROUP BY 1 ORDER BY 1
    )
    SELECT * FROM summary
    """
    try:
        kpi_df = client.query(query).to_dataframe()
        ts_df = client.query(query.split("ts AS (")[1].split(")")[0]).to_dataframe()
        return kpi_df, ts_df
    except: return None, None

@st.cache_data(ttl=3600)
def get_master_item_list():
    if client is None: return pd.DataFrame()
    query = "SELECT DISTINCT item_name FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item WHERE item_name IS NOT NULL AND item_name != '(not set)'"
    return client.query(query).to_dataframe()

# 4. 사이드바 구성
with st.sidebar:
    st.header("📅 기간 설정")
    # 분석 기간: 최근 7일 (어제 날짜 기준)
    yesterday = datetime.now() - timedelta(days=1)
    seven_days_ago = yesterday - timedelta(days=6)
    curr_d = st.date_input("분석 기간 (Current)", [seven_days_ago, yesterday])
    
    # 비교 기간: 기본 선택 없음 (체크박스로 활성화)
    use_compare = st.checkbox("비교 기간 사용 (Previous)")
    comp_d = [None, None]
    if use_compare:
        comp_d = st.date_input("비교 기간 선택", [seven_days_ago - timedelta(days=7), yesterday - timedelta(days=7)])

    time_unit = st.selectbox("추이 단위", ["일별", "주별", "월별"])
    
    st.markdown("---")
    st.header("🔍 제품 필터 (Tab 2)")
    master_items = get_master_item_list()
    search_kw = st.text_input("제품명 키워드 검색", value="T50")
    
    selected_names = []
    if not master_items.empty:
        filtered = master_items[master_items['item_name'].str.contains(search_kw, case=False, na=False)]
        selected_names = st.multiselect("분석할 상품명 선택", options=filtered['item_name'].unique())

# 5. 메인 화면 - 탭 구성
tab1, tab2 = st.tabs(["📊 전체 KPI 현황", "🪑 제품별 상세 분석"])

# --- Tab 1: 전체 KPI ---
with tab1:
    if len(curr_d) == 2:
        kpi_df, ts_df = get_combined_data(curr_d[0], curr_d[1], comp_d[0] if use_compare else None, comp_d[1] if use_compare else None, time_unit)
        
        if kpi_df is not None and not kpi_df.empty:
            curr = kpi_df[kpi_df['type']=='Current'].iloc[0]
            has_prev = 'Previous' in kpi_df['type'].values
            prev = kpi_df[kpi_df['type']=='Previous'].iloc[0] if has_prev else curr

            st.subheader("🎯 핵심 성과 요약")
            c1, c2, c3, c4 = st.columns(4)
            def get_delta(c, p): return f"{((c-p)/p*100):+.1f}%" if has_prev and p > 0 else None
            
            c1.metric("매출액", f"₩{int(curr['revenue']):,}", get_delta(curr['revenue'], prev['revenue']))
            c2.metric("주문수", f"{int(curr['orders']):,}", get_delta(curr['orders'], prev['orders']))
            c3.metric("세션", f"{int(curr['sessions']):,}", get_delta(curr['sessions'], prev['sessions']))
            c4.metric("구매전환율", f"{(curr['orders']/curr['sessions']*100 if curr['sessions']>0 else 0):.2f}%")

            # 그래프
            st.markdown("---")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=ts_df['period_label'], y=ts_df['revenue'], name='매출', marker_color='#2ca02c'))
            fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['sessions'], name='세션', yaxis='y2', line=dict(color='#1f77b4')))
            fig.update_layout(yaxis2=dict(overlaying='y', side='right'), template="plotly_white", hovermode="x unified")
            fig.update_yaxes(tickformat=",d")
            st.plotly_chart(fig, use_container_width=True)

# --- Tab 2: 제품 상세 ---
with tab2:
    if not selected_names:
        st.info("사이드바에서 상품명을 검색하고 선택해 주세요.")
    else:
        formatted_names = ", ".join([f"'{n}'" for n in selected_names])
        p_query = f"""
            SELECT item_name, COUNTIF(event_name='view_item') as views, COUNTIF(event_name='purchase') as orders, SUM(item_revenue) as revenue
            FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
            WHERE _TABLE_SUFFIX BETWEEN '{curr_d[0].strftime('%Y%m%d')}' AND '{curr_d[1].strftime('%Y%m%d')}'
            AND item_name IN ({formatted_names}) GROUP BY 1 ORDER BY revenue DESC
        """
        res_df = client.query(p_query).to_dataframe()
        st.subheader(f"🔍 선택 상품 성과 ({len(selected_names)}건)")
        st.dataframe(res_df.style.format({'revenue': '₩{:,.0f}'}), use_container_width=True)
