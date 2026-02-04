import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Intelligence", layout="wide")

# Gemini 설정
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
    except: return None

client = get_bq_client()

# 3. 데이터 추출 함수 (에러 수정 및 기능 확장)
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None, None, None
    
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p, e_p = start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d')

    # [에러 해결] date 대신 event_date를 PARSE_DATE하여 사용
    if time_unit == "일별": group_sql = "PARSE_DATE('%Y%m%d', event_date)"
    elif time_unit == "주별": group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)"
    else: group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)"

    # KPI 쿼리
    summary_query = f"""
    SELECT 
        CASE 
            WHEN _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}' THEN 'Current' 
            WHEN _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}' THEN 'Previous' 
        END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(ecommerce.purchase_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    GROUP BY 1 HAVING type IS NOT NULL
    """

    # 시계열 쿼리 (Unrecognized name: date 에러 수정 완료)
    ts_query = f"""
    SELECT CAST({group_sql} AS STRING) as period_label, SUM(ecommerce.purchase_revenue) as revenue,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY 1
    """

    source_query = f"""
    SELECT traffic_source.source, SUM(ecommerce.purchase_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY revenue DESC LIMIT 5
    """
    
    return client.query(summary_query).to_dataframe(), client.query(ts_query).to_dataframe(), client.query(source_query).to_dataframe()

# 4. UI 구성
st.title("🪑 SIDIZ AI Intelligence Dashboard")

with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간", [datetime.now() - timedelta(days=8), datetime.now() - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [datetime.now() - timedelta(days=16), datetime.now() - timedelta(days=9)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])
    
    st.markdown("---")
    # 상품 리스트 (한자 정제 생략 버전)
    @st.cache_data
    def get_items():
        return client.query("SELECT DISTINCT item_name FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item").to_dataframe()['item_name'].dropna().unique()
    
    selected_prods = st.multiselect("분석 제품 선택 (Tab 2용)", options=get_items(), default=[])

tab1, tab2 = st.tabs(["📊 전체 성과 요약", "🪑 제품 상세 분석"])

if len(curr_date) == 2 and len(comp_date) == 2:
    summary_df, ts_df, source_df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], time_unit)

    with tab1:
        if summary_df is not None and not summary_df.empty:
            curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
            prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else curr
            
            # KPI 메트릭 및 그래프 (기존 유지)
            st.subheader("🎯 핵심 성과")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("매출액", f"₩{int(curr['revenue'] or 0):,}")
            c2.metric("세션수", f"{int(curr['sessions']):,}")
            c3.metric("전환율", f"{(curr['orders']/curr['sessions']*100):.2f}%")
            c4.metric("주문수", f"{int(curr['orders']):,}")

            # AI 분석 리포트
            st.markdown("---")
            if HAS_GEMINI:
                context = f"매출 {int(curr['revenue']):,}원, 전환율 {(curr['orders']/curr['sessions']*100):.2f}%, 주요유입 {source_df['source'].tolist()}"
                st.info(f"🤖 **AI 요약:** {model.generate_content(f'{context} 분석해줘').text}")

    with tab2:
        if selected_prods:
            formatted_p = ", ".join([f"'{p}'" for p in selected_prods])
            # 제품별 + 전체평균 비교 쿼리
            p_query = f"""
            SELECT 
                item_name,
                COUNTIF(event_name='view_item') as pv,
                COUNTIF(event_name='purchase') as orders,
                SUM(item_revenue) as revenue,
                device.category as device
            FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
            WHERE _TABLE_SUFFIX BETWEEN '{curr_date[0].strftime('%Y%m%d')}' AND '{curr_date[1].strftime('%Y%m%d')}'
            AND item_name IN ({formatted_p})
            GROUP BY 1, 5
            """
            p_df = client.query(p_query).to_dataframe()
            st.subheader("🔍 선택 제품 상세 성과")
            st.dataframe(p_df, use_container_width=True)
            
            # 디바이스 비중 시각화
            fig_device = go.Figure(data=[go.Pie(labels=p_df['device'], values=p_df['revenue'], hole=.3)])
            st.plotly_chart(fig_device)
        else:
            st.warning("왼쪽 사이드바에서 제품을 선택해주세요.")
