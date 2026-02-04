import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정 및 API 키 설정
st.set_page_config(page_title="SIDIZ Advanced Analytics", layout="wide")

# Secrets에서 Gemini API 키 가져오기 (설정이 필요합니다)
if "gemini_api_key" in st.secrets:
    genai.configure(api_key=st.secrets["gemini_api_key"])
else:
    st.warning("⚠️ Gemini API 키가 Secrets에 설정되지 않았습니다. 분석 코멘트 기능이 제한됩니다.")

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

# 3. 데이터 추출 함수 (매체별/제품별 쿼리 보강)
def get_advanced_data(start_c, end_c, start_p, end_p, product_keyword):
    if client is None: return None, None, None
    
    # [A] 매체별 성과 쿼리
    source_query = f"""
    SELECT 
        traffic_source.source,
        traffic_source.medium,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SAFE_DIVIDE(COUNTIF(event_name = 'purchase'), COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)))) * 100 as cvr,
        SUM(ecommerce.purchase_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    GROUP BY 1, 2
    ORDER BY revenue DESC
    LIMIT 10
    """

    # [B] 제품 키워드 분석 쿼리
    product_query = f"""
    SELECT 
        '{product_keyword}' as keyword,
        COUNTIF(event_name = 'page_view' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%{product_keyword}%') as pv,
        COUNTIF(event_name = 'purchase' AND EXISTS(SELECT 1 FROM UNNEST(items) WHERE item_name LIKE '%{product_keyword}%')) as orders,
        SUM((SELECT item_revenue FROM UNNEST(items) WHERE item_name LIKE '%{product_keyword}%')) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    """

    try:
        source_df = client.query(source_query).to_dataframe()
        prod_df = client.query(product_query).to_dataframe()
        return source_df, prod_df
    except Exception as e:
        st.error(f"⚠️ 심화 분석 데이터 로드 실패: {e}")
        return None, None

# 4. Gemini 인사이트 생성 함수
def get_gemini_insight(curr_data, prev_data):
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"""
        당신은 시디즈의 데이터 분석 전문가입니다. 아래의 데이터를 바탕으로 비즈니스 요약과 향후 전략을 한 문단으로 짧고 예리하게 분석해주세요.
        - 이번 기간: 매출 {curr_data['revenue']:,}원, 세션 {curr_data['sessions']:,}회, 전환율 {curr_data['cvr']:.2f}%
        - 이전 기간: 매출 {prev_data['revenue']:,}원, 세션 {prev_data['sessions']:,}회, 전환율 {prev_data['cvr']:.2f}%
        (특히 매출 변동의 원인이 세션 유입량 변화인지, 전환율 변화인지 짚어주세요.)
        """
        response = model.generate_content(prompt)
        return response.text
    except:
        return "Gemini 분석을 불러오는 데 실패했습니다."

# 5. UI 구성
st.title("🪑 SIDIZ AI Intelligence Dashboard (Advanced)")

with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간", [datetime.now() - timedelta(days=8), datetime.now() - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [datetime.now() - timedelta(days=16), datetime.now() - timedelta(days=9)])
    st.markdown("---")
    st.header("🔍 필터")
    product_keyword = st.text_input("제품 키워드 필터 (예: T50)", value="T50")

# 6. 메인 로직
if len(curr_date) == 2 and len(comp_date) == 2:
    # 기존 KPI 데이터 및 심화 데이터 로드 (함수 호출 생략, 이전 코드의 get_dashboard_data 활용)
    source_df, prod_df = get_advanced_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], product_keyword)

    # --- [섹션 1: Gemini 인사이트] ---
    st.subheader("🤖 Gemini AI 비즈니스 인사이트")
    with st.expander("데이터 기반 자동 분석 코멘트 보기", expanded=True):
        # 예시용 더미 딕셔너리 (실제로는 앞선 KPI summary_df에서 추출)
        curr_info = {'revenue': 50000000, 'sessions': 10000, 'cvr': 1.5}
        prev_info = {'revenue': 45000000, 'sessions': 12000, 'cvr': 1.2}
        insight = get_gemini_insight(curr_info, prev_info)
        st.info(insight)

    # --- [섹션 2: 매체별 성과 분석] ---
    st.markdown("---")
    st.subheader("🌐 매체별 성과 (Source / Medium)")
    if source_df is not None:
        st.table(source_df.style.format({'cvr': '{:.2f}%', 'revenue': '₩{:,.0f}'}))
        

    # --- [섹션 3: 특정 제품군 필터링 성과] ---
    st.markdown("---")
    st.subheader(f"🪑 '{product_keyword}' 제품군 성과 분석")
    if prod_df is not None:
        p1, p2, p3 = st.columns(3)
        p1.metric(f"{product_keyword} PV", f"{int(prod_df['pv']):,}")
        p2.metric(f"{product_keyword} 주문수", f"{int(prod_df['orders']):,}")
        p3.metric(f"{product_keyword} 추정 매출", f"₩{int(prod_df['revenue']):,}")

else:
    st.info("기간을 선택해주세요.")
