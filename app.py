import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Analytics", layout="wide")

# Gemini 설정
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

# 3. [보정] 상품명 정제 로직 (옵션 제거)
# 'T50 HLDA - 블랙' -> 'T50 HLDA'로 변환
def clean_product_name(name):
    if not name: return name
    # 대시(-), 슬래시(/), 괄호(() 앞까지만 취함
    for char in [' - ', ' / ', ' (']:
        if char in name:
            name = name.split(char)[0]
    return name.strip()

@st.cache_data(ttl=3600)
def get_master_item_list():
    if client is None: return pd.DataFrame(columns=['clean_name'])
    query = """
    SELECT DISTINCT item_name 
    FROM `sidiz-458301.analytics_487246344.events_*` , UNNEST(items) as item
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
    AND item_name IS NOT NULL AND item_name NOT IN ('(not set)', '')
    """
    try:
        df = client.query(query).to_dataframe()
        df['clean_name'] = df['item_name'].apply(clean_product_name)
        return df[['clean_name']].drop_duplicates().sort_values('clean_name')
    except:
        return pd.DataFrame(columns=['clean_name'])

# 4. 데이터 추출 함수 (빈 화면 해결을 위해 쿼리 분리)
def get_kpi_data(start_c, end_c, start_p, end_p):
    # 날짜를 BigQuery 테이블 접미사 형식(YYYYMMDD)으로 변환
    s_c = start_c.strftime('%Y%m%d')
    e_c = end_c.strftime('%Y%m%d')
    
    # 기본 Current 쿼리
    query = f"""
    SELECT 
        '{start_c}' as period_start,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(ecommerce.purchase_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    """
    
    # Previous 쿼리 (선택 시에만 결합)
    if start_p and end_p:
        s_p = start_p.strftime('%Y%m%d')
        e_p = end_p.strftime('%Y%m%d')
        query = f"""
        ({query})
        UNION ALL
        (SELECT 
            '{start_p}' as period_start,
            COUNT(DISTINCT user_pseudo_id) as users,
            COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions,
            COUNTIF(event_name = 'purchase') as orders,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}')
        """
    
    return client.query(query).to_dataframe()

# 5. 사이드바 구성
with st.sidebar:
    st.header("📅 기간 설정")
    yesterday = datetime.now() - timedelta(days=1)
    seven_days_ago = yesterday - timedelta(days=6)
    curr_d = st.date_input("분석 기간 (Current)", [seven_days_ago, yesterday])
    
    use_compare = st.checkbox("비교 기간 사용 (Previous)")
    comp_d = [None, None]
    if use_compare:
        comp_d = st.date_input("비교 기간 선택", [seven_days_ago - timedelta(days=7), yesterday - timedelta(days=7)])

    st.markdown("---")
    st.header("🔍 제품 필터 (Tab 2)")
    master_items = get_master_item_list()
    search_kw = st.text_input("제품명 키워드 검색", value="T50")
    
    selected_names = []
    if not master_items.empty:
        filtered = master_items[master_items['clean_name'].str.contains(search_kw, case=False, na=False)]
        selected_names = st.multiselect("분석할 상품명 선택", options=filtered['clean_name'].unique())

# 6. 메인 화면 - 탭 구성
tab1, tab2 = st.tabs(["📊 전체 KPI 현황", "🪑 제품별 상세 분석"])

with tab1:
    if len(curr_d) == 2:
        kpi_res = get_kpi_data(curr_d[0], curr_d[1], comp_d[0] if use_compare else None, comp_d[1] if use_compare else None)
        
        if not kpi_res.empty:
            # 첫 번째 행이 Current
            curr = kpi_res.iloc[0]
            prev = kpi_res.iloc[1] if len(kpi_res) > 1 else curr
            
            st.subheader("🎯 핵심 성과 요약")
            c1, c2, c3, c4 = st.columns(4)
            
            def delta(c, p):
                if not use_compare or p == 0: return None
                return f"{((c-p)/p*100):+.1f}%"

            c1.metric("매출액", f"₩{int(curr['revenue'] or 0):,}", delta(curr['revenue'], prev['revenue']))
            c2.metric("주문수", f"{int(curr['orders']):,}", delta(curr['orders'], prev['orders']))
            c3.metric("세션", f"{int(curr['sessions']):,}", delta(curr['sessions'], prev['sessions']))
            cvR = (curr['orders']/curr['sessions']*100) if curr['sessions'] > 0 else 0
            c4.metric("구매전환율", f"{cvR:.2f}%")
        else:
            st.warning("선택한 기간에 데이터가 없습니다. BigQuery 연결을 확인해 주세요.")

with tab2:
    if not selected_names:
        st.info("사이드바에서 상품명을 선택해 주세요.")
    else:
        # 상품명 리스트를 쿼리에 넣기 위해 정제된 이름으로 다시 매칭
        formatted_names = ", ".join([f"'{n}%'" for n in selected_names])
        p_query = f"""
            SELECT 
                item_name, 
                COUNTIF(event_name='view_item') as views, 
                COUNTIF(event_name='purchase') as orders, 
                SUM(item_revenue) as revenue
            FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
            WHERE _TABLE_SUFFIX BETWEEN '{curr_d[0].strftime('%Y%m%d')}' AND '{curr_d[1].strftime('%Y%m%d')}'
            AND ({' OR '.join([f"item_name LIKE '{n}%'" for n in selected_names])})
            GROUP BY 1 ORDER BY revenue DESC
        """
        res_df = client.query(p_query).to_dataframe()
        
        # 결과 데이터프레임에서도 옵션을 제거하여 합산
        if not res_df.empty:
            res_df['item_name'] = res_df['item_name'].apply(clean_product_name)
            final_df = res_df.groupby('item_name').sum().reset_index()
            
            st.subheader(f"🔍 선택 상품 통합 성과")
            st.dataframe(final_df.style.format({'revenue': '₩{:,.0f}'}), use_container_width=True)
