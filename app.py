import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Analytics", layout="wide")

# Gemini 설정 (Secrets 키 명칭 확인 필요)
if "gemini" in st.secrets and "api_key" in st.secrets["gemini"]:
    genai.configure(api_key=st.secrets["gemini"]["api_key"])
    model = genai.GenerativeModel('gemini-1.5-flash')
    HAS_GEMINI = True
else:
    HAS_GEMINI = False

# 2. BigQuery 클라이언트 설정
@st.cache_resource
def get_bq_client():
    try:
        # st.secrets 구조에 따라 접근 방식 수정
        if "gcp_service_account" in st.secrets:
            info = dict(st.secrets["gcp_service_account"])
            # JSON 내부 필드가 문자열로 박혀있는 경우 처리
            if "json_key" in info:
                info = json.loads(info["json_key"])
            return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# 3. 상품명 정제 로직
def clean_product_name(name):
    if not name: return name
    for char in [' - ', ' / ', ' (']:
        if char in name:
            name = name.split(char)[0]
    return name.strip()

@st.cache_data(ttl=3600)
def get_master_item_list():
    if client is None: return pd.DataFrame(columns=['clean_name'])
    # _TABLE_SUFFIX를 STRING으로 비교하여 속도 향상
    query = """
    SELECT DISTINCT item.item_name 
    FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
    AND item.item_name IS NOT NULL AND item.item_name NOT IN ('(not set)', '')
    """
    try:
        df = client.query(query).to_dataframe()
        df['clean_name'] = df['item_name'].apply(clean_product_name)
        return df[['clean_name']].drop_duplicates().sort_values('clean_name')
    except Exception as e:
        st.sidebar.error(f"상품 목록 로드 실패: {e}")
        return pd.DataFrame(columns=['clean_name'])

# 4. 데이터 추출 함수 (에러 상세 출력 추가)
def get_kpi_data(start_c, end_c, start_p, end_p):
    s_c = start_c.strftime('%Y%m%d')
    e_c = end_c.strftime('%Y%m%d')
    
    # 기본 쿼리 (Canonical 작업 전이므로 원본 활용)
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
    
    if start_p and end_p:
        s_p = start_p.strftime('%Y%m%d')
        e_p = end_p.strftime('%Y%m%d')
        query = f"({query}) UNION ALL (SELECT '{start_p}', COUNT(DISTINCT user_pseudo_id), COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))), COUNTIF(event_name = 'purchase'), SUM(ecommerce.purchase_revenue) FROM `sidiz-458301.analytics_487246344.events_*` WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}')"
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        # 한자 오류/Redacted 방지를 위해 상세 에러 강제 출력
        st.error("🚨 BigQuery 쿼리 실행 에러 발생")
        st.code(str(e)) # 여기서 실제 원인이 나옵니다.
        return pd.DataFrame()

# 5. 사이드바
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
    st.header("🔍 제품 필터")
    master_items = get_master_item_list()
    search_kw = st.text_input("제품명 키워드 검색", value="T50")
    
    selected_names = []
    if not master_items.empty:
        filtered = master_items[master_items['clean_name'].str.contains(search_kw, case=False, na=False)]
        selected_names = st.multiselect("분석할 상품명 선택", options=filtered['clean_name'].unique())

# 6. 메인 화면
tab1, tab2 = st.tabs(["📊 전체 KPI 현황", "🪑 제품별 상세 분석"])

with tab1:
    if len(curr_d) == 2:
        kpi_res = get_kpi_data(curr_d[0], curr_d[1], comp_d[0] if use_compare else None, comp_d[1] if use_compare else None)
        
        if not kpi_res.empty:
            curr = kpi_res.iloc[0]
            prev = kpi_res.iloc[1] if len(kpi_res) > 1 else curr
            
            st.subheader("🎯 핵심 성과 요약")
            c1, c2, c3, c4 = st.columns(4)
            
            def delta(c, p):
                if not use_compare or p == 0: return None
                return f"{((float(c)-float(p))/float(p)*100):+.1f}%"

            c1.metric("매출액", f"₩{int(curr['revenue'] or 0):,}", delta(curr['revenue'], prev['revenue']))
            c2.metric("주문수", f"{int(curr['orders']):,}", delta(curr['orders'], prev['orders']))
            c3.metric("세션", f"{int(curr['sessions']):,}", delta(curr['sessions'], prev['sessions']))
            cvR = (curr['orders']/curr['sessions']*100) if curr['sessions'] > 0 else 0
            c4.metric("구매전환율", f"{cvR:.2f}%")
            
            # AI 분석 추가 (원하실 경우)
            if HAS_GEMINI and st.button("AI 인사이트 도출"):
                with st.spinner("데이터 분석 중..."):
                    res = model.generate_content(f"다음 시디즈 지표를 분석해줘: 매출 ₩{curr['revenue']}, 주문 {curr['orders']}, 세션 {curr['sessions']}")
                    st.info(res.text)
        else:
            st.error("데이터를 불러오지 못했습니다. 위 에러 메시지를 확인하세요.")

with tab2:
    if not selected_names:
        st.info("사이드바에서 상품명을 선택해 주세요.")
    else:
        # 상품명 쿼리 보정
        name_filters = " OR ".join([f"item.item_name LIKE '{n}%'" for n in selected_names])
        p_query = f"""
            SELECT 
                item.item_name, 
                COUNTIF(event_name='view_item') as views, 
                COUNTIF(event_name='purchase') as orders, 
                SUM(item.item_revenue) as revenue
            FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
            WHERE _TABLE_SUFFIX BETWEEN '{curr_d[0].strftime('%Y%m%d')}' AND '{curr_d[1].strftime('%Y%m%d')}'
            AND ({name_filters})
            GROUP BY 1 ORDER BY revenue DESC
        """
        try:
            res_df = client.query(p_query).to_dataframe()
            if not res_df.empty:
                res_df['item_name'] = res_df['item_name'].apply(clean_product_name)
                final_df = res_df.groupby('item_name').sum().reset_index()
                st.subheader(f"🔍 선택 상품 통합 성과")
                st.dataframe(final_df.style.format({'revenue': '₩{:,.0f}'}), use_container_width=True)
        except Exception as e:
            st.error("상품 상세 분석 중 오류 발생")
            st.code(str(e))
