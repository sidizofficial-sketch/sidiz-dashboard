import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai
import re

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

# 3. [보정] 데이터 클렌징 함수 (한자 및 옵션 제거)
def clean_product_name(name):
    if not name or name == '(not set)': return ""
    
    # [A] 한자 포함 여부 체크 - 한자가 포함된 이름은 무시하거나 한글로 치환
    if re.search(r'[\u4e00-\u9fff]', name):
        # 'T50 全選項' 같은 이름을 'T50 풀옵션' 등으로 치환하고 싶다면 여기에 추가
        name = name.replace('全選項', '풀옵션').replace('空中', '에어')
        # 만약 한자가 섞인 데이터를 아예 안 보고 싶다면 return "" 처리
    
    # [B] 특수문자 및 옵션 텍스트 제거
    # 대시(-), 슬래시(/), 괄호(() 앞까지만 취함
    for char in [' - ', ' / ', ' (', '[']:
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
    df = client.query(query).to_dataframe()
    df['clean_name'] = df['item_name'].apply(clean_product_name)
    # 빈 값 제거 후 정렬
    return df[df['clean_name'] != ""].drop_duplicates().sort_values('clean_name')

# 4. 데이터 추출 함수 (KPI + 시계열)
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    
    if time_unit == "일별": group_sql = "date"
    elif time_unit == "주별": group_sql = "DATE_TRUNC(date, WEEK)"
    else: group_sql = "DATE_TRUNC(date, MONTH)"

    def build_kpi_sql(s, e, label):
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
    
    kpi_query = build_kpi_sql(s_c, e_c, 'Current')
    if start_p:
        kpi_query += f" UNION ALL {build_kpi_sql(start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d'), 'Previous')}"
    
    ts_query = f"""
    SELECT 
        CAST({group_sql} AS STRING) as period_label,
        SUM(ecommerce.purchase_revenue) as revenue,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY 1
    """
    
    return client.query(kpi_query).to_dataframe(), client.query(ts_query).to_dataframe()

# 5. 사이드바 구성
with st.sidebar:
    st.header("📅 기간 설정")
    yesterday = datetime.now() - timedelta(days=1)
    curr_d = st.date_input("분석 기간", [yesterday - timedelta(days=6), yesterday])
    
    use_compare = st.checkbox("비교 기간 사용")
    comp_d = [None, None]
    if use_compare:
        comp_d = st.date_input("비교 기간", [yesterday - timedelta(days=13), yesterday - timedelta(days=7)])
    
    time_unit = st.selectbox("추이 단위", ["일별", "주별", "월별"])
    
    st.markdown("---")
    st.header("🔍 제품 필터 (Tab 2)")
    master_items = get_master_item_list()
    search_kw = st.text_input("제품명 검색", value="T50")
    
    # 필터링된 리스트에서 한자가 포함되지 않은 깨끗한 이름만 제공
    filtered_options = master_items[master_items['clean_name'].str.contains(search_kw, case=False)]['clean_name'].unique()
    selected_names = st.multiselect("분석할 상품 선택", options=filtered_options)

# 6. 메인 화면
tab1, tab2 = st.tabs(["📊 KPI 현황", "🪑 제품 상세"])

with tab1:
    if len(curr_d) == 2:
        kpi_df, ts_df = get_dashboard_data(curr_d[0], curr_d[1], comp_d[0] if use_compare else None, comp_d[1] if use_compare else None, time_unit)
        
        if not kpi_df.empty:
            curr = kpi_df[kpi_df['type']=='Current'].iloc[0]
            prev = kpi_df[kpi_df['type']=='Previous'].iloc[0] if len(kpi_df) > 1 else curr
            
            # AI 인사이트
            if HAS_GEMINI:
                try:
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    insight = model.generate_content(f"시디즈 매출 {curr['revenue']:,}원 성과 요약해줘").text
                    st.info(f"🤖 AI 분석: {insight}")
                except: st.warning("AI 분석 로드 실패")

            st.subheader("🎯 핵심 성과 요약")
            def delta(c, p): return f"{((c-p)/p*100):+.1f}%" if use_compare and p > 0 else None
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("매출액", f"₩{int(curr['revenue'] or 0):,}", delta(curr['revenue'], prev['revenue']))
            c2.metric("주문수", f"{int(curr['orders']):,}", delta(curr['orders'], prev['orders']))
            c3.metric("세션", f"{int(curr['sessions']):,}", delta(curr['sessions'], prev['sessions']))
            c4.metric("신규 방문율", f"{(curr['new_users']/curr['users']*100 if curr['users'] > 0 else 0):.1f}%")

            st.markdown("---")
            c5, c6, c7, c8 = st.columns(4)
            c5.metric("전환율(CVR)", f"{(curr['orders']/curr['sessions']*100 if curr['sessions'] > 0 else 0):.2f}%")
            c6.metric("객단가(AOV)", f"₩{int(curr['revenue']/curr['orders'] if curr['orders']>0 else 0):,}")
            c7.metric("활성 사용자", f"{int(curr['users']):,}")
            c8.metric("인당 세션수", f"{(curr['sessions']/curr['users'] if curr['users']>0 else 0):.1f}")

            # 그래프
            fig = go.Figure()
            fig.add_trace(go.Bar(x=ts_df['period_label'], y=ts_df['revenue'], name='매출', marker_color='#2ca02c'))
            fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['sessions'], name='세션', yaxis='y2', line=dict(color='#1f77b4')))
            fig.update_layout(yaxis2=dict(overlaying='y', side='right'), template="plotly_white", hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)

with tab2:
    if selected_names:
        # LIKE 조건으로 선택된 모든 유사 상품(옵션 포함) 데이터 호출
        query_conditions = " OR ".join([f"item_name LIKE '{n}%'" for n in selected_names])
        p_query = f"""
            SELECT item_name, COUNTIF(event_name='view_item') as views, COUNTIF(event_name='purchase') as orders, SUM(item_revenue) as revenue
            FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
            WHERE _TABLE_SUFFIX BETWEEN '{curr_d[0].strftime('%Y%m%d')}' AND '{curr_d[1].strftime('%Y%m%d')}'
            AND ({query_conditions})
            GROUP BY 1 ORDER BY revenue DESC
        """
        res_df = client.query(p_query).to_dataframe()
        
        # 실제 데이터 집계 시에도 이름을 정제하여 합산
        res_df['item_name'] = res_df['item_name'].apply(clean_product_name)
        final_df = res_df.groupby('item_name').sum().reset_index()
        
        st.subheader("🔍 상품명 통합 분석 결과")
        st.dataframe(final_df.style.format({'revenue': '₩{:,.0f}'}), use_container_width=True)
    else:
        st.info("사이드바에서 상품명을 검색하고 선택해 주세요.")
