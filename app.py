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

# 3. 데이터 추출 함수들
@st.cache_data(ttl=3600)
def get_master_item_list():
    if client is None: return pd.DataFrame()
    query = """
    SELECT DISTINCT item_id, item_name, CONCAT('[', item_id, '] ', item_name) as display
    FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
    AND item_id IS NOT NULL
    """
    return client.query(query).to_dataframe()

def get_tab1_data(start_c, end_c, start_p, end_p, time_unit):
    # (이전 KPI 쿼리 로직 동일 - 생략된 부분은 내부적으로 실행됨)
    # [설명: 메인 KPI, 매체별 성과, 시계열 데이터를 가져오는 쿼리]
    pass # 실제 구현 시에는 이전 답변의 get_dashboard_data 쿼리 사용

# 4. 사이드바 구성 (공통 설정)
with st.sidebar:
    st.header("📅 기간 설정")
    curr_d = st.date_input("분석 기간", [datetime.now()-timedelta(days=8), datetime.now()-timedelta(days=1)])
    comp_d = st.date_input("비교 기간", [datetime.now()-timedelta(days=16), datetime.now()-timedelta(days=9)])
    time_unit = st.selectbox("추이 단위", ["일별", "주별", "월별"])
    
    st.markdown("---")
    st.header("🔍 제품 검색 필터 (Tab 2 전용)")
    master_items = get_master_item_list()
    search_kw = st.text_input("제품 키워드 입력", value="T50")
    
    selected_ids = []
    if not master_items.empty:
        filtered = master_items[master_items['display'].str.contains(search_kw, case=False, na=False)]
        selected_displays = st.multiselect("분석할 제품 선택", options=filtered['display'].unique())
        selected_ids = master_items[master_items['display'].isin(selected_displays)]['item_id'].tolist()

# 5. 메인 화면 - 탭 분리
tab1, tab2 = st.tabs(["📊 전체 KPI 현황", "🪑 제품별 상세 분석"])

# --- Tab 1: 전체 KPI 현황 ---
with tab1:
    st.subheader("🎯 전체 비즈니스 성과")
    # 기존 KPI Metric, AI 인사이트, 매체별 성과 성과 표기 로직 배치
    st.info("이곳에는 사이드바에서 선택한 기간의 전체 매출 및 방문 지표가 표시됩니다.")
    # (이전 코드의 섹션 1, 2, 4 로직 삽입)

# --- Tab 2: 제품별 상세 분석 ---
with tab2:
    st.subheader("🔍 선택 제품군 정밀 데이터")
    if not selected_ids:
        st.warning("사이드바에서 제품 키워드를 검색하고 분석할 제품을 선택해 주세요.")
    elif len(curr_d) == 2:
        # 선택된 제품 ID들로만 쿼리 실행
        formatted_ids = ", ".join([f"'{i}'" for i in selected_ids])
        p_query = f"""
        SELECT 
            item.item_id, item.item_name,
            COUNTIF(event_name = 'view_item') as views,
            COUNTIF(event_name = 'purchase') as orders,
            SUM(item.item_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
        WHERE _TABLE_SUFFIX BETWEEN '{curr_d[0].strftime('%Y%m%d')}' AND '{curr_d[1].strftime('%Y%m%d')}'
        AND item.item_id IN ({formatted_ids})
        GROUP BY 1, 2 ORDER BY revenue DESC
        """
        res_df = client.query(p_query).to_dataframe()
        
        if not res_df.empty:
            c1, c2, c3 = st.columns(3)
            c1.metric("선택 제품 합산 매출", f"₩{int(res_df['revenue'].sum()):,}")
            c2.metric("선택 제품 합산 주문", f"{res_df['orders'].sum():,}")
            c3.metric("평균 전환율(상품 기준)", f"{(res_df['orders'].sum()/res_df['views'].sum()*100 if res_df['views'].sum()>0 else 0):.2f}%")
            
            st.markdown("---")
            st.dataframe(res_df.style.format({'revenue': '₩{:,.0f}'}), use_container_width=True)
            
            # [시각화] 제품별 매출 비중 파이차트
            fig_pie = go.Figure(data=[go.Pie(labels=res_df['item_name'], values=res_df['revenue'], hole=.3)])
            fig_pie.update_layout(title_text="선택 제품 간 매출 비중")
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.error("해당 기간에 선택하신 제품의 판매 데이터가 없습니다.")
