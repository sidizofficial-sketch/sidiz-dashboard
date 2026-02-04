import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta

# 1. 초기 설정 및 BigQuery 클라이언트 생성 (수정된 핵심 부분)
st.set_page_config(page_title="시디즈 데이터 인사이트", layout="wide")

# 프로젝트 ID를 명시적으로 입력하여 'Project not passed' 에러 해결
PROJECT_ID = "sidiz-458301" 

@st.cache_resource
def get_bq_client():
    try:
        # 프로젝트 ID를 직접 전달합니다.
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ BigQuery 클라이언트 생성 실패: {e}")
        return None

client = get_bq_client()

# 2. 데이터 추출 함수 (중복 로직 제거 및 안정화)
def run_kpi_query(start_date, end_date):
    if client is None:
        return pd.Series({'sessions':0, 'users':0, 'purchase':0, 'revenue':0})
        
    s_str = start_date.strftime('%Y-%m-%d')
    e_str = end_date.strftime('%Y-%m-%d')
    
    query = f"""
        SELECT 
            SUM(sessions) as sessions,
            SUM(is_active_user) as users,
            SUM(purchase) as purchase,
            SUM(purchase_revenue) as revenue
        FROM `{PROJECT_ID}.ga4_dashboard.basic_table`
        WHERE date BETWEEN '{s_str}' AND '{e_str}'
    """
    
    try:
        # query_job 실행
        query_job = client.query(query)
        result = query_job.to_dataframe()
        if not result.empty:
            return result.fillna(0).iloc[0]
        return pd.Series({'sessions':0, 'users':0, 'purchase':0, 'revenue':0})
    except Exception as e:
        st.error(f"⚠️ 데이터 로드 실패: {e}")
        return pd.Series({'sessions':0, 'users':0, 'purchase':0, 'revenue':0})

# 3. 사이드바 - 기간 컨트롤
st.sidebar.header("📅 분석 기간 설정")
curr_range = st.sidebar.date_input("기준 날짜", 
    [datetime.now() - timedelta(days=7), datetime.now()], key="curr")
prev_range = st.sidebar.date_input("비교 날짜", 
    [datetime.now() - timedelta(days=15), datetime.now() - timedelta(days=8)], key="prev")

# 4. 메인 UI 구성
st.title("🪑 시디즈 의사결정 지원 시스템")
st.markdown("---")

# 날짜가 모두 선택되었을 때만 실행
if len(curr_range) == 2 and len(prev_range) == 2:
    with st.spinner('BigQuery 데이터를 분석 중입니다...'):
        curr_data = run_kpi_query(curr_range[0], curr_range[1])
        prev_data = run_kpi_query(prev_range[0], prev_range[1])

    # 지표 계산 함수
    def calc_delta(curr, prev):
        if prev == 0 or prev is None or pd.isna(prev): return 0
        return ((curr - prev) / prev) * 100

    st.subheader("1️⃣ 핵심 KPI 요약 및 비교")
    
    col1, col2, col3, col4 = st.columns(4)
    
    kpi_configs = [
        ("세션", 'sessions', "{:,.0f}"),
        ("활성 사용자", 'users', "{:,.0f}"),
        ("구매 수", 'purchase', "{:,.0f}"),
        ("총 매출액", 'revenue', "₩{:,.0f}")
    ]

    for i, (label, key, fmt) in enumerate(kpi_configs):
        c_val = curr_data[key]
        p_val = prev_data[key]
        delta = calc_delta(c_val, p_val)
        with [col1, col2, col3, col4][i]:
            st.metric(label=label, value=fmt.format(c_val), delta=f"{delta:.1f}%")
else:
    st.info("💡 사이드바에서 분석할 시작일과 종료일을 모두 드래그하여 선택해주세요.")
