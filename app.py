import streamlit as st
from google.cloud import bigquery
import pandas as pd
import google.generativeai as genai
from datetime import datetime, timedelta

# 1. 초기 설정
st.set_page_config(page_title="시디즈 데이터 인사이트", layout="wide")

# [보안 주의] 실제 배포 시에는 st.secrets를 사용하세요.
# genai.configure(api_key=st.secrets["gemini_api_key"])
# client = bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

# 2. 데이터 추출 함수 (BigQuery)
def run_kpi_query(start_date, end_date):
    """특정 기간의 KPI 합계를 가져오는 함수"""
    # 날짜 객체를 문자열로 변환
    s_str = start_date.strftime('%Y-%m-%d')
    e_str = end_date.strftime('%Y-%m-%d')
    
    # basic_table 기준 쿼리
    query = f"""
        SELECT 
            SUM(sessions) as sessions,
            SUM(is_active_user) as users,
            SUM(purchase) as purchase,
            SUM(purchase_revenue) as revenue,
            SUM(pageviews) as pv
        FROM `your-project.ga4_dashboard.basic_table`
        WHERE date BETWEEN '{s_str}' AND '{e_str}'
    """
    try:
        # 실제 연결 시 아래 주석 해제
        # return client.query(query).to_dataframe().iloc[0]
        
        # [테스트용 더미 데이터] 실제 연결 전까지 화면 확인용
        return pd.Series({
            'sessions': 150000, 'users': 95000, 
            'purchase': 1200, 'revenue': 180000000, 'pv': 450000
        })
    except Exception as e:
        st.error(f"BQ 에러: {e}")
        return None

# 3. 사이드바 - 기간 컨트롤
st.sidebar.header("📅 분석 기간 설정")

# 기준 기간
st.sidebar.subheader("1. 분석 기준 기간")
curr_range = st.sidebar.date_input("기준 날짜", 
    [datetime.now() - timedelta(days=7), datetime.now()], key="curr")

# 비교 기간
st.sidebar.subheader("2. 대조 비교 기간")
prev_range = st.sidebar.date_input("비교 날짜", 
    [datetime.now() - timedelta(days=15), datetime.now() - timedelta(days=8)], key="prev")

# 4. 메인 UI 구성
st.title("🪑 시디즈 의사결정 지원 시스템")
st.markdown("---")

if len(curr_range) == 2 and len(prev_range) == 2:
    # 데이터 로드
    curr_data = run_kpi_query(curr_range[0], curr_range[1])
    prev_data = run_kpi_query(prev_range[0], prev_range[1])

    # 지표 계산
    def calc_delta(curr, prev):
        if prev == 0 or prev is None: return 0
        return ((curr - prev) / prev) * 100

    st.subheader("1️⃣ 핵심 KPI 요약 및 비교")
    
    # KPI 카드 배치 (4열)
    col1, col2, col3, col4 = st.columns(4)
    
    kpis = [
        ("세션", 'sessions', "{:,.0f}"),
        ("활성 사용자", 'users', "{:,.0f}"),
        ("구매 수", 'purchase', "{:,.0f}"),
        ("총 매출액", 'revenue', "₩{:,.0f}")
    ]

    for i, (label, key, fmt) in enumerate(kpis):
        c_val = curr_data[key]
        p_val = prev_data[key]
        delta = calc_delta(c_val, p_val)
        
        with [col1, col2, col3, col4][i]:
            st.metric(label=label, value=fmt.format(c_val), delta=f"{delta:.1f}%")

    st.markdown("---")

    # 5. AI 인사이트 영역 (Gemini)
    st.subheader("🤖 AI 데이터 해석")
    if st.button("✨ 데이터 요약
