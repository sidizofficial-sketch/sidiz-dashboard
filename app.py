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
    s_str = start_date.strftime('%Y-%m-%d')
    e_str = end_date.strftime('%Y-%m-%d')
    
    # 1. 여기에 실제 프로젝트 ID와 테이블 경로를 넣으세요
    query = f"""
        SELECT 
            SUM(sessions) as sessions,
            SUM(is_active_user) as users,
            SUM(purchase) as purchase,
            SUM(purchase_revenue) as revenue
        FROM `sidiz-458301.ga4_dashboard.basic_table`
        WHERE date BETWEEN '{s_str}' AND '{e_str}'
    """
    
    # 2. 이 아래 부분을 아래와 같이 수정하세요
    try:
        query_job = client.query(query)  # 실제 쿼리 실행
        result = query_job.to_dataframe()
        if not result.empty:
            return result.iloc[0]
        else:
            return pd.Series({'sessions':0, 'users':0, 'purchase':0, 'revenue':0})
    except Exception as e:
        st.error(f"실제 BQ 연결 실패: {e}")
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
    if st.button("✨ 데이터 요약 및 원인 분석 요청"):
        with st.spinner("Gemini가 데이터를 분석 중입니다..."):
            # AI에게 전달할 문맥 생성
            context = f"""
            분석 결과:
            - 세션: {curr_data['sessions']:,} (전기 대비 {calc_delta(curr_data['sessions'], prev_data['sessions']):.1f}%)
            - 매출: {curr_data['revenue']:,} (전기 대비 {calc_delta(curr_data['revenue'], prev_data['revenue']):.1f}%)
            - 구매건수: {curr_data['purchase']:,} (전기 대비 {calc_delta(curr_data['purchase'], prev_data['purchase']):.1f}%)
            
            위 데이터를 바탕으로 비즈니스 인사이트를 3줄로 요약해줘.
            """
            # response = model.generate_content(context)
            # st.write(response.text)
            st.info("AI 연결 설정(API Key)이 완료되면 여기에 분석 결과가 출력됩니다.")
    
FROM `sidiz-458301.ga4_dashboard.basic_table`
WHERE date = '2024-02-03' -- 루커와 동일한 하루 날짜

else:
    st.warning("사이드바에서 시작일과 종료일을 모두 선택해주세요.")
