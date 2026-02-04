import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Dashboard", page_icon="🪑", layout="wide")

# 2. BigQuery 클라이언트 설정 (기존 코드의 인증 방식 적용)
@st.cache_resource
def get_bq_client():
    try:
        # Streamlit Secrets에 저장된 gcp_service_account 정보를 사용
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# 3. 데이터 추출 함수 (기존 코드의 정밀 쿼리 이식)
def run_kpi_query(start_date, end_date):
    if client is None: return None
    
    # GA4 형식의 날짜 문자열 변환 (YYYYMMDD)
    current_start = start_date.strftime('%Y%m%d')
    current_end = end_date.strftime('%Y%m%d')
    
    # 기존 코드에서 검증된 루커스튜디오 일치 쿼리
    query = f"""
    WITH raw_events AS (
      SELECT 
        user_pseudo_id,
        event_name,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
        ecommerce.purchase_revenue,
        items
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{current_start}' AND '{current_end}'
    )
    SELECT 
        COUNT(DISTINCT CONCAT(user_pseudo_id, '.', ga_session_id)) as sessions,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNTIF(event_name = 'purchase') as purchase,
        SUM(CASE WHEN event_name = 'purchase' THEN purchase_revenue END) as revenue
    FROM raw_events
    """
    
    try:
        query_job = client.query(query)
        result = query_job.to_dataframe()
        return result.fillna(0).iloc[0] if not result.empty else None
    except Exception as e:
        st.error(f"쿼리 실행 오류: {e}")
        return None

# 4. 메인 UI
st.title("🪑 SIDIZ 실시간 KPI 대시보드")
st.markdown("---")

# 사이드바 기간 설정
st.sidebar.header("📅 분석 기간 설정")
today = datetime.now()
curr_range = st.sidebar.date_input("분석 기간", [today - timedelta(days=7), today - timedelta(days=1)])

if len(curr_range) == 2:
    with st.spinner('데이터 분석 중...'):
        data = run_kpi_query(curr_range[0], curr_range[1])
        
        if data is not None:
            # KPI 카드 배치
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("세션 (루커일치)", f"{int(data['sessions']):,}")
            col2.metric("활성 사용자", f"{int(data['users']):,}")
            col3.metric("구매 수", f"{int(data['purchase']):,}")
            col4.metric("총 매출액", f"₩{int(data['revenue']):,}")
            
            st.success("✅ 루커스튜디오와 동일한 로직으로 집계되었습니다.")
else:
    st.info("왼쪽 사이드바에서 시작일과 종료일을 드래그해서 선택해주세요.")
