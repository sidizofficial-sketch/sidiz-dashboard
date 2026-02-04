import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Dashboard", page_icon="🪑", layout="wide")

# 2. BigQuery 클라이언트 설정
@st.cache_resource
def get_bq_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# 3. 데이터 추출 함수 (지표 추가 및 세션 로직 보정)
def run_kpi_query(start_date, end_date):
    if client is None: return None
    
    current_start = start_date.strftime('%Y%m%d')
    current_end = end_date.strftime('%Y%m%d')
    
    # 세션 정합성을 위해 ga_session_id가 없는 데이터는 제외하고 집계합니다.
    query = f"""
    WITH raw_events AS (
      SELECT 
        user_pseudo_id,
        event_name,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as ga_session_number,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{current_start}' AND '{current_end}'
    )
    SELECT 
        -- 세션 (루커스튜디오 방식: 유저와 세션ID 조합의 고유값)
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(ga_session_id AS STRING))) as sessions,
        -- 활성 사용자
        COUNT(DISTINCT user_pseudo_id) as users,
        -- 신규 사용자 (세션 번호가 1인 경우)
        COUNT(DISTINCT CASE WHEN ga_session_number = 1 THEN user_pseudo_id END) as new_users,
        -- 페이지뷰
        COUNTIF(event_name = 'page_view') as pageviews,
        -- 구매 수
        COUNTIF(event_name = 'purchase') as purchase,
        -- 매출액
        SUM(CASE WHEN event_name = 'purchase' THEN purchase_revenue END) as revenue
    FROM raw_events
    WHERE ga_session_id IS NOT NULL
    """
    
    try:
        query_job = client.query(query)
        result = query_job.to_dataframe()
        if not result.empty:
            df = result.fillna(0).iloc[0]
            # 신규 방문율 계산 (%)
            df['new_user_rate'] = (df['new_users'] / df['users'] * 100) if df['users'] > 0 else 0
            return df
        return None
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
            # 첫 번째 줄: 트래픽 지표
            st.subheader("📈 트래픽 및 방문")
            t_col1, t_col2, t_col3, t_col4 = st.columns(4)
            t_col1.metric("세션 (보정됨)", f"{int(data['sessions']):,}")
            t_col2.metric("활성 사용자", f"{int(data['users']):,}")
            t_col3.metric("신규 사용자", f"{int(data['new_users']):,}")
            t_col4.metric("신규 방문율", f"{data['new_user_rate']:.1f}%")
            
            # 두 번째 줄: 성과 지표
            st.markdown("---")
            st.subheader("💰 구매 및 콘텐츠 성과")
            p_col1, p_col2, p_col3 = st.columns(3)
            p_col1.metric("페이지뷰 (PV)", f"{int(data['pageviews']):,}")
            p_col2.metric("구매 수", f"{int(data['purchase']):,}")
            p_col3.metric("총 매출액", f"₩{int(data['revenue']):,}")
            
            st.success("✅ 루커스튜디오 지표 구성이 업데이트되었습니다.")
else:
    st.info("왼쪽 사이드바에서 날짜 범위를 선택해주세요.")
