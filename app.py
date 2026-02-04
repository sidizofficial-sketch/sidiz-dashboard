import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Intelligence Dashboard", layout="wide")

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

# 3. 데이터 추출 함수
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None, None
    
    # 시간 단위별 레이블 설정 (SQL 내에서 가독성을 위해 미리 문자열 생성)
    if time_unit == "일별":
        group_sql = "CAST(date AS STRING)"
    elif time_unit == "주별":
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, WEEK) AS STRING), ' ~ ', CAST(LAST_DAY(date, WEEK) AS STRING))"
    else: # 월별
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, MONTH) AS STRING), ' ~ ', CAST(LAST_DAY(date, MONTH) AS STRING))"

    # 1. 요약 데이터용 쿼리 (Current vs Previous)
    # f-string 내부에 중괄호가 겹치지 않도록 주의하여 작성
    summary_query = f"""
    WITH raw_data AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as session_num,
        event_name,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{min(start_c, start_p).strftime('%Y%m%d')}' AND '{max(end_c, end_p).strftime('%Y%m%d')}'
    )
    SELECT 
        CASE 
            WHEN date BETWEEN '{start_c.strftime('%Y-%m-%d')}' AND '{end_c.strftime('%Y-%m-%d')}' THEN 'Current' 
            WHEN date BETWEEN '{start_p.strftime('%Y-%m-%d')}' AND '{end_p.strftime('%Y-%m-%d')}' THEN 'Previous' 
        END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN session_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(purchase_revenue) as revenue
    FROM raw_data
    WHERE session_id IS NOT NULL
    GROUP BY 1
    HAVING type IS NOT NULL
    """

    # 2. 시계열 데이터용 쿼리 (Current 기간만)
    ts_query = f"""
    WITH ts_raw AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        event_name,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    )
    SELECT 
        {group_sql} as period_label,
        SUM(purchase_revenue) as revenue,
        COUNTIF(event_name = 'purchase') as orders,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions
    FROM ts_raw
    WHERE session_id IS NOT NULL
    GROUP BY 1
    ORDER BY 1
    """
    
    try:
        summary_df = client.query(summary_query).to_dataframe()
        ts_df = client.query(ts_query).to_dataframe()
        return summary_df, ts_df
    except Exception as e:
        st.error(f"⚠️ 데이터 쿼리 중 오류가 발생했습니다: {e}")
        return None, None

# 4. 메인 UI 구성
st.title("🪑 SIDIZ AI Intelligence Dashboard")

with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간 (Current)", [datetime.now() - timedelta(days=8), datetime.now() - timedelta(days=1)])
    comp_date = st.date_input("비교 기간 (Previous)", [datetime.now() - timedelta(days=16), datetime.now() - timedelta(days=9)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_date) == 2 and len(comp_date) == 2:
    summary_df, ts_df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], time_unit)
    
    if summary_df is not None and not summary_df.empty:
        # 지표 추출 및 화면 렌더링 (이전 로직 동일)
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0] if 'Current' in summary_df['type'].values else pd.Series(0, index=summary_df.columns)
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else pd.Series(0, index=summary_df.columns)

        def calc_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        st.subheader("🎯 핵심 성과 요약")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("활성 사용자", f"{int(curr['users']):,}", calc_delta(curr['users'], prev['users']))
        c2.metric("신규 사용자", f"{int(curr['new_users']):,}", calc_delta(curr['new_users'], prev['new_users']))
        
        curr_nv = (curr['new_users']/curr['users']*100) if curr['users']>0 else 0
        prev_nv = (prev['new_users']/prev['users']*100) if prev['users']>0 else 0
        c3.metric("신규 방문율", f"{curr_nv:.1f}%", f"{(curr_nv-prev_nv):+.1f}%p")
        c4.metric("총 매출액", f"₩{int(curr['revenue']):,}", calc_delta(curr['revenue'], prev['revenue']))

        st.markdown("---")
        c5, c6, c7, c8 = st.columns(4)
        c5.metric("세션 수", f"{int(curr['sessions']):,}", calc_delta(curr['sessions'], prev['sessions']))
        c6.metric("주문수", f"{int(curr['orders']):,}", calc_delta(curr['orders'], prev['orders']))
        
        curr_cr = (curr['orders']/curr['sessions']*100) if curr['sessions']>0 else 0
        prev_cr = (prev['orders']/prev['sessions']*100) if prev['sessions']>0 else 0
        c7.metric("구매전환율(CVR)", f"{curr_cr:.2f}%", f"{(curr_cr-prev_cr):+.2f}%p")
        
        curr_aov = (curr['revenue']/curr['orders']) if curr['orders']>0 else 0
        prev_aov = (prev['revenue']/prev['orders']) if prev['orders']>0 else 0
        c8.metric("평균 객단가(AOV)", f"₩{int(curr_aov):,}", calc_delta(curr_aov, prev_aov))

        if ts_df is not None and not ts_df.empty:
            st.markdown("---")
            st.subheader(f"📊 {time_unit} 추이 분석 (매출액 / 주문수 / 세션)")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=ts_df['period_label'], y=ts_df['revenue'], name='매출액', marker_color='#2ca02c', yaxis='y1'))
            fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['orders'], name='주문수', line=dict(color='#FF4B4B', width=3), yaxis='y2'))
            fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['sessions'], name='세션 수', line=dict(color='#1f77b4', width=2, dash='dot'), yaxis='y2'))
            fig.update_layout(
                yaxis=dict(title="매출액 (원)", side="left", tickformat=","),
                yaxis2=dict(title="주문/세션 (건)", side="right", overlaying="y", tickformat=","),
                hovermode="x unified", template="plotly_white",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            fig.update_yaxes(tickformat=",d") 
            st.plotly_chart(fig, use_container_width=True)
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go

# -------------------------------------------------
# 1. 페이지 설정
# -------------------------------------------------
st.set_page_config(page_title="SIDIZ Intelligence Dashboard", layout="wide")

# -------------------------------------------------
# 2. BigQuery 클라이언트
# -------------------------------------------------
@st.cache_resource
def get_bq_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(
            info, location="asia-northeast3"
        )
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# -------------------------------------------------
# 3. 데이터 추출 함수
# -------------------------------------------------
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None:
        return None, None

    if time_unit == "일별":
        group_sql = "CAST(date AS STRING)"
    elif time_unit == "주별":
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, WEEK) AS STRING), ' ~ ', CAST(LAST_DAY(date, WEEK) AS STRING))"
    else:
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, MONTH) AS STRING), ' ~ ', CAST(LAST_DAY(date, MONTH) AS STRING))"

    summary_query = f"""
    WITH raw_data AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as session_num,
        event_name,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{min(start_c, start_p).strftime('%Y%m%d')}'
                            AND '{max(end_c, end_p).strftime('%Y%m%d')}'
    )
    SELECT 
        CASE 
            WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current'
            WHEN date BETWEEN '{start_p}' AND '{end_p}' THEN 'Previous'
        END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN session_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(purchase_revenue) as revenue
    FROM raw_data
    WHERE session_id IS NOT NULL
    GROUP BY 1
    HAVING type IS NOT NULL
    """

    ts_query = f"""
    WITH ts_raw AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        event_name,
        ecommerce.purchase_revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}'
                            AND '{end_c.strftime('%Y%m%d')}'
    )
    SELECT 
        {group_sql} as period_label,
        SUM(purchase_revenue) as revenue,
        COUNTIF(event_name = 'purchase') as orders,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions
    FROM ts_raw
    WHERE session_id IS NOT NULL
    GROUP BY 1
    ORDER BY 1
    """

    try:
        return (
            client.query(summary_query).to_dataframe(),
            client.query(ts_query).to_dataframe(),
        )
    except Exception as e:
        st.error(f"⚠️ 데이터 쿼리 오류: {e}")
        return None, None

# -------------------------------------------------
# 4. AI 인사이트 생성 로직
# -------------------------------------------------
def generate_ai_insights(curr, prev):
    insights = []

    curr_cr = curr['orders']/curr['sessions'] if curr['sessions'] > 0 else 0
    prev_cr = prev['orders']/prev['sessions'] if prev['sessions'] > 0 else 0

    curr_aov = curr['revenue']/curr['orders'] if curr['orders'] > 0 else 0
    prev_aov = prev['revenue']/prev['orders'] if prev['orders'] > 0 else 0

    if curr['revenue'] > prev['revenue'] and curr['orders'] <= prev['orders']:
        insights.append({
            "title": "매출 구조 변화",
            "content": "매출은 증가했지만 주문 수는 감소했습니다. 평균 객단가(AOV) 상승에 의존한 성장으로, 할인/구성 변경 시 리스크가 존재합니다."
        })

    if curr_cr < prev_cr:
        insights.append({
            "title": "전환율 하락",
            "content": "구매 전환율이 이전 기간 대비 하락했습니다. 유입 품질 저하 또는 상품 상세·결제 단계 이탈 가능성이 있습니다."
        })

    if curr['sessions'] > prev['sessions'] and curr['orders'] <= prev['orders']:
        insights.append({
            "title": "트래픽 질 변화",
            "content": "세션은 증가했지만 주문은 정체되어 있습니다. 정보 탐색 목적 유입 비중이 증가했을 가능성이 있습니다."
        })

    if curr['new_users']/curr['users'] > 0.7:
        insights.append({
            "title": "신규 유입 중심 구조",
            "content": "신규 방문자 비중이 매우 높습니다. 브랜드 확산 단계로 보이며, 재구매/리텐션 전략 보완이 필요합니다."
        })

    if not insights:
        insights.append({
            "title": "안정적 성과",
            "content": "주요 KPI에서 구조적인 이상 징후는 발견되지 않았습니다."
        })

    return insights

# -------------------------------------------------
# 5. UI
# -------------------------------------------------
st.title("🪑 SIDIZ AI Intelligence Dashboard")

with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input(
        "분석 기간 (Current)",
        [datetime.now() - timedelta(days=8), datetime.now() - timedelta(days=1)]
    )
    comp_date = st.date_input(
        "비교 기간 (Previous)",
        [datetime.now() - timedelta(days=16), datetime.now() - timedelta(days=9)]
    )
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_date) == 2 and len(comp_date) == 2:
    summary_df, ts_df = get_dashboard_data(
        curr_date[0], curr_date[1], comp_date[0], comp_date[1], time_unit
    )

    if summary_df is not None and not summary_df.empty:
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0]

        def calc_delta(c, p):
            if p == 0:
                return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        st.subheader("🎯 핵심 성과 요약")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("활성 사용자", f"{int(curr['users']):,}", calc_delta(curr['users'], prev['users']))
        c2.metric("신규 사용자", f"{int(curr['new_users']):,}", calc_delta(curr['new_users'], prev['new_users']))
        c3.metric("총 매출", f"₩{int(curr['revenue']):,}", calc_delta(curr['revenue'], prev['revenue']))
        c4.metric("주문수", f"{int(curr['orders']):,}", calc_delta(curr['orders'], prev['orders']))

        if ts_df is not None and not ts_df.empty:
            st.markdown("---")
            st.subheader(f"📈 {time_unit} 추이 분석")

            fig = go.Figure()
            fig.add_bar(x=ts_df['period_label'], y=ts_df['revenue'], name="매출")
            fig.add_scatter(x=ts_df['period_label'], y=ts_df['orders'], name="주문수", yaxis="y2")

            fig.update_layout(
                yaxis=dict(title="매출"),
                yaxis2=dict(title="주문수", overlaying="y", side="right"),
                hovermode="x unified",
                template="plotly_white"
            )
            st.plotly_chart(fig, use_container_width=True)

            # -------------------------------
            # 🤖 AI 인사이트 카드 영역
            # -------------------------------
            st.markdown("### 🤖 AI 인사이트 요약")

            insights = generate_ai_insights(curr, prev)
            cols = st.columns(len(insights))

            for col, insight in zip(cols, insights):
                with col:
                    st.markdown(
                        f"""
                        <div style="
                            background-color:#ffffff;
                            padding:20px;
                            border-radius:14px;
                            box-shadow:0 6px 18px rgba(0,0,0,0.08);
                            height:100%;
                        ">
                            <h4 style="margin-bottom:10px;">{insight['title']}</h4>
                            <p style="color:#555555; font-size:14px; line-height:1.5;">
                                {insight['content']}
                            </p>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

            st.markdown("#### 🤔 더 깊게 볼 질문")
            st.markdown("""
            - 전환율 하락이 **특정 페이지나 디바이스**에 집중되어 있을까?
            - 매출 성장이 **특정 고가 상품**에 의존하고 있지는 않을까?
            - 최근 유입 증가는 **어떤 채널 변화**에서 시작되었을까?
            """)

else:
    st.info("사이드바에서 분석 기간을 선택해주세요.")
