import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go

# --------------------------------------------------
# 1. Page Config
# --------------------------------------------------
st.set_page_config(
    page_title="SIDIZ AI Intelligence Dashboard",
    page_icon="🪑",
    layout="wide"
)

# --------------------------------------------------
# 2. BigQuery Client (Secrets 기반)
# --------------------------------------------------
@st.cache_resource
def get_bq_client():
    try:
        # Streamlit Secrets에서 정보를 가져와 인증
        info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
            location="asia-northeast3"
        )
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# --------------------------------------------------
# 3. Data Query Function
# --------------------------------------------------
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None, None

    # 시간 단위별 SQL 그룹화 설정
    if time_unit == "일별":
        group_sql = "CAST(date AS STRING)"
    elif time_unit == "주별":
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, WEEK) AS STRING), ' ~ ', CAST(LAST_DAY(date, WEEK) AS STRING))"
    else: # 월별
        group_sql = "CONCAT(CAST(DATE_TRUNC(date, MONTH) AS STRING), ' ~ ', CAST(LAST_DAY(date, MONTH) AS STRING))"

    # 요약 및 시계열 통합 쿼리 (효율성을 위해 날짜 범위 최적화)
    query = f"""
    WITH raw_data AS (
      SELECT 
        PARSE_DATE('%Y%m%d', event_date) as date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as session_num,
        event_name,
        ecommerce.purchase_revenue as revenue
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{min(start_c, start_p).strftime('%Y%m%d')}' AND '{max(end_c, end_p).strftime('%Y%m%d')}'
    )
    SELECT 
        CASE 
            WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' 
            WHEN date BETWEEN '{start_p}' AND '{end_p}' THEN 'Previous' 
        END as period,
        {group_sql} as period_label,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN session_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(revenue) as revenue
    FROM raw_data
    WHERE session_id IS NOT NULL
    GROUP BY 1, 2
    HAVING period IS NOT NULL
    ORDER BY period_label
    """
    
    try:
        df = client.query(query).to_dataframe()
        # 요약용(Summary)과 시계열용(TS) 분리
        summary_df = df.groupby('period').agg({
            'users': 'sum',
            'new_users': 'sum',
            'sessions': 'sum',
            'orders': 'sum',
            'revenue': 'sum'
        }).reset_index()
        
        ts_df = df[df['period'] == 'Current'].copy()
        return summary_df, ts_df
    except Exception as e:
        st.error(f"⚠️ 데이터 쿼리 오류: {e}")
        return None, None

# --------------------------------------------------
# 4. Logic: AI Insight Generation
# --------------------------------------------------
def generate_dynamic_insights(curr, prev):
    insights = []
    
    # 지표 계산
    curr_cr = (curr['orders']/curr['sessions']*100) if curr['sessions'] > 0 else 0
    prev_cr = (prev['orders']/prev['sessions']*100) if prev['sessions'] > 0 else 0
    curr_aov = (curr['revenue']/curr['orders']) if curr['orders'] > 0 else 0
    prev_aov = (prev['revenue']/prev['orders']) if prev['orders'] > 0 else 0

    # 1. 전환율 관련 인사이트
    if curr_cr < prev_cr:
        diff = prev_cr - curr_cr
        insights.append({
            "title": "📉 구매 전환율(CVR) 하락 경고",
            "content": f"현재 전환율이 이전 대비 **{diff:.2f}%p 하락**했습니다. 유입 트래픽 대비 실결제가 부족합니다. 상세페이지의 '장바구니' 전환 단계를 점검하고, 모바일 결제 오류 여부를 확인하세요."
        })
    
    # 2. 트래픽 질 및 매출 구조
    if curr['revenue'] < prev['revenue'] and curr['sessions'] > prev['sessions']:
        insights.append({
            "title": "⚠️ 트래픽 효율성 저하",
            "content": "방문자(세션)는 늘었으나 매출은 오히려 감소했습니다. 정보 탐색 위주의 체리피커형 유입이 늘었거나, 광고 타겟팅의 정교함이 떨어졌을 가능성이 높습니다."
        })

    # 3. 객단가 관련
    if curr_aov > prev_aov * 1.1:
        insights.append({
            "title": "💎 고가치 상품 판매 비중 상승",
            "content": f"평균 객단가(AOV)가 **₩{int(curr_aov-prev_aov):,}** 상승했습니다. 고가 라인업의 판매 호조로 보이며, 이 유입 경로를 파악하여 캠페인을 확장할 필요가 있습니다."
        })
    
    # 기본 인사이트 (데이터가 정상일 때)
    if not insights:
        insights.append({
            "title": "✅ 안정적인 비즈니스 흐름",
            "content": "주요 지표가 안정적인 성과를 보이고 있습니다. 현재의 마케팅 믹스를 유지하면서 신규 가입자 대상의 리텐션 캠페인을 강화해보세요."
        })

    return insights

# --------------------------------------------------
# 5. Sidebar UI
# --------------------------------------------------
with st.sidebar:
    st.header("⚙️ 분석 설정")
    
    today = datetime.now().date()
    curr_date = st.date_input("분석 기간 (Current)", [today - timedelta(days=8), today - timedelta(days=1)])
    prev_date = st.date_input("비교 기간 (Previous)", [today - timedelta(days=16), today - timedelta(days=9)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])
    
    st.info("💡 GA4 데이터를 BigQuery를 통해 실시간 분석합니다.")

# --------------------------------------------------
# 6. Main Dashboard UI
# --------------------------------------------------
st.title("🪑 SIDIZ AI Intelligence Dashboard")

if len(curr_date) == 2 and len(prev_date) == 2:
    summary_df, ts_df = get_dashboard_data(curr_date[0], curr_date[1], prev_date[0], prev_date[1], time_unit)

    if summary_df is not None and not summary_df.empty:
        # 데이터 매핑
        curr = summary_df[summary_df['period'] == 'Current'].iloc[0] if 'Current' in summary_df['period'].values else pd.Series(0, index=summary_df.columns)
        prev = summary_df[summary_df['period'] == 'Previous'].iloc[0] if 'Previous' in summary_df['period'].values else pd.Series(0, index=summary_df.columns)

        def calc_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        # KPI 섹션
        st.subheader("🎯 핵심 성과 요약")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("활성 사용자", f"{int(curr['users']):,}", calc_delta(curr['users'], prev['users']))
        m2.metric("세션 수", f"{int(curr['sessions']):,}", calc_delta(curr['sessions'], prev['sessions']))
        m3.metric("총 매출액", f"₩{int(curr['revenue']):,}", calc_delta(curr['revenue'], prev['revenue']))
        
        curr_cr = (curr['orders']/curr['sessions']*100) if curr['sessions'] > 0 else 0
        prev_cr = (prev['orders']/prev['sessions']*100) if prev['sessions'] > 0 else 0
        m4.metric("구매전환율(CVR)", f"{curr_cr:.2f}%", f"{(curr_cr-prev_cr):+.2f}%p")

        # 추이 차트
        if ts_df is not None and not ts_df.empty:
            st.markdown("---")
            st.subheader(f"📊 {time_unit} 성장 추이")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=ts_df['period_label'], y=ts_df['revenue'], name='매출액', marker_color='#2ca02c', yaxis='y1'))
            fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['orders'], name='주문수', line=dict(color='#FF4B4B', width=3), yaxis='y2'))
            fig.update_layout(
                yaxis=dict(title="매출액 (원)", side="left"),
                yaxis2=dict(title="주문수 (건)", side="right", overlaying="y"),
                hovermode="x unified", template="plotly_white",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig, use_container_width=True)

        # --------------------------------------------------
        # 7. AI Insight Section (고도화된 스타일)
        # --------------------------------------------------
        st.markdown("---")
        st.subheader("🧠 AI 비즈니스 인사이트")
        
        insights = generate_dynamic_insights(curr, prev)
        
        # 인사이트 카드 출력
        idx = 0
        for insight in insights:
            with st.expander(insight['title'], expanded=(idx == 0)):
                st.markdown(f"""
                <div style="padding:10px; border-left: 5px solid #ff4b4b; background-color: #f9f9f9;">
                    {insight['content']}
                </div>
                """, unsafe_allow_html=True)
                st.markdown("**권장 Action Item:**")
                if "전환율" in insight['title']:
                    st.write("- [ ] 유입 채널별 전환율 확인 (UTM 소스별)")
                    st.write("- [ ] 결제 페이지 이탈률 확인")
                elif "트래픽" in insight['title']:
                    st.write("- [ ] 신규 광고 캠페인의 타겟 적절성 검토")
                    st.write("- [ ] 체류 시간 분석을 통한 콘텐츠 매력도 점검")
                else:
                    st.write("- [ ] 현재 성과 유지 및 우수 채널 예산 증액")
            idx += 1

        # 딥다이브 질문
        st.info("🤔 **데이터 담당자에게 물어보세요:**\n"
                "- '전환율이 떨어진 시점에 특정 디바이스(iOS/Android) 이슈가 있었나요?'\n"
                "- '신규 유입 고객의 첫 구매 상품 비중이 어떻게 되나요?'")

        st.caption(f"📌 기준: Google Analytics 4 (BigQuery) | 마지막 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

else:
    st.warning("사이드바에서 분석 기간(시작일과 종료일)을 모두 선택해주세요.")
