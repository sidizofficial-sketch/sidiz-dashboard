import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Intelligence Dashboard", layout="wide")

if "gemini" in st.secrets and "gemini_api_key" in st.secrets["gemini"]:
    genai.configure(api_key=st.secrets["gemini"]["gemini_api_key"])
    model = genai.GenerativeModel('gemini-1.5-flash')
    HAS_GEMINI = True
else:
    HAS_GEMINI = False

@st.cache_resource
def get_bq_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# -------------------------------------------------
# 2. 데이터 추출 함수 (대량 구매 포함)
# -------------------------------------------------
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None, None, None
    
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p, e_p = start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d')

    if time_unit == "일별": group_sql = "PARSE_DATE('%Y%m%d', event_date)"
    elif time_unit == "주별": group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)"
    else: group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)"

    query = f"""
    WITH base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id, event_name, ecommerce.purchase_revenue,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    )
    SELECT 
        CASE WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(IFNULL(purchase_revenue, 0)) as revenue,
        COUNTIF(event_name = 'purchase' AND purchase_revenue >= 1500000) as bulk_orders,
        SUM(CASE WHEN event_name = 'purchase' AND purchase_revenue >= 1500000 THEN purchase_revenue ELSE 0 END) as bulk_revenue
    FROM base GROUP BY 1 HAVING type IS NOT NULL
    """

    ts_query = f"""
    SELECT 
        CAST({group_sql} AS STRING) as period_label, 
        SUM(IFNULL(ecommerce.purchase_revenue, 0)) as revenue,
        COUNTIF(event_name = 'purchase' AND ecommerce.purchase_revenue >= 1500000) as bulk_orders
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY 1
    """

    source_query = f"""
    SELECT traffic_source.source, SUM(IFNULL(ecommerce.purchase_revenue, 0)) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY revenue DESC LIMIT 5
    """

    try:
        return client.query(query).to_dataframe(), client.query(ts_query).to_dataframe(), client.query(source_query).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ 쿼리 오류: {e}")
        return None, None, None

# -------------------------------------------------
# 3. AI 인사이트 함수 (대량 구매 로직 강화)
# -------------------------------------------------
def generate_deep_report(curr, prev, source_df):
    if not HAS_GEMINI: return "🤖 AI API 설정이 필요합니다."

    # 주요 지표 계산
    rev_delta = ((curr['revenue'] - prev['revenue']) / prev['revenue'] * 100) if prev['revenue'] > 0 else 0
    c_cr = (curr['orders']/curr['sessions']*100) if curr['sessions'] > 0 else 0
    
    # 대량 구매 지표
    c_bulk_share = (curr['bulk_revenue'] / curr['revenue'] * 100) if curr['revenue'] > 0 else 0
    p_bulk_share = (prev['bulk_revenue'] / prev['revenue'] * 100) if prev['revenue'] > 0 else 0
    bulk_delta = curr['bulk_orders'] - prev['bulk_orders']
    
    top_channels = source_df['source'].tolist()[:3] if not source_df.empty else ["N/A"]

    prompt = f"""
    시디즈 데이터 전략가로서 보고서를 작성하세요. 
    특히 '대량 구매(150만원 이상)' 데이터가 이번 성과에 미친 영향을 분석에 반드시 포함하되, 변화가 두드러질 때만 집중적으로 다루세요.

    [핵심 데이터]
    - 전체 매출: {int(curr['revenue']):,}원 ({rev_delta:+.1f}%)
    - 전체 주문: {curr['orders']}건 / 전환율 {c_cr:.2f}%
    - 대량 구매: {curr['bulk_orders']}건 (매출비중 {c_bulk_share:.1f}%, 전기대비 {bulk_delta:+}건)
    - 주요 채널: {top_channels}

    [작성 구조]
    1.🎯 한 줄 요약: 매출 성패의 '진짜 원인' (대량구매 영향력 여부 포함)
    2.🔎 현상 분석 (What): 대량구매와 일반구매 중 무엇이 지표를 견인했는지 비교.
    3.💡 인과 추론 (Why): 채널 유입과 대량 구매 발생 사이의 상관관계 추측.
    4.🚀 Action Plan: 현재 대량구매 비중에 따른 B2B 혹은 프로모션 전략 제안.
    """
    try:
        return model.generate_content(prompt).text
    except: return "인사이트 생성 실패"

# -------------------------------------------------
# 4. 메인 UI 및 출력 (수정 버전)
# -------------------------------------------------
st.title("🪑 SIDIZ Intelligence Dashboard")

today = datetime.now()
with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간", [today - timedelta(days=7), today - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [today - timedelta(days=14), today - timedelta(days=8)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_date) == 2 and len(comp_date) == 2:
    summary_df, ts_df, source_df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], time_unit)
    
    if summary_df is not None and not summary_df.empty:
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else curr

        # [8대 지표 출력]
        def get_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        st.subheader("🎯 핵심 성과 요약")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("활성 사용자", f"{int(curr['users']):,}", get_delta(curr['users'], prev['users']))
        c1.metric("세션 수", f"{int(curr['sessions']):,}", get_delta(curr['sessions'], prev['sessions']))
        
        c2.metric("신규 사용자", f"{int(curr['new_users']):,}", get_delta(curr['new_users'], prev['new_users']))
        c2.metric("주문 수", f"{int(curr['orders']):,}", get_delta(curr['orders'], prev['orders']))
        
        c_nv = (curr['new_users']/curr['users']*100) if curr['users'] > 0 else 0
        p_nv = (prev['new_users']/prev['users']*100) if prev['users'] > 0 else 0
        c3.metric("신규 방문율", f"{c_nv:.1f}%", f"{c_nv-p_nv:+.1f}%p")
        c3.metric("구매전환율", f"{(curr['orders']/curr['sessions']*100):.2f}%", f"{(curr['orders']/curr['sessions']*100 - prev['orders']/prev['sessions']*100):+.2f}%p")
        
        c4.metric("총 매출액", f"₩{int(curr['revenue']):,}", get_delta(curr['revenue'], prev['revenue']))
        c_aov = (curr['revenue']/curr['orders']) if curr['orders'] > 0 else 0
        p_aov = (prev['revenue']/prev['orders']) if prev['orders'] > 0 else 0
        c4.metric("평균 객단가(AOV)", f"₩{int(c_aov):,}", get_delta(c_aov, p_aov))

        # [대량 구매 성과 섹션]
        st.markdown("---")
        st.subheader("📦 대량 구매 세그먼트 (150만 원↑)")
        b1, b2, b3 = st.columns(3)

        # 대량 구매 비율 계산
        bulk_ratio = (curr['bulk_revenue'] / curr['revenue'] * 100) if curr['revenue'] > 0 else 0

        b1.metric("대량 주문 건수", f"{int(curr['bulk_orders'])}건", f"{int(curr['bulk_orders'] - prev['bulk_orders']):+}건")
        b2.metric("대량 구매 매출", f"₩{int(curr['bulk_revenue']):,}", get_delta(curr['bulk_revenue'], prev['bulk_revenue']))
        b3.metric("대량 구매 매출 비중", f"{bulk_ratio:.1f}%")

        # [차트 섹션]
        st.markdown("---")
        st.subheader(f"📊 {time_unit} 매출 추이")
        fig = go.Figure()
        fig.add_bar(x=ts_df['period_label'], y=ts_df['revenue'], name="전체 매출", marker_color='#2ca02c')
        fig.add_scatter(x=ts_df['period_label'], y=ts_df['bulk_orders'], name="대량 주문수", yaxis="y2", line=dict(color='#FF4B4B'))
        fig.update_layout(yaxis2=dict(overlaying="y", side="right"), template="plotly_white", hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # -------------------------------------------------
        # [고도화 인사이트 카드 섹션]
        # -------------------------------------------------
        st.markdown("---")
        st.subheader("💡 데이터 기반 핵심 인사이트")
        
        # 데이터 계산 로직
        bulk_ratio = (curr['bulk_revenue'] / curr['revenue'] * 100) if curr['revenue'] > 0 else 0
        nu_ratio = (curr['new_users'] / curr['users'] * 100) if curr['users'] > 0 else 0
        u_delta = ((curr['users'] - prev['users']) / prev['users'] * 100) if prev['users'] > 0 else 0

        i1, i2 = st.columns(2)
        
        with i1:
            # 1. 총 매출액 인사이트
            msg_rev = f"대량 구매가 전체 매출의 {bulk_ratio:.1f}%를 차지하며 성장을 견인 중" if bulk_ratio > 15 else "일반 구매 위주의 매출 구조 유지 중"
            st.info(f"**💰 총 매출액**\n\n{msg_rev}")
            
            # 2. 주문수 인사이트
            st.info(f"**📦 주문수 분석**\n\n총 주문 {int(curr['orders'])}건 중 대량 구매 주문 {int(curr['bulk_orders'])}건 포함")
            
            # 3. 사용자 분석
            st.info(f"**👥 사용자 행동**\n\n활성 사용자 증감률 {u_delta:+.1f}%, 신규 사용자 비중 {nu_ratio:.1f}%")

        with i2:
            # 4. 객단가 분석
            aov_msg = "대량 구매 비중 상승으로 객단가 방어" if c_aov > p_aov else "객단가 하락세, 묶음 상품 구성 검토 필요"
            st.success(f"**💳 평균 객단가 (AOV)**\n\n₩{int(c_aov):,} ({get_delta(c_aov, p_aov)}), {aov_msg}")
            
            # 5. 유입 채널 분석 (Top 3 추출)
            if source_df is not None and not source_df.empty:
                top_sources = source_df['source'].head(3).tolist()
                st.success(f"**🔗 주요 유입 채널**\n\n{', '.join(top_sources)} 채널이 매출 상위 견인")
            
            # 6. 고객 행동/퍼널 (예시 로직 - 실제 이탈률 데이터 연결 가능)
            st.success(f"**🛤️ 고객 행동/퍼널**\n\n평균 구매 수량 {(curr['orders']/curr['users'] if curr['users']>0 else 0):.1f}개, 결합 상품(쿠션, 발받침) 연계 판매 강화 필요")

else:
    st.info("💡 사이드바에서 분석 기간을 선택해주세요.")
