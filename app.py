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
# 2. 데이터 추출 함수 (User Properties 쿼리 수정)
# -------------------------------------------------
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None, None, None, None
    
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p, e_p = start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d')

    if time_unit == "일별": group_sql = "PARSE_DATE('%Y%m%d', event_date)"
    elif time_unit == "주별": group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)"
    else: group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)"

    # [쿼리 1] 메인 요약 및 인구통계 (user_properties 언네스트 구조 반영)
    summary_query = f"""
    WITH base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id, event_name, ecommerce.purchase_revenue as rev,
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'u_gender') as gender,
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'u_age') as age,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT SUM(quantity) FROM UNNEST(items)) as total_qty
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    )
    SELECT 
        CASE WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT sid) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(IFNULL(rev, 0)) as revenue,
        SUM(IFNULL(total_qty, 0)) as total_qty,
        COUNTIF(event_name = 'purchase' AND rev >= 1500000) as bulk_orders,
        SUM(CASE WHEN event_name = 'purchase' AND rev >= 1500000 THEN rev ELSE 0 END) as bulk_revenue,
        IFNULL(APPROX_TOP_SUM(gender, 1, 1)[OFFSET(0)].value, 'unknown') as top_gender,
        IFNULL(APPROX_TOP_SUM(age, 1, 1)[OFFSET(0)].value, 'unknown') as top_age
    FROM base GROUP BY 1 HAVING type IS NOT NULL
    """

    item_query = f"""
    SELECT 
        item_name,
        SUM(CASE WHEN _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}' THEN item_revenue ELSE 0 END) as curr_rev,
        SUM(CASE WHEN _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}' THEN item_revenue ELSE 0 END) as prev_rev
    FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items)
    WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    GROUP BY 1 ORDER BY curr_rev DESC LIMIT 5
    """

    source_query = f"""
    SELECT 
        CONCAT(traffic_source.source, ' / ', traffic_source.medium) as channel,
        SUM(CASE WHEN _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}' THEN ecommerce.purchase_revenue ELSE 0 END) as curr_rev,
        SUM(CASE WHEN _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}' THEN ecommerce.purchase_revenue ELSE 0 END) as prev_rev
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    GROUP BY 1 ORDER BY curr_rev DESC LIMIT 5
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

    try:
        return (client.query(summary_query).to_dataframe(), 
                client.query(item_query).to_dataframe(), 
                client.query(source_query).to_dataframe(), 
                client.query(ts_query).to_dataframe())
    except Exception as e:
        st.error(f"⚠️ 쿼리 오류: {e}")
        return None, None, None, None

# -------------------------------------------------
# 4. 메인 UI 및 출력
# -------------------------------------------------
st.title("🪑 SIDIZ Intelligence Dashboard")

today = datetime.now()
with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간", [today - timedelta(days=7), today - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [today - timedelta(days=14), today - timedelta(days=8)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_date) == 2 and len(comp_date) == 2:
    # ⚠️ 변수를 4개(summary_df, item_df, source_df, ts_df)로 받아야 ValueError가 발생하지 않습니다.
    summary_df, item_df, source_df, ts_df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], time_unit)
    
    if summary_df is not None and not summary_df.empty:
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else curr

        def get_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

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
        # [고도화 인사이트 분석 카드]
        # -------------------------------------------------
        st.markdown("---")
        st.subheader("🧐 기간 대비 특이점 정밀 분석")
        col1, col2 = st.columns(2)

        with col1:
            # 매출 효율성 분석
            rpv_c = curr['revenue'] / curr['sessions'] if curr['sessions'] > 0 else 0
            rpv_p = prev['revenue'] / prev['sessions'] if prev['sessions'] > 0 else 0
            st.info(f"""
            **💰 총 매출액 & 유입 효율**
            * **특징:** {"매출 효율 상승" if rpv_c > rpv_p else "유입 대비 효율 저하"} (유입당 매출 {get_delta(rpv_c, rpv_p)})
            * **신규 고객:** 매출 비중 {(curr['new_users']/curr['users']*100 if curr['users']>0 else 0):.1f}%
            * **인구통계:** {curr['top_age']} 연령대 및 {curr['top_gender']}성 고객 유입 지배적
            """)

            # 유입 채널 분석 (실제 소스/매체 데이터 연동)
            top_channels = [f"{row['channel']} ({get_delta(row['curr_rev'], row['prev_rev'])})" for _, row in source_df.head(3).iterrows()]
            st.info(f"""
            **🔗 유입 및 매출 채널 (Top 3)**
            * **채널별 증감:** {', '.join(top_channels)}
            * **특이사항:** 매출 기여도가 가장 높은 소스/매체는 '{source_df.iloc[0]['channel']}'입니다.
            """)

        with col2:
            # 제품 변동폭 분석 (실제 아이템 데이터 연동)
            item_list = []
            for _, row in item_df.head(3).iterrows():
                item_list.append(f"{row['item_name']} ({get_delta(row['curr_rev'], row['prev_rev'])})")
            
            st.success(f"""
            **💳 객단가 및 제품 기여도**
            * **평균 객단가(AOV):** ₩{int(curr['revenue']/curr['orders'] if curr['orders']>0 else 0):,} ({get_delta(curr['revenue']/curr['orders'] if curr['orders']>0 else 0, prev['revenue']/prev['orders'] if prev['orders']>0 else 0)})
            * **변동폭 상위 제품:**
                {chr(10).join([f"{i+1}. {item}" for i, item in enumerate(item_list)])}
            """)

            # 고객 행동 분석 (수식 교정)
            avg_qty = curr['total_qty'] / curr['orders'] if curr['orders'] > 0 else 0
            st.success(f"""
            **🛤️ 고객 퍼널 및 행동**
            * **평균 구매 수량:** {avg_qty:.2f}개 (주문당 실 판매 피스 수 기준)
            * **퍼널 진단:** 쇼핑 퍼널 기준 이탈률 {100 - (curr['orders']/curr['sessions']*100 if curr['sessions']>0 else 0):.1f}% 관리 필요
            * **Action:** 장바구니 단계에서 소모품(좌판, 바퀴 등) 결합 상품 제안 추천
            """)
else:
    st.info("💡 사이드바에서 분석 기간을 선택해주세요.")
