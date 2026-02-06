import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Intelligence Dashboard", layout="wide")

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
# 2. 핵심 지표 데이터 추출
# -------------------------------------------------
def get_dashboard_data(start_c, end_c, start_p, end_p):
    if client is None: 
        return None, None, None
    
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p, e_p = start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d')

    # 핵심 지표 쿼리 (회원가입 포함)
    query = f"""
    WITH base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id, 
            event_name, 
            ecommerce.purchase_revenue,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    )
    SELECT 
        CASE WHEN date BETWEEN PARSE_DATE('%Y%m%d', '{s_c}') AND PARSE_DATE('%Y%m%d', '{e_c}') THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions,
        COUNTIF(event_name = 'sign_up') as signups,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(IFNULL(purchase_revenue, 0)) as revenue,
        COUNTIF(event_name = 'purchase' AND purchase_revenue >= 1500000) as bulk_orders,
        SUM(CASE WHEN event_name = 'purchase' AND purchase_revenue >= 1500000 THEN purchase_revenue ELSE 0 END) as bulk_revenue
    FROM base 
    GROUP BY 1 
    HAVING type IS NOT NULL
    """

    # 시계열 데이터 (일별)
    ts_query = f"""
    SELECT 
        event_date,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions,
        SUM(IFNULL(ecommerce.purchase_revenue, 0)) as revenue,
        COUNTIF(event_name = 'purchase') as orders
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 
    ORDER BY 1
    """

    try:
        summary_df = client.query(query).to_dataframe()
        ts_df = client.query(ts_query).to_dataframe()
        return summary_df, ts_df
    except Exception as e:
        st.error(f"⚠️ 쿼리 오류: {e}")
        return None, None

# -------------------------------------------------
# 3. 인사이트 분석 쿼리
# -------------------------------------------------
def get_insight_data(start_c, end_c, start_p, end_p):
    """매출 변동 원인 분석을 위한 상세 데이터"""
    if client is None:
        return None
    
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p, e_p = start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d')
    
    # 1. 제품별 매출 변화
    product_query = f"""
    WITH current_products AS (
        SELECT 
            item.item_name as product,
            SUM(item.quantity) as qty,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`,
        UNNEST(items) as item
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
        AND event_name = 'purchase'
        GROUP BY 1
    ),
    previous_products AS (
        SELECT 
            item.item_name as product,
            SUM(item.quantity) as qty,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`,
        UNNEST(items) as item
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}'
        AND event_name = 'purchase'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.product, p.product) as product,
        IFNULL(c.revenue, 0) as current_revenue,
        IFNULL(p.revenue, 0) as previous_revenue,
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0) as revenue_change,
        IFNULL(c.qty, 0) as current_qty,
        IFNULL(p.qty, 0) as previous_qty
    FROM current_products c
    FULL OUTER JOIN previous_products p ON c.product = p.product
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC
    LIMIT 10
    """
    
    # 2. 채널별 매출 변화
    channel_query = f"""
    WITH current_channels AS (
        SELECT 
            CONCAT(traffic_source.source, ' / ', traffic_source.medium) as channel,
            SUM(ecommerce.purchase_revenue) as revenue,
            COUNT(DISTINCT user_pseudo_id) as users
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
        AND event_name = 'purchase'
        GROUP BY 1
    ),
    previous_channels AS (
        SELECT 
            CONCAT(traffic_source.source, ' / ', traffic_source.medium) as channel,
            SUM(ecommerce.purchase_revenue) as revenue,
            COUNT(DISTINCT user_pseudo_id) as users
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}'
        AND event_name = 'purchase'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.channel, p.channel) as channel,
        IFNULL(c.revenue, 0) as current_revenue,
        IFNULL(p.revenue, 0) as previous_revenue,
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0) as revenue_change,
        IFNULL(c.users, 0) as current_users,
        IFNULL(p.users, 0) as previous_users
    FROM current_channels c
    FULL OUTER JOIN previous_channels p ON c.channel = p.channel
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC
    LIMIT 10
    """
    
    # 3. 인구통계 변화 (국가, 도시)
    demo_query = f"""
    WITH current_demo AS (
        SELECT 
            geo.country as country,
            geo.city as city,
            SUM(ecommerce.purchase_revenue) as revenue,
            COUNT(DISTINCT user_pseudo_id) as users
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
        AND event_name = 'purchase'
        GROUP BY 1, 2
    ),
    previous_demo AS (
        SELECT 
            geo.country as country,
            geo.city as city,
            SUM(ecommerce.purchase_revenue) as revenue,
            COUNT(DISTINCT user_pseudo_id) as users
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}'
        AND event_name = 'purchase'
        GROUP BY 1, 2
    )
    SELECT 
        COALESCE(c.country, p.country) as country,
        COALESCE(c.city, p.city) as city,
        IFNULL(c.revenue, 0) as current_revenue,
        IFNULL(p.revenue, 0) as previous_revenue,
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0) as revenue_change
    FROM current_demo c
    FULL OUTER JOIN previous_demo p ON c.country = p.country AND c.city = p.city
    WHERE IFNULL(c.revenue, 0) + IFNULL(p.revenue, 0) > 0
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC
    LIMIT 10
    """
    
    # 4. 디바이스별 변화
    device_query = f"""
    WITH current_device AS (
        SELECT 
            device.category as device,
            SUM(ecommerce.purchase_revenue) as revenue,
            COUNT(DISTINCT user_pseudo_id) as users
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
        AND event_name = 'purchase'
        GROUP BY 1
    ),
    previous_device AS (
        SELECT 
            device.category as device,
            SUM(ecommerce.purchase_revenue) as revenue,
            COUNT(DISTINCT user_pseudo_id) as users
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}'
        AND event_name = 'purchase'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.device, p.device) as device,
        IFNULL(c.revenue, 0) as current_revenue,
        IFNULL(p.revenue, 0) as previous_revenue,
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0) as revenue_change
    FROM current_device c
    FULL OUTER JOIN previous_device p ON c.device = p.device
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC
    """
    
    try:
        product_df = client.query(product_query).to_dataframe()
        channel_df = client.query(channel_query).to_dataframe()
        demo_df = client.query(demo_query).to_dataframe()
        device_df = client.query(device_query).to_dataframe()
        
        return {
            'product': product_df,
            'channel': channel_df,
            'demo': demo_df,
            'device': device_df
        }
    except Exception as e:
        st.error(f"⚠️ 인사이트 데이터 추출 오류: {e}")
        return None

# -------------------------------------------------
# 4. 데이터 기반 인사이트 생성
# -------------------------------------------------
def generate_data_insights(curr, prev, insight_data):
    """GA4 데이터 기반 자동 인사이트 생성"""
    
    insights = []
    
    # 1. 전체 매출 변동 분석
    revenue_change = curr['revenue'] - prev['revenue']
    revenue_change_pct = (revenue_change / prev['revenue'] * 100) if prev['revenue'] > 0 else 0
    
    if abs(revenue_change_pct) > 5:
        direction = "증가" if revenue_change > 0 else "감소"
        insights.append(f"### 📊 전체 매출 {direction}")
        insights.append(f"매출이 전기 대비 **{abs(revenue_change_pct):.1f}% {direction}**했습니다 (₩{abs(revenue_change):,.0f}).")
    
    # 2. 제품 영향 분석
    if insight_data and 'product' in insight_data:
        top_product = insight_data['product'].iloc[0]
        if abs(top_product['revenue_change']) > 1000000:
            direction = "증가" if top_product['revenue_change'] > 0 else "감소"
            insights.append(f"\n### 🏆 주요 제품 영향")
            insights.append(f"**'{top_product['product']}'** 제품의 매출이 ₩{abs(top_product['revenue_change']):,.0f} {direction}하여 전체 매출에 큰 영향을 미쳤습니다.")
            insights.append(f"- 현재 기간: ₩{top_product['current_revenue']:,.0f} ({top_product['current_qty']:.0f}개)")
            insights.append(f"- 이전 기간: ₩{top_product['previous_revenue']:,.0f} ({top_product['previous_qty']:.0f}개)")
    
    # 3. 채널 영향 분석
    if insight_data and 'channel' in insight_data:
        top_channel = insight_data['channel'].iloc[0]
        if abs(top_channel['revenue_change']) > 500000:
            direction = "증가" if top_channel['revenue_change'] > 0 else "감소"
            insights.append(f"\n### 🎯 주요 채널 영향")
            insights.append(f"**{top_channel['channel']}** 채널의 매출이 ₩{abs(top_channel['revenue_change']):,.0f} {direction}했습니다.")
            insights.append(f"- 현재 기간 구매자: {top_channel['current_users']:.0f}명")
            insights.append(f"- 이전 기간 구매자: {top_channel['previous_users']:.0f}명")
    
    # 4. 대량 구매 영향 분석
    bulk_change = curr['bulk_orders'] - prev['bulk_orders']
    bulk_rev_change = curr['bulk_revenue'] - prev['bulk_revenue']
    
    if abs(bulk_change) >= 2 or abs(bulk_rev_change) > 5000000:
        direction = "증가" if bulk_change > 0 else "감소"
        insights.append(f"\n### 💼 대량 구매 (150만원 이상) 영향")
        insights.append(f"대량 구매 주문이 **{abs(bulk_change):.0f}건 {direction}**했습니다 (매출 ₩{abs(bulk_rev_change):,.0f} {direction}).")
        
        bulk_share_curr = (curr['bulk_revenue'] / curr['revenue'] * 100) if curr['revenue'] > 0 else 0
        bulk_share_prev = (prev['bulk_revenue'] / prev['revenue'] * 100) if prev['revenue'] > 0 else 0
        insights.append(f"- 전체 매출 대비 비중: {bulk_share_curr:.1f}% (전기: {bulk_share_prev:.1f}%)")
    
    # 5. 인구통계 변화 분석
    if insight_data and 'demo' in insight_data and not insight_data['demo'].empty:
        top_demo = insight_data['demo'].iloc[0]
        if abs(top_demo['revenue_change']) > 500000:
            direction = "증가" if top_demo['revenue_change'] > 0 else "감소"
            insights.append(f"\n### 🌍 지역별 변화")
            location = f"{top_demo['country']} / {top_demo['city']}" if pd.notna(top_demo['city']) else top_demo['country']
            insights.append(f"**{location}** 지역의 매출이 ₩{abs(top_demo['revenue_change']):,.0f} {direction}했습니다.")
    
    # 6. 디바이스 변화 분석
    if insight_data and 'device' in insight_data:
        for _, row in insight_data['device'].iterrows():
            if abs(row['revenue_change']) > 1000000:
                direction = "증가" if row['revenue_change'] > 0 else "감소"
                insights.append(f"\n### 📱 디바이스 변화")
                insights.append(f"**{row['device']}** 디바이스의 매출이 ₩{abs(row['revenue_change']):,.0f} {direction}했습니다.")
                break
    
    # 7. 전환율 분석
    curr_cr = (curr['orders'] / curr['sessions'] * 100) if curr['sessions'] > 0 else 0
    prev_cr = (prev['orders'] / prev['sessions'] * 100) if prev['sessions'] > 0 else 0
    cr_change = curr_cr - prev_cr
    
    if abs(cr_change) > 0.2:
        direction = "개선" if cr_change > 0 else "하락"
        insights.append(f"\n### 🎯 전환율 {direction}")
        insights.append(f"구매 전환율이 **{abs(cr_change):.2f}%p {direction}**했습니다 ({prev_cr:.2f}% → {curr_cr:.2f}%).")
    
    # 8. 신규 사용자 영향
    new_user_change_pct = ((curr['new_users'] - prev['new_users']) / prev['new_users'] * 100) if prev['new_users'] > 0 else 0
    
    if abs(new_user_change_pct) > 10:
        direction = "증가" if new_user_change_pct > 0 else "감소"
        insights.append(f"\n### 👥 신규 사용자 {direction}")
        insights.append(f"신규 사용자가 **{abs(new_user_change_pct):.1f}% {direction}**했습니다 ({prev['new_users']:.0f}명 → {curr['new_users']:.0f}명).")
    
    if not insights:
        return "데이터 분석 결과, 전기 대비 큰 변화가 발견되지 않았습니다."
    
    return "\n".join(insights)

# -------------------------------------------------
# 5. 메인 UI
# -------------------------------------------------
st.title("🪑 SIDIZ AI Intelligence Dashboard")

today = datetime.now()
with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간", [today - timedelta(days=7), today - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [today - timedelta(days=14), today - timedelta(days=8)])

if len(curr_date) == 2 and len(comp_date) == 2:
    summary_df, ts_df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1])
    
    if summary_df is not None and not summary_df.empty:
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else curr

        # 증감율 계산 함수
        def get_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        # [9대 지표 출력 - 회원가입 추가]
        st.subheader("🎯 핵심 성과 요약")
        c1, c2, c3, c4 = st.columns(4)
        
        c1.metric("활성 사용자", f"{int(curr['users']):,}", get_delta(curr['users'], prev['users']))
        c1.metric("세션 수", f"{int(curr['sessions']):,}", get_delta(curr['sessions'], prev['sessions']))
        
        c2.metric("신규 사용자", f"{int(curr['new_users']):,}", get_delta(curr['new_users'], prev['new_users']))
        c2.metric("회원가입", f"{int(curr['signups']):,}", get_delta(curr['signups'], prev['signups']))
        
        c_nv = (curr['new_users']/curr['users']*100) if curr['users'] > 0 else 0
        p_nv = (prev['new_users']/prev['users']*100) if prev['users'] > 0 else 0
        c3.metric("신규 방문율", f"{c_nv:.1f}%", f"{c_nv-p_nv:+.1f}%p")
        c3.metric("구매전환율", f"{(curr['orders']/curr['sessions']*100):.2f}%", f"{(curr['orders']/curr['sessions']*100 - prev['orders']/prev['sessions']*100):+.2f}%p")
        
        c4.metric("주문 수", f"{int(curr['orders']):,}", get_delta(curr['orders'], prev['orders']))
        c4.metric("총 매출액", f"₩{int(curr['revenue']):,}", get_delta(curr['revenue'], prev['revenue']))
        
        c_aov = (curr['revenue']/curr['orders']) if curr['orders'] > 0 else 0
        p_aov = (prev['revenue']/prev['orders']) if prev['orders'] > 0 else 0
        
        st.markdown("---")
        st.metric("평균 객단가(AOV)", f"₩{int(c_aov):,}", get_delta(c_aov, p_aov))

        # [대량 구매 세그먼트]
        st.markdown("---")
        st.subheader("📦 대량 구매 세그먼트 (150만 원↑)")
        b1, b2, b3 = st.columns(3)
        b1.metric("대량 주문 건수", f"{int(curr['bulk_orders'])}건", f"{int(curr['bulk_orders'] - prev['bulk_orders']):+}건")
        b2.metric("대량 구매 매출", f"₩{int(curr['bulk_revenue']):,}", get_delta(curr['bulk_revenue'], prev['bulk_revenue']))
        b3.metric("대량 구매 매출 비중", f"{(curr['bulk_revenue']/curr['revenue']*100 if curr['revenue']>0 else 0):.1f}%")

        # [매출 추이 차트 - 3축]
        st.markdown("---")
        st.subheader("📊 일별 매출 추이")
        
        if ts_df is not None and not ts_df.empty:
            # 전환율 계산
            ts_df['conversion_rate'] = (ts_df['orders'] / ts_df['sessions'] * 100).fillna(0)
            
            # 3축 차트 생성
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # 세션 수 (왼쪽 축)
            fig.add_trace(
                go.Scatter(x=ts_df['event_date'], y=ts_df['sessions'], name="세션 수", 
                          line=dict(color='#1f77b4', width=2)),
                secondary_y=False
            )
            
            # 매출액 (오른쪽 축)
            fig.add_trace(
                go.Bar(x=ts_df['event_date'], y=ts_df['revenue'], name="매출액",
                      marker_color='#2ca02c', opacity=0.6),
                secondary_y=True
            )
            
            # 구매 전환율 (왼쪽 축, 작은 값)
            fig.add_trace(
                go.Scatter(x=ts_df['event_date'], y=ts_df['conversion_rate'], name="구매 전환율 (%)",
                          line=dict(color='#ff7f0e', width=2, dash='dash')),
                secondary_y=False
            )
            
            fig.update_xaxes(title_text="날짜")
            fig.update_yaxes(title_text="세션 수 / 전환율 (%)", secondary_y=False)
            fig.update_yaxes(title_text="매출액 (원)", secondary_y=True)
            
            fig.update_layout(
                template="plotly_white",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig, use_container_width=True)

        # [데이터 기반 인사이트]
        st.markdown("---")
        st.subheader("🧠 GA4/BigQuery 데이터 인사이트")
        
        with st.spinner("데이터 분석 중..."):
            insight_data = get_insight_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1])
            insights = generate_data_insights(curr, prev, insight_data)
            st.markdown(insights)
            
            # 상세 데이터 테이블 (옵션)
            with st.expander("📋 상세 분석 데이터 보기"):
                if insight_data:
                    tab1, tab2, tab3, tab4 = st.tabs(["제품별", "채널별", "지역별", "디바이스별"])
                    
                    with tab1:
                        st.dataframe(insight_data['product'].head(10), use_container_width=True)
                    with tab2:
                        st.dataframe(insight_data['channel'].head(10), use_container_width=True)
                    with tab3:
                        st.dataframe(insight_data['demo'].head(10), use_container_width=True)
                    with tab4:
                        st.dataframe(insight_data['device'], use_container_width=True)

else:
    st.info("💡 사이드바에서 기간을 선택해주세요.")
