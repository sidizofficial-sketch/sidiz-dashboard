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
# 2. 데이터 추출 함수 (EASY REPAIR 필터링 포함)
# -------------------------------------------------
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit):
    if client is None: return None, None, None
    
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p, e_p = start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d')

    if time_unit == "일별": group_sql = "PARSE_DATE('%Y%m%d', event_date)"
    elif time_unit == "주별": group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)"
    else: group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)"

    # 핵심 지표 쿼리
    query = f"""
    WITH base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id, event_name, ecommerce.purchase_revenue, ecommerce.transaction_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num,
            items
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    ),
    -- EASY REPAIR만 구매한 주문 식별 (하이픈 포함)
    easy_repair_check AS (
        SELECT 
            transaction_id,
            -- 각 아이템이 EASY REPAIR인지 확인
            (UPPER(COALESCE(item.item_category, '')) LIKE '%EASY REPAIR%' OR
             UPPER(COALESCE(item.item_category, '')) LIKE '%EASY-REPAIR%') as is_easy_repair
        FROM base,
        UNNEST(items) as item
        WHERE event_name = 'purchase'
        GROUP BY transaction_id, item.item_id, item.item_category
    ),
-- EASY REPAIR(소모품)만 구매한 주문 식별 (로직 대폭 강화)
    easy_repair_only_orders AS (
        SELECT transaction_id
        FROM base, UNNEST(items) as item
        WHERE event_name = 'purchase'
        GROUP BY transaction_id
        HAVING LOGICAL_AND(
            -- 1. 카테고리에 EASY REPAIR가 포함됨
            REGEXP_CONTAINS(UPPER(IFNULL(item.item_category, '')), r'EASY.REPAIR') OR 
            -- 2. 상품명에 EASY REPAIR가 포함됨
            REGEXP_CONTAINS(UPPER(IFNULL(item.item_name, '')), r'EASY.REPAIR') OR
            -- 3. 샘플에서 확인된 주요 소모품 키워드들 (필터링 핵심)
            REGEXP_CONTAINS(item.item_name, r'패드|헤드레스트|커버|다리|바퀴|글라이드|블록|좌판|이지리페어')
        )
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
        SUM(CASE WHEN event_name = 'purchase' AND purchase_revenue >= 1500000 THEN purchase_revenue ELSE 0 END) as bulk_revenue,
        -- EASY REPAIR만 구매한 주문 제외
        COUNTIF(event_name = 'purchase' AND transaction_id NOT IN (SELECT transaction_id FROM easy_repair_only_orders)) as filtered_orders,
        SUM(CASE WHEN event_name = 'purchase' AND transaction_id NOT IN (SELECT transaction_id FROM easy_repair_only_orders) THEN purchase_revenue ELSE 0 END) as filtered_revenue
    FROM base 
    GROUP BY 1 
    HAVING type IS NOT NULL
    """

    # 시계열 데이터
    ts_query = f"""
    SELECT 
        CAST({group_sql} AS STRING) as period_label, 
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING))) as sessions,
        SUM(IFNULL(ecommerce.purchase_revenue, 0)) as revenue,
        COUNTIF(event_name = 'purchase') as orders
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    GROUP BY 1 ORDER BY 1
    """

    try:
        return client.query(query).to_dataframe(), client.query(ts_query).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ 쿼리 오류: {e}")
        return None, None

# -------------------------------------------------
# 3. 인사이트 데이터 추출 (TOP3 + 증감율)
# -------------------------------------------------
def get_insight_data(start_c, end_c, start_p, end_p):
    if client is None: return None
    
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p, e_p = start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d')
    
    # 제품별 매출 변화 (TOP3)
    product_query = f"""
    WITH current_products AS (
        SELECT 
            item.item_name as product,
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
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`,
        UNNEST(items) as item
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}'
        AND event_name = 'purchase'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.product, p.product) as product_name,
        IFNULL(c.revenue, 0) as current_revenue,
        IFNULL(p.revenue, 0) as previous_revenue,
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0) as revenue_change,
        ROUND(SAFE_DIVIDE((IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) * 100, IFNULL(p.revenue, 0)), 1) as change_pct
    FROM current_products c
    FULL OUTER JOIN previous_products p ON c.product = p.product
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC
    LIMIT 10
    """
    
    # 채널별 매출 변화 (TOP3)
    channel_query = f"""
    WITH current_channels AS (
        SELECT 
            CONCAT(traffic_source.source, ' / ', traffic_source.medium) as channel,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
        AND event_name = 'purchase'
        GROUP BY 1
    ),
    previous_channels AS (
        SELECT 
            CONCAT(traffic_source.source, ' / ', traffic_source.medium) as channel,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}'
        AND event_name = 'purchase'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.channel, p.channel) as channel_name,
        IFNULL(c.revenue, 0) as current_revenue,
        IFNULL(p.revenue, 0) as previous_revenue,
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0) as revenue_change,
        ROUND(SAFE_DIVIDE((IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) * 100, IFNULL(p.revenue, 0)), 1) as change_pct
    FROM current_channels c
    FULL OUTER JOIN previous_channels p ON c.channel = p.channel
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC
    LIMIT 10
    """
    
    # 지역별 변화
    demo_query = f"""
    WITH current_demo AS (
        SELECT 
            CONCAT(IFNULL(geo.country, 'Unknown'), ' / ', IFNULL(geo.city, 'Unknown')) as location,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
        AND event_name = 'purchase'
        GROUP BY 1
    ),
    previous_demo AS (
        SELECT 
            CONCAT(IFNULL(geo.country, 'Unknown'), ' / ', IFNULL(geo.city, 'Unknown')) as location,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}'
        AND event_name = 'purchase'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.location, p.location) as location_name,
        IFNULL(c.revenue, 0) as current_revenue,
        IFNULL(p.revenue, 0) as previous_revenue,
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0) as revenue_change,
        ROUND(SAFE_DIVIDE((IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) * 100, IFNULL(p.revenue, 0)), 1) as change_pct
    FROM current_demo c
    FULL OUTER JOIN previous_demo p ON c.location = p.location
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC
    LIMIT 10
    """
    
    # 디바이스별 변화
    device_query = f"""
    WITH current_device AS (
        SELECT 
            device.category as device,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
        AND event_name = 'purchase'
        GROUP BY 1
    ),
    previous_device AS (
        SELECT 
            device.category as device,
            SUM(ecommerce.purchase_revenue) as revenue
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}'
        AND event_name = 'purchase'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.device, p.device) as device_name,
        IFNULL(c.revenue, 0) as current_revenue,
        IFNULL(p.revenue, 0) as previous_revenue,
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0) as revenue_change,
        ROUND(SAFE_DIVIDE((IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) * 100, IFNULL(p.revenue, 0)), 1) as change_pct
    FROM current_device c
    FULL OUTER JOIN previous_device p ON c.device = p.device
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC
    """
    
# 인구통계별 매출 변화 (성별&연령 조합) - 고성능/에러방지 버전
    demographics_query = f"""
    WITH raw_purchase AS (
        SELECT 
            _TABLE_SUFFIX as suffix,
            ecommerce.purchase_revenue,
            -- event_params와 user_properties 양쪽 모두에서 추출 시도
            COALESCE(
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'u_gender'),
                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'u_gender'),
                'Unknown'
            ) as gender_raw,
            COALESCE(
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'u_age'),
                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'u_age'),
                'Unknown'
            ) as age_raw
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
        AND event_name = 'purchase'
    ),
    processed_data AS (
        SELECT 
            suffix,
            purchase_revenue,
            CONCAT(
                CASE WHEN gender_raw = 'male' THEN '남성' WHEN gender_raw = 'female' THEN '여성' ELSE gender_raw END,
                ' / ',
                age_raw
            ) as demographic
        FROM raw_purchase
        WHERE gender_raw != 'Unknown' OR age_raw != 'Unknown' -- 정보가 하나라도 있는 경우만
    )
    SELECT 
        demographic as demographic_name,
        SUM(CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' THEN purchase_revenue ELSE 0 END) as current_revenue,
        SUM(CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN purchase_revenue ELSE 0 END) as previous_revenue,
        SUM(CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' THEN purchase_revenue ELSE 0 END) - 
        SUM(CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN purchase_revenue ELSE 0 END) as revenue_change,
        ROUND(SAFE_DIVIDE(
            (SUM(CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' THEN purchase_revenue ELSE 0 END) - SUM(CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN purchase_revenue ELSE 0 END)) * 100,
            SUM(CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN purchase_revenue ELSE 0 END)
        ), 1) as change_pct
    FROM processed_data
    GROUP BY 1
    ORDER BY ABS(revenue_change) DESC
    LIMIT 10
    """
    
    # 채널별 유입(세션) 변화
    channel_sessions_query = f"""
    WITH current_channels AS (
        SELECT 
            CONCAT(traffic_source.source, ' / ', traffic_source.medium) as channel,
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING)
            )) as sessions
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
        GROUP BY 1
    ),
    previous_channels AS (
        SELECT 
            CONCAT(traffic_source.source, ' / ', traffic_source.medium) as channel,
            COUNT(DISTINCT CONCAT(
                user_pseudo_id, 
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING)
            )) as sessions
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.channel, p.channel) as channel_name,
        IFNULL(c.sessions, 0) as current_sessions,
        IFNULL(p.sessions, 0) as previous_sessions,
        IFNULL(c.sessions, 0) - IFNULL(p.sessions, 0) as sessions_change,
        ROUND(SAFE_DIVIDE((IFNULL(c.sessions, 0) - IFNULL(p.sessions, 0)) * 100, IFNULL(p.sessions, 0)), 1) as change_pct
    FROM current_channels c
    FULL OUTER JOIN previous_channels p ON c.channel = p.channel
    ORDER BY ABS(IFNULL(c.sessions, 0) - IFNULL(p.sessions, 0)) DESC
    LIMIT 10
    """
    
# 인구통계별 유입(세션) 변화 - 고성능/에러방지 버전
    demographics_sessions_query = f"""
    WITH raw_sessions AS (
        SELECT 
            _TABLE_SUFFIX as suffix,
            user_pseudo_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as sid,
            COALESCE(
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'u_gender'),
                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'u_gender'),
                'Unknown'
            ) as gender_raw,
            COALESCE(
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'u_age'),
                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'u_age'),
                'Unknown'
            ) as age_raw
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    ),
    processed_sessions AS (
        SELECT 
            suffix,
            CONCAT(user_pseudo_id, CAST(sid AS STRING)) as session_id,
            CONCAT(
                CASE WHEN gender_raw = 'male' THEN '남성' WHEN gender_raw = 'female' THEN '여성' ELSE gender_raw END,
                ' / ',
                age_raw
            ) as demographic
        FROM raw_sessions
        WHERE gender_raw != 'Unknown' OR age_raw != 'Unknown'
    )
    SELECT 
        demographic as demographic_name,
        COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' THEN session_id END) as current_sessions,
        COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN session_id END) as previous_sessions,
        COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' THEN session_id END) - 
        COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN session_id END) as sessions_change,
        ROUND(SAFE_DIVIDE(
            (COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' THEN session_id END) - COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN session_id END)) * 100,
            COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN session_id END)
        ), 1) as change_pct
    FROM processed_sessions
    GROUP BY 1
    ORDER BY ABS(sessions_change) DESC
    LIMIT 10
    """
        
        # 컬럼명을 한글로 변경 (쿼리 후)
        product_df.columns = ['제품명', '현재매출', '이전매출', '매출변화', '증감율']
        channel_df.columns = ['채널', '현재매출', '이전매출', '매출변화', '증감율']
        demo_df.columns = ['지역', '현재매출', '이전매출', '매출변화', '증감율']
        device_df.columns = ['디바이스', '현재매출', '이전매출', '매출변화', '증감율']
        demographics_df.columns = ['인구통계', '현재매출', '이전매출', '매출변화', '증감율']
        channel_sessions_df.columns = ['채널', '현재세션', '이전세션', '세션변화', '증감율']
        demographics_sessions_df.columns = ['인구통계', '현재세션', '이전세션', '세션변화', '증감율']
        
        return {
            'product': product_df,
            'channel_revenue': channel_df,
            'channel_sessions': channel_sessions_df,
            'demo': demo_df,
            'device': device_df,
            'demographics_revenue': demographics_df,
            'demographics_sessions': demographics_sessions_df
        }
    except Exception as e:
        st.error(f"⚠️ 인사이트 데이터 오류: {e}")
        return None

# -------------------------------------------------
# 4. 데이터 기반 인사이트 생성
# -------------------------------------------------
def generate_insights(curr, prev, insight_data):
    insights = []
    
    # 1. 전체 매출 변동
    rev_change = curr['revenue'] - prev['revenue']
    rev_pct = (rev_change / prev['revenue'] * 100) if prev['revenue'] > 0 else 0
    
    if abs(rev_pct) > 3:
        direction = "증가" if rev_change > 0 else "감소"
        insights.append(f"### 📊 전체 매출 {direction}")
        insights.append(f"매출이 **₩{abs(rev_change):,.0f} ({abs(rev_pct):.1f}%) {direction}**했습니다.")
    
    # 2. 제품 영향 (TOP3)
    if insight_data and 'product' in insight_data and not insight_data['product'].empty:
        insights.append(f"\n### 🏆 주요 제품 영향 TOP3")
        for idx, row in insight_data['product'].head(3).iterrows():
            if abs(row['매출변화']) > 500000:
                direction = "↑" if row['매출변화'] > 0 else "↓"
                insights.append(f"**{idx+1}. {row['제품명']}** {direction} ₩{abs(row['매출변화']):,.0f} ({row['증감율']:+.1f}%)")
    
    # 3. 채널 매출 영향 (TOP3)
    if insight_data and 'channel_revenue' in insight_data and not insight_data['channel_revenue'].empty:
        insights.append(f"\n### 🎯 주요 채널 매출 영향 TOP3")
        for idx, row in insight_data['channel_revenue'].head(3).iterrows():
            if abs(row['매출변화']) > 300000:
                direction = "↑" if row['매출변화'] > 0 else "↓"
                insights.append(f"**{idx+1}. {row['채널']}** {direction} ₩{abs(row['매출변화']):,.0f} ({row['증감율']:+.1f}%)")
    
    # 4. 채널 유입 영향 (TOP3)
    if insight_data and 'channel_sessions' in insight_data and not insight_data['channel_sessions'].empty:
        insights.append(f"\n### 🚪 주요 채널 유입 영향 TOP3")
        for idx, row in insight_data['channel_sessions'].head(3).iterrows():
            if abs(row['세션변화']) > 100:
                direction = "↑" if row['세션변화'] > 0 else "↓"
                insights.append(f"**{idx+1}. {row['채널']}** {direction} {abs(row['세션변화']):,.0f}세션 ({row['증감율']:+.1f}%)")
    
    # 5. 인구통계 매출 영향 (TOP3)
    if insight_data and 'demographics_revenue' in insight_data and not insight_data['demographics_revenue'].empty:
        insights.append(f"\n### 👥 인구통계 매출 영향 TOP3")
        for idx, row in insight_data['demographics_revenue'].head(3).iterrows():
            if abs(row['매출변화']) > 300000:
                direction = "↑" if row['매출변화'] > 0 else "↓"
                insights.append(f"**{idx+1}. {row['인구통계']}** {direction} ₩{abs(row['매출변화']):,.0f} ({row['증감율']:+.1f}%)")
    
    # 6. 인구통계 유입 영향 (TOP3)
    if insight_data and 'demographics_sessions' in insight_data and not insight_data['demographics_sessions'].empty:
        insights.append(f"\n### 🚶 인구통계 유입 영향 TOP3")
        for idx, row in insight_data['demographics_sessions'].head(3).iterrows():
            if abs(row['세션변화']) > 100:
                direction = "↑" if row['세션변화'] > 0 else "↓"
                insights.append(f"**{idx+1}. {row['인구통계']}** {direction} {abs(row['세션변화']):,.0f}세션 ({row['증감율']:+.1f}%)")
    
    # 7. 대량 구매 영향
    bulk_change = curr['bulk_revenue'] - prev['bulk_revenue']
    bulk_pct = (bulk_change / prev['bulk_revenue'] * 100) if prev['bulk_revenue'] > 0 else 0
    
    if abs(bulk_pct) > 10 or abs(bulk_change) > 5000000:
        direction = "증가" if bulk_change > 0 else "감소"
        insights.append(f"\n### 💼 대량 구매 영향")
        insights.append(f"대량 구매(150만원↑) 매출이 **₩{abs(bulk_change):,.0f} ({abs(bulk_pct):.1f}%) {direction}**했습니다.")
    
    # 8. 지역 변화
    if insight_data and 'demo' in insight_data and not insight_data['demo'].empty:
        top_demo = insight_data['demo'].iloc[0]
        if abs(top_demo['매출변화']) > 1000000:
            direction = "↑" if top_demo['매출변화'] > 0 else "↓"
            insights.append(f"\n### 🌍 지역별 변화")
            insights.append(f"**{top_demo['지역']}** {direction} ₩{abs(top_demo['매출변화']):,.0f} ({top_demo['증감율']:+.1f}%)")
    
    # 9. 전환율 변화
    curr_cr = (curr['orders'] / curr['sessions'] * 100) if curr['sessions'] > 0 else 0
    prev_cr = (prev['orders'] / prev['sessions'] * 100) if prev['sessions'] > 0 else 0
    cr_change = curr_cr - prev_cr
    
    if abs(cr_change) > 0.15:
        direction = "개선" if cr_change > 0 else "하락"
        insights.append(f"\n### 🎯 구매 전환율 {direction}")
        insights.append(f"전환율이 **{abs(cr_change):.2f}%p {direction}**했습니다 ({prev_cr:.2f}% → {curr_cr:.2f}%).")
    
    return "\n".join(insights) if insights else "📊 전기 대비 큰 변화가 발견되지 않았습니다."

# -------------------------------------------------
# 5. 메인 UI
# -------------------------------------------------
st.title("🪑 SIDIZ AI Intelligence Dashboard")

today = datetime.now()
with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간", [today - timedelta(days=7), today - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [today - timedelta(days=14), today - timedelta(days=8)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_date) == 2 and len(comp_date) == 2:
    summary_df, ts_df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], time_unit)
    
    if summary_df is not None and not summary_df.empty:
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else curr

        def get_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        # [10대 지표 - 2줄 5개씩]
        st.subheader("🎯 핵심 성과 요약")
        
        # 첫 번째 줄 (5개)
        cols = st.columns(5)
        cols[0].metric("활성 사용자", f"{int(curr['users']):,}명", get_delta(curr['users'], prev['users']))
        cols[1].metric("신규 사용자", f"{int(curr['new_users']):,}명", get_delta(curr['new_users'], prev['new_users']))
        cols[2].metric("세션 수", f"{int(curr['sessions']):,}", get_delta(curr['sessions'], prev['sessions']))
        cols[3].metric("회원가입", f"{int(curr['signups']):,}건", get_delta(curr['signups'], prev['signups']))
        
        c_nv = (curr['new_users']/curr['users']*100) if curr['users'] > 0 else 0
        p_nv = (prev['new_users']/prev['users']*100) if prev['users'] > 0 else 0
        cols[4].metric("신규 방문율", f"{c_nv:.1f}%", f"{c_nv-p_nv:+.1f}%p")
        
        # 두 번째 줄 (5개)
        cols = st.columns(5)
        cols[0].metric("주문 수", f"{int(curr['orders']):,}건", get_delta(curr['orders'], prev['orders']))
        cols[1].metric("총 매출액", f"₩{int(curr['revenue']):,}", get_delta(curr['revenue'], prev['revenue']))
        
        c_cr = (curr['orders']/curr['sessions']*100) if curr['sessions'] > 0 else 0
        p_cr = (prev['orders']/prev['sessions']*100) if prev['sessions'] > 0 else 0
        cols[2].metric("구매 전환율", f"{c_cr:.2f}%", f"{c_cr-p_cr:+.2f}%p")
        
        c_aov = (curr['revenue']/curr['orders']) if curr['orders'] > 0 else 0
        p_aov = (prev['revenue']/prev['orders']) if prev['orders'] > 0 else 0
        cols[3].metric("평균 객단가", f"₩{int(c_aov):,}", get_delta(c_aov, p_aov))
        
        # EASY REPAIR만 구매한 주문 제외 객단가
        c_filtered_aov = (curr['filtered_revenue']/curr['filtered_orders']) if curr.get('filtered_orders', 0) > 0 else 0
        p_filtered_aov = (prev['filtered_revenue']/prev['filtered_orders']) if prev.get('filtered_orders', 0) > 0 else 0
        
        if c_filtered_aov > 0:
            cols[4].metric("필터링 객단가", f"₩{int(c_filtered_aov):,}", get_delta(c_filtered_aov, p_filtered_aov), 
                          help="EASY REPAIR만 구매한 주문 제외")
        else:
            cols[4].metric("필터링 객단가", "데이터 없음", help="EASY REPAIR만 구매한 주문 제외")

        # [대량 구매]
        st.markdown("---")
        st.subheader("📦 대량 구매 세그먼트 (150만 원↑)")
        b1, b2, b3 = st.columns(3)
        b1.metric("대량 주문 건수", f"{int(curr['bulk_orders'])}건", f"{int(curr['bulk_orders'] - prev['bulk_orders']):+}건")
        b2.metric("대량 구매 매출", f"₩{int(curr['bulk_revenue']):,}", get_delta(curr['bulk_revenue'], prev['bulk_revenue']))
        b3.metric("대량 매출 비중", f"{(curr['bulk_revenue']/curr['revenue']*100 if curr['revenue']>0 else 0):.1f}%")

        # [개선된 매출 추이 차트]
        st.markdown("---")
        st.subheader(f"📊 {time_unit} 매출 추이")
        
        if ts_df is not None and not ts_df.empty:
            ts_df['conversion_rate'] = (ts_df['orders'] / ts_df['sessions'] * 100).fillna(0)
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # 세션 수 (선 그래프)
            fig.add_trace(
                go.Scatter(
                    x=ts_df['period_label'], 
                    y=ts_df['sessions'], 
                    name="세션 수",
                    line=dict(color='#4A90E2', width=3),
                    mode='lines+markers',
                    marker=dict(size=8)
                ),
                secondary_y=False
            )
            
            # 매출액 (막대 그래프)
            fig.add_trace(
                go.Bar(
                    x=ts_df['period_label'], 
                    y=ts_df['revenue'], 
                    name="매출액",
                    marker_color='#50C878',
                    opacity=0.7,
                    text=ts_df['revenue'].apply(lambda x: f'₩{x/1000000:.1f}M'),
                    textposition='outside'
                ),
                secondary_y=True
            )
            
            # 전환율 (점선)
            fig.add_trace(
                go.Scatter(
                    x=ts_df['period_label'], 
                    y=ts_df['conversion_rate'], 
                    name="구매 전환율",
                    line=dict(color='#FF6B6B', width=2, dash='dash'),
                    mode='lines+markers',
                    marker=dict(size=6)
                ),
                secondary_y=False
            )
            
            fig.update_xaxes(title_text="기간", showgrid=True, gridwidth=1, gridcolor='#E8E8E8')
            fig.update_yaxes(title_text="<b>세션 수 / 전환율 (%)</b>", secondary_y=False, showgrid=True, gridwidth=1, gridcolor='#E8E8E8')
            fig.update_yaxes(title_text="<b>매출액 (원)</b>", secondary_y=True)
            
            fig.update_layout(
                template="plotly_white",
                hovermode="x unified",
                font=dict(size=13, family="Pretendard, -apple-system, sans-serif"),
                legend=dict(
                    orientation="h", 
                    yanchor="bottom", 
                    y=1.02, 
                    xanchor="center", 
                    x=0.5,
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="#CCCCCC",
                    borderwidth=1
                ),
                plot_bgcolor='#FAFAFA',
                height=450
            )
            
            st.plotly_chart(fig, use_container_width=True)

        # [데이터 인사이트]
        st.markdown("---")
        st.subheader("🧠 데이터 기반 인사이트")
        
        with st.spinner("분석 중..."):
            insight_data = get_insight_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1])
            insights = generate_insights(curr, prev, insight_data)
            st.markdown(insights)
            
            # [개선된 상세 데이터 테이블]
            with st.expander("📋 상세 분석 데이터 보기"):
                if insight_data:
                    tab1, tab2, tab3, tab4, tab5 = st.tabs([
                        "제품별 분석", 
                        "채널별 분석",
                        "인구통계별 분석",
                        "지역별 분석", 
                        "디바이스별 분석"
                    ])
                    
                    # 숫자 포맷 함수
                    def format_currency(val):
                        return f"₩{val:,.0f}"
                    
                    def format_number(val):
                        return f"{val:,.0f}"
                    
                    def format_percent(val):
                        return f"{val:+.1f}%" if pd.notna(val) else "-"
                    
                    with tab1:
                        df = insight_data['product'].copy()
                        df['현재매출'] = df['현재매출'].apply(format_currency)
                        df['이전매출'] = df['이전매출'].apply(format_currency)
                        df['매출변화'] = df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                        df['증감율'] = df['증감율'].apply(format_percent)
                        st.dataframe(df, use_container_width=True, height=400)
                    
                    with tab2:
                        # 채널 매출 데이터
                        df_rev = insight_data['channel_revenue'].copy()
                        df_rev = df_rev.rename(columns={'채널': '채널명'})
                        
                        # 채널 세션 데이터
                        df_ses = insight_data['channel_sessions'].copy()
                        df_ses = df_ses.rename(columns={'채널': '채널명'})
                        
                        # 두 데이터 합치기
                        df = pd.merge(df_rev, df_ses, on='채널명', how='outer', suffixes=('_매출', '_세션'))
                        
                        # 컬럼 순서 재정렬
                        df = df[['채널명', '현재매출', '이전매출', '매출변화', '증감율_매출', '현재세션', '이전세션', '세션변화', '증감율_세션']]
                        df = df.rename(columns={'증감율_매출': '매출증감율', '증감율_세션': '세션증감율'})
                        
                        # 포맷 적용
                        df['현재매출'] = df['현재매출'].apply(format_currency)
                        df['이전매출'] = df['이전매출'].apply(format_currency)
                        df['매출변화'] = df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                        df['매출증감율'] = df['매출증감율'].apply(format_percent)
                        df['현재세션'] = df['현재세션'].apply(format_number)
                        df['이전세션'] = df['이전세션'].apply(format_number)
                        df['세션변화'] = df['세션변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_number(abs(x))}")
                        df['세션증감율'] = df['세션증감율'].apply(format_percent)
                        
                        st.dataframe(df, use_container_width=True, height=400)
                    
                    with tab3:
                        # 인구통계 매출 데이터
                        df_rev = insight_data['demographics_revenue'].copy()
                        df_rev = df_rev.rename(columns={'인구통계': '인구통계'})
                        
                        # 인구통계 세션 데이터
                        df_ses = insight_data['demographics_sessions'].copy()
                        df_ses = df_ses.rename(columns={'인구통계': '인구통계'})
                        
                        # 두 데이터 합치기
                        df = pd.merge(df_rev, df_ses, on='인구통계', how='outer', suffixes=('_매출', '_세션'))
                        
                        # 컬럼 순서 재정렬
                        df = df[['인구통계', '현재매출', '이전매출', '매출변화', '증감율_매출', '현재세션', '이전세션', '세션변화', '증감율_세션']]
                        df = df.rename(columns={'증감율_매출': '매출증감율', '증감율_세션': '세션증감율'})
                        
                        # 포맷 적용
                        df['현재매출'] = df['현재매출'].apply(format_currency)
                        df['이전매출'] = df['이전매출'].apply(format_currency)
                        df['매출변화'] = df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                        df['매출증감율'] = df['매출증감율'].apply(format_percent)
                        df['현재세션'] = df['현재세션'].apply(format_number)
                        df['이전세션'] = df['이전세션'].apply(format_number)
                        df['세션변화'] = df['세션변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_number(abs(x))}")
                        df['세션증감율'] = df['세션증감율'].apply(format_percent)
                        
                        st.dataframe(df, use_container_width=True, height=400)
                    
                    with tab4:
                        df = insight_data['demo'].copy()
                        df['현재매출'] = df['현재매출'].apply(format_currency)
                        df['이전매출'] = df['이전매출'].apply(format_currency)
                        df['매출변화'] = df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                        df['증감율'] = df['증감율'].apply(format_percent)
                        st.dataframe(df, use_container_width=True, height=400)
                    
                    with tab5:
                        df = insight_data['device'].copy()
                        df['현재매출'] = df['현재매출'].apply(format_currency)
                        df['이전매출'] = df['이전매출'].apply(format_currency)
                        df['매출변화'] = df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                        df['증감율'] = df['증감율'].apply(format_percent)
                        st.dataframe(df, use_container_width=True, height=400)

else:
    st.info("💡 사이드바에서 기간을 선택해주세요.")
