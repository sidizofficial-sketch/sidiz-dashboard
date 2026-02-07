# SIDIZ Dashboard v2.2 - NameError 해결 완료 버전
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
def get_dashboard_data(start_c, end_c, start_p, end_p, time_unit, data_source="시디즈닷컴 (매장 제외)"):
    if client is None:
        return None, None
    
    # 날짜 변수 미리 변환 (f-string 충돌 방지)
    s_c = start_c.strftime('%Y%m%d')
    e_c = end_c.strftime('%Y%m%d')
    s_p = start_p.strftime('%Y%m%d')
    e_p = end_p.strftime('%Y%m%d')
    
    min_date = min(s_c, s_p)
    max_date = max(e_c, e_p)

    if time_unit == "일별":
        group_sql = "PARSE_DATE('%Y%m%d', event_date)"
    elif time_unit == "주별":
        group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK)"
    else:
        group_sql = "DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)"

    # 핵심 지표 쿼리 (.format() 방식으로 안전하게 변수 치환)
    if data_source == "시디즈닷컴 (매장 제외)":
        # 매장 데이터 제외 모드
        query = """
    WITH store_sessions AS (
        -- 매장 유입 세션 블랙리스트: 11개 매장 QR 코드
        SELECT DISTINCT 
            CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING)) as session_key
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
        AND (
            -- traffic_source.source
            LOWER(COALESCE(traffic_source.source, '')) IN (
                'store_register_qr',
                'qr_store_',
                'qr_store_247482',
                'qr_store_247483',
                'qr_store_247488',
                'qr_store_247476',
                'qr_store_247474',
                'qr_store_247486',
                'qr_store_247489',
                'qr_store_252941',
                'qr_store_247475'
            ) OR
            -- event_params의 source
            LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source' LIMIT 1), '')) IN (
                'store_register_qr',
                'qr_store_',
                'qr_store_247482',
                'qr_store_247483',
                'qr_store_247488',
                'qr_store_247476',
                'qr_store_247474',
                'qr_store_247486',
                'qr_store_247489',
                'qr_store_252941',
                'qr_store_247475'
            ) OR
            -- collected_traffic_source.manual_source
            LOWER(COALESCE(collected_traffic_source.manual_source, '')) IN (
                'store_register_qr',
                'qr_store_',
                'qr_store_247482',
                'qr_store_247483',
                'qr_store_247488',
                'qr_store_247476',
                'qr_store_247474',
                'qr_store_247486',
                'qr_store_247489',
                'qr_store_252941',
                'qr_store_247475'
            )
        )
    ),
    base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id, event_name, ecommerce.purchase_revenue, ecommerce.transaction_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num,
            items
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
    ),
    filtered_base AS (
        -- 매장 세션 제외 (session_key 기반)
        SELECT b.*
        FROM base b
        WHERE CONCAT(b.user_pseudo_id, CAST(b.sid AS STRING)) NOT IN (
            SELECT session_key FROM store_sessions
        )
    ),
    easy_repair_only_orders AS (
        SELECT transaction_id
        FROM filtered_base, UNNEST(items) as item
        WHERE event_name = 'purchase'
        GROUP BY transaction_id
        HAVING LOGICAL_AND(
            REGEXP_CONTAINS(UPPER(IFNULL(item.item_category, '')), r'EASY.REPAIR') OR 
            REGEXP_CONTAINS(UPPER(IFNULL(item.item_name, '')), r'EASY.REPAIR') OR
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
        COUNTIF(event_name = 'purchase' AND transaction_id NOT IN (SELECT transaction_id FROM easy_repair_only_orders)) as filtered_orders,
        SUM(CASE WHEN event_name = 'purchase' AND transaction_id NOT IN (SELECT transaction_id FROM easy_repair_only_orders) THEN purchase_revenue ELSE 0 END) as filtered_revenue
    FROM filtered_base
    GROUP BY 1 
    HAVING type IS NOT NULL
    """.format(min_date=min_date, max_date=max_date, s_c=s_c, e_c=e_c)
    
Python
    elif data_source == "매장 전용":
        # 1. 메인 지표용 쿼리 (15,765,000원 정밀 타격)
        query = """
    WITH raw_events AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id,
            event_name,
            ecommerce.purchase_revenue,
            ecommerce.transaction_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num,
            LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source' LIMIT 1), traffic_source.source, '')) as src,
            LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium' LIMIT 1), traffic_source.medium, '')) as med
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
    ),
    filtered_events AS (
        SELECT *, CONCAT(user_pseudo_id, CAST(sid AS STRING)) as session_key
        FROM raw_events
        WHERE src IN ('store_register_qr', 'qr_store_', 'qr_store_247482', 'qr_store_247483', 'qr_store_247488', 'qr_store_247476', 'qr_store_247474', 'qr_store_247486', 'qr_store_247489', 'qr_store_252941', 'qr_store_247475')
          AND med IN ('qr_code', 'qr_coupon', 'qr_product')
    )
    SELECT 
        CASE WHEN date BETWEEN PARSE_DATE('%Y%m%d', '{s_c}') AND PARSE_DATE('%Y%m%d', '{e_c}') THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT session_key) as sessions,
        COUNTIF(event_name = 'sign_up') as signups,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(IFNULL(purchase_revenue, 0)) as revenue,
        COUNTIF(event_name = 'purchase' AND purchase_revenue >= 1500000) as bulk_orders,
        SUM(CASE WHEN event_name = 'purchase' AND purchase_revenue >= 1500000 THEN purchase_revenue ELSE 0 END) as bulk_revenue,
        SUM(IFNULL(purchase_revenue, 0)) as filtered_revenue
    FROM filtered_events
    GROUP BY 1 HAVING type IS NOT NULL
    """.format(min_date=min_date, max_date=max_date, s_c=s_c, e_c=e_c)

        # 2. 시계열 그래프용 쿼리 (NameError 해결)
        ts_query = """
    WITH raw_ts AS (
        SELECT 
            {group_sql} as period_date,
            user_pseudo_id,
            event_name,
            ecommerce.purchase_revenue,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source' LIMIT 1), traffic_source.source, '')) as src,
            LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium' LIMIT 1), traffic_source.medium, '')) as med
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
    )
    SELECT 
        CAST(period_date AS STRING) as period_label,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions,
        SUM(IFNULL(purchase_revenue, 0)) as revenue,
        COUNTIF(event_name = 'purchase') as orders
    FROM raw_ts
    WHERE src IN ('store_register_qr', 'qr_store_', 'qr_store_247482', 'qr_store_247483', 'qr_store_247488', 'qr_store_247476', 'qr_store_247474', 'qr_store_247486', 'qr_store_247489', 'qr_store_252941', 'qr_store_247475')
      AND med IN ('qr_code', 'qr_coupon', 'qr_product')
    GROUP BY 1 ORDER BY 1
    """.format(s_c=s_c, e_c=e_c, group_sql=group_sql)
    
    else:
        # 전체 데이터 모드
        query = """
    WITH base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id, event_name, ecommerce.purchase_revenue, ecommerce.transaction_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num,
            items
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
    ),
    easy_repair_only_orders AS (
        SELECT transaction_id
        FROM base, UNNEST(items) as item
        WHERE event_name = 'purchase'
        GROUP BY transaction_id
        HAVING LOGICAL_AND(
            REGEXP_CONTAINS(UPPER(IFNULL(item.item_category, '')), r'EASY.REPAIR') OR 
            REGEXP_CONTAINS(UPPER(IFNULL(item.item_name, '')), r'EASY.REPAIR') OR
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
        COUNTIF(event_name = 'purchase' AND transaction_id NOT IN (SELECT transaction_id FROM easy_repair_only_orders)) as filtered_orders,
        SUM(CASE WHEN event_name = 'purchase' AND transaction_id NOT IN (SELECT transaction_id FROM easy_repair_only_orders) THEN purchase_revenue ELSE 0 END) as filtered_revenue
    FROM base
    GROUP BY 1 
    HAVING type IS NOT NULL
    """.format(min_date=min_date, max_date=max_date, s_c=s_c, e_c=e_c)

    # 시계열 데이터
    if data_source == "시디즈닷컴 (매장 제외)":
        ts_query = """
        WITH store_sessions AS (
            -- 매장 유입 세션 블랙리스트: store 포함 모든 소스
            SELECT DISTINCT 
                CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING)) as session_key
            FROM `sidiz-458301.analytics_487246344.events_*`
            WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
            AND (
                -- traffic_source에서 'store' 포함
                LOWER(COALESCE(traffic_source.source, '')) LIKE '%store%' OR
                LOWER(COALESCE(traffic_source.medium, '')) LIKE '%store%' OR
                -- event_params에서 'store' 포함
                LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source' LIMIT 1), '')) LIKE '%store%' OR
                LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium' LIMIT 1), '')) LIKE '%store%' OR
                -- collected_traffic_source에서 'store' 포함
                LOWER(COALESCE(collected_traffic_source.manual_source, '')) LIKE '%store%' OR
                LOWER(COALESCE(collected_traffic_source.manual_medium, '')) LIKE '%store%'
            )
        ),
        events_base AS (
            SELECT 
                {group_sql} as period_date,
                user_pseudo_id,
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
                event_name,
                ecommerce.purchase_revenue
            FROM `sidiz-458301.analytics_487246344.events_*`
            WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}'
        )
        SELECT 
            CAST(period_date AS STRING) as period_label,
            COUNT(DISTINCT CONCAT(e.user_pseudo_id, CAST(e.sid AS STRING))) as sessions,
            SUM(IFNULL(e.purchase_revenue, 0)) as revenue,
            COUNTIF(e.event_name = 'purchase') as orders
        FROM events_base e
        WHERE CONCAT(e.user_pseudo_id, CAST(e.sid AS STRING)) NOT IN (
            SELECT session_key FROM store_sessions
        )
        GROUP BY 1 ORDER BY 1
        """.format(s_c=s_c, e_c=e_c, group_sql=group_sql)
    
    elif data_source == "매장 전용":
        query = """
    WITH raw_events AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id,
            event_name,
            ecommerce.purchase_revenue,
            ecommerce.transaction_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num,
            -- 소스와 매체를 추출 (LOWER로 통일)
            LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source' LIMIT 1), traffic_source.source, '')) as src,
            LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium' LIMIT 1), traffic_source.medium, '')) as med
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
    ),
    filtered_events AS (
        SELECT *,
            CONCAT(user_pseudo_id, CAST(sid AS STRING)) as session_key
        FROM raw_events
        WHERE 
            -- 1. 소스 11개 고정
            src IN (
                'store_register_qr', 'qr_store_', 'qr_store_247482', 'qr_store_247483', 
                'qr_store_247488', 'qr_store_247476', 'qr_store_247474', 'qr_store_247486', 
                'qr_store_247489', 'qr_store_252941', 'qr_store_247475'
            )
            -- 2. 매체 3개 고정 (이미지에서 확인된 값)
            AND med IN ('qr_code', 'qr_coupon', 'qr_product')
    )
    SELECT 
        CASE WHEN date BETWEEN PARSE_DATE('%Y%m%d', '{s_c}') AND PARSE_DATE('%Y%m%d', '{e_c}') THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT session_key) as sessions,
        COUNTIF(event_name = 'sign_up') as signups,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(IFNULL(purchase_revenue, 0)) as revenue,
        -- 대량구매 및 필터링 매출 집계
        COUNTIF(event_name = 'purchase' AND purchase_revenue >= 1500000) as bulk_orders,
        SUM(CASE WHEN event_name = 'purchase' AND purchase_revenue >= 1500000 THEN purchase_revenue ELSE 0 END) as bulk_revenue,
        SUM(IFNULL(purchase_revenue, 0)) as filtered_revenue
    FROM filtered_events
    GROUP BY 1 
    HAVING type IS NOT NULL
    """.format(min_date=min_date, max_date=max_date, s_c=s_c, e_c=e_c)
    
    else:
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
def get_insight_data(start_c, end_c, start_p, end_p, data_source="시디즈닷컴 (매장 제외)"):
    if client is None:
        return None
    
    # 날짜 변수 미리 변환 (f-string 충돌 방지)
    s_c = start_c.strftime('%Y%m%d')
    e_c = end_c.strftime('%Y%m%d')
    s_p = start_p.strftime('%Y%m%d')
    e_p = end_p.strftime('%Y%m%d')
    
    min_date = min(s_c, s_p)
    max_date = max(e_c, e_p)

    # 제품별 매출 변화 (item_id 기준)
    if data_source == "시디즈닷컴 (매장 제외)":
        product_query = """
        WITH store_sessions AS (
        -- 매장 유입 세션 블랙리스트: store 포함 모든 소스
        SELECT DISTINCT 
            CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING)) as session_key
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
        AND (
            -- traffic_source에서 'store' 포함
            LOWER(COALESCE(traffic_source.source, '')) LIKE '%store%' OR
            LOWER(COALESCE(traffic_source.medium, '')) LIKE '%store%' OR
            -- event_params에서 'store' 포함
            LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source' LIMIT 1), '')) LIKE '%store%' OR
            LOWER(COALESCE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium' LIMIT 1), '')) LIKE '%store%' OR
            -- collected_traffic_source에서 'store' 포함
            LOWER(COALESCE(collected_traffic_source.manual_source, '')) LIKE '%store%' OR
            LOWER(COALESCE(collected_traffic_source.manual_medium, '')) LIKE '%store%'
        )
    ),
    base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id,
            event_name,
            ecommerce.purchase_revenue,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            items
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
    ),
    filtered_base AS (
        -- 매장 세션 제외 (session_key 기반)
        SELECT b.*
        FROM base b
        WHERE CONCAT(b.user_pseudo_id, CAST(b.sid AS STRING)) NOT IN (
            SELECT session_key FROM store_sessions
        )
    ),
        """
    else:
        product_query = """
        WITH base AS (
            SELECT 
                PARSE_DATE('%Y%m%d', event_date) as date,
                user_pseudo_id,
                event_name,
                ecommerce.purchase_revenue,
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
                items
            FROM `sidiz-458301.analytics_487246344.events_*`
            WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
        ),
        """
    
    product_query += """
    product_items AS (
        SELECT 
            date,
            user_pseudo_id,
            event_name,
            sid,
            -- item_id 기준 (없으면 정규화된 이름)
            COALESCE(
                item.item_id,
                REGEXP_REPLACE(
                    UPPER(TRIM(REGEXP_REPLACE(item.item_name, r'\\[.*?\\]', ''))),
                    r'\\s+|[^A-Z0-9가-힣]', ''
                )
            ) as match_key,
            item.item_name as original_name,
            item.price,
            item.quantity
        FROM """ + ("filtered_base" if exclude_store else "base") + """, UNNEST(items) as item
        WHERE item.item_name IS NOT NULL
    ),
    latest_product_names AS (
        SELECT 
            match_key,
            ARRAY_AGG(original_name ORDER BY date DESC LIMIT 1)[OFFSET(0)] as product_name
        FROM product_items
        GROUP BY match_key
    ),
    product_metrics AS (
        SELECT 
            match_key,
            -- 현재 기간 매출 (핵심 성과 요약과 동일)
            SUM(CASE 
                WHEN date BETWEEN PARSE_DATE('%Y%m%d', '{s_c}') AND PARSE_DATE('%Y%m%d', '{e_c}')
                AND event_name = 'purchase'
                THEN COALESCE(price, 0) * COALESCE(quantity, 0)
                ELSE 0
            END) as curr_rev,
            
            -- 이전 기간 매출
            SUM(CASE 
                WHEN date BETWEEN PARSE_DATE('%Y%m%d', '{s_p}') AND PARSE_DATE('%Y%m%d', '{e_p}')
                AND event_name = 'purchase'
                THEN COALESCE(price, 0) * COALESCE(quantity, 0)
                ELSE 0
            END) as prev_rev,
            
            -- 세션 (핵심 성과 요약과 동일)
            COUNT(DISTINCT CASE 
                WHEN date BETWEEN PARSE_DATE('%Y%m%d', '{s_c}') AND PARSE_DATE('%Y%m%d', '{e_c}')
                THEN CONCAT(user_pseudo_id, CAST(sid AS STRING))
            END) as curr_sess,
            
            COUNT(DISTINCT CASE 
                WHEN date BETWEEN PARSE_DATE('%Y%m%d', '{s_p}') AND PARSE_DATE('%Y%m%d', '{e_p}')
                THEN CONCAT(user_pseudo_id, CAST(sid AS STRING))
            END) as prev_sess,
            
            -- 수량
            SUM(CASE 
                WHEN date BETWEEN PARSE_DATE('%Y%m%d', '{s_c}') AND PARSE_DATE('%Y%m%d', '{e_c}')
                AND event_name = 'purchase'
                THEN COALESCE(quantity, 0)
                ELSE 0
            END) as curr_qty,
            
            SUM(CASE 
                WHEN date BETWEEN PARSE_DATE('%Y%m%d', '{s_p}') AND PARSE_DATE('%Y%m%d', '{e_p}')
                AND event_name = 'purchase'
                THEN COALESCE(quantity, 0)
                ELSE 0
            END) as prev_qty
        FROM product_items
        GROUP BY match_key
    )
    SELECT 
        n.product_name,
        m.curr_rev as current_revenue,
        m.prev_rev as previous_revenue,
        m.curr_rev - m.prev_rev as revenue_change,
        ROUND(SAFE_DIVIDE((m.curr_rev - m.prev_rev) * 100, NULLIF(m.prev_rev, 0)), 1) as change_pct,
        m.curr_sess as current_sessions,
        m.prev_sess as previous_sessions,
        m.curr_qty as current_quantity,
        m.prev_qty as previous_quantity
    FROM product_metrics m
    JOIN latest_product_names n ON m.match_key = n.match_key
    WHERE m.curr_rev > 0 OR m.prev_rev > 0
    ORDER BY m.curr_rev DESC
    LIMIT 20
    """.format(min_date=min_date, max_date=max_date, s_c=s_c, e_c=e_c, s_p=s_p, e_p=e_p)

    # 채널별 매출 & 세션 변화 (통합 쿼리 - 단일 소스)
    channel_combined_query = """
    WITH base_events AS (
        SELECT 
            user_pseudo_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as session_id,
            event_name,
            ecommerce.purchase_revenue,
            -- 이벤트 파라미터에서만 소스/매체 추출 (traffic_source 사용 중단)
            LOWER(NULLIF(TRIM((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source' LIMIT 1)), '')) as raw_source,
            LOWER(NULLIF(TRIM((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium' LIMIT 1)), '')) as raw_medium,
            event_timestamp,
            _TABLE_SUFFIX as suffix
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
        
    ),
    session_mapping AS (
        SELECT 
            user_pseudo_id,
            session_id,
            suffix,
            event_name,
            purchase_revenue,
            -- 세션 내에서 NULL이 아닌 첫 번째 소스 값을 찾아 전파 (IGNORE NULLS)
            COALESCE(
                FIRST_VALUE(raw_source IGNORE NULLS) OVER (
                    PARTITION BY user_pseudo_id, session_id 
                    ORDER BY event_timestamp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ),
                '(direct)'
            ) as final_source,
            COALESCE(
                FIRST_VALUE(raw_medium IGNORE NULLS) OVER (
                    PARTITION BY user_pseudo_id, session_id 
                    ORDER BY event_timestamp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ),
                '(none)'
            ) as final_medium
        FROM base_events
    ),
    events_with_channel AS (
        SELECT 
            suffix,
            CONCAT(final_source, ' / ', final_medium) as channel,
            CONCAT(user_pseudo_id, '-', CAST(session_id AS STRING)) as unique_session,
            event_name,
            purchase_revenue
        FROM session_mapping
    ),
    aggregated AS (
        SELECT 
            channel,
            -- 현재 기간 매출
            SUM(CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' AND event_name = 'purchase' THEN COALESCE(purchase_revenue, 0) ELSE 0 END) as current_revenue,
            -- 이전 기간 매출
            SUM(CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' AND event_name = 'purchase' THEN COALESCE(purchase_revenue, 0) ELSE 0 END) as previous_revenue,
            -- 현재 기간 세션
            COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' THEN unique_session END) as current_sessions,
            -- 이전 기간 세션
            COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN unique_session END) as previous_sessions
        FROM events_with_channel
        GROUP BY 1
    )
    SELECT 
        channel as channel_name,
        COALESCE(current_revenue, 0) as current_revenue,
        COALESCE(previous_revenue, 0) as previous_revenue,
        COALESCE(current_revenue, 0) - COALESCE(previous_revenue, 0) as revenue_change,
        ROUND(SAFE_DIVIDE((COALESCE(current_revenue, 0) - COALESCE(previous_revenue, 0)) * 100, NULLIF(COALESCE(previous_revenue, 0), 0)), 1) as revenue_change_pct,
        COALESCE(current_sessions, 0) as current_sessions,
        COALESCE(previous_sessions, 0) as previous_sessions,
        COALESCE(current_sessions, 0) - COALESCE(previous_sessions, 0) as sessions_change,
        ROUND(SAFE_DIVIDE((COALESCE(current_sessions, 0) - COALESCE(previous_sessions, 0)) * 100, NULLIF(COALESCE(previous_sessions, 0), 0)), 1) as sessions_change_pct
    FROM aggregated
    ORDER BY COALESCE(current_revenue, 0) DESC
    LIMIT 20
    """
    # 지역별 변화
    demo_query = """
    WITH current_demo AS (
        SELECT CONCAT(IFNULL(geo.country, 'Unknown'), ' / ', IFNULL(geo.city, 'Unknown')) as location, SUM(ecommerce.purchase_revenue) as revenue 
        FROM `sidiz-458301.analytics_487246344.events_*` 
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}' AND event_name = 'purchase' 
        GROUP BY 1
    ),
    previous_demo AS (
        SELECT CONCAT(IFNULL(geo.country, 'Unknown'), ' / ', IFNULL(geo.city, 'Unknown')) as location, SUM(ecommerce.purchase_revenue) as revenue 
        FROM `sidiz-458301.analytics_487246344.events_*` 
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}' AND event_name = 'purchase'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.location, p.location), 
        IFNULL(c.revenue, 0), 
        IFNULL(p.revenue, 0), 
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0), 
        ROUND(SAFE_DIVIDE((IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) * 100, IFNULL(p.revenue, 0)), 1)
    FROM current_demo c 
    FULL OUTER JOIN previous_demo p ON c.location = p.location 
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC 
    LIMIT 10
    """.format(s_c=s_c, e_c=e_c, s_p=s_p, e_p=e_p)

    # 디바이스별 변화
    device_query = """
    WITH current_device AS (
        SELECT device.category as device, SUM(ecommerce.purchase_revenue) as revenue 
        FROM `sidiz-458301.analytics_487246344.events_*` 
        WHERE _TABLE_SUFFIX BETWEEN '{s_c}' AND '{e_c}' AND event_name = 'purchase' 
        GROUP BY 1
    ),
    previous_device AS (
        SELECT device.category as device, SUM(ecommerce.purchase_revenue) as revenue 
        FROM `sidiz-458301.analytics_487246344.events_*` 
        WHERE _TABLE_SUFFIX BETWEEN '{s_p}' AND '{e_p}' AND event_name = 'purchase'
        GROUP BY 1
    )
    SELECT 
        COALESCE(c.device, p.device), 
        IFNULL(c.revenue, 0), 
        IFNULL(p.revenue, 0), 
        IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0), 
        ROUND(SAFE_DIVIDE((IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) * 100, IFNULL(p.revenue, 0)), 1)
    FROM current_device c 
    FULL OUTER JOIN previous_device p ON c.device = p.device 
    ORDER BY ABS(IFNULL(c.revenue, 0) - IFNULL(p.revenue, 0)) DESC
    """.format(s_c=s_c, e_c=e_c, s_p=s_p, e_p=e_p)

    # 인구통계별 매출 & 세션 변화 (user_properties 포함 + 필터 제거)
    demographics_combined_query = """
    WITH base_events AS (
        SELECT 
            _TABLE_SUFFIX as suffix,
            user_pseudo_id,
            event_name,
            ecommerce.purchase_revenue,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as session_id,
            -- event_params에서 성별 추출 (여러 키 시도)
            COALESCE(
                LOWER((SELECT value.string_value FROM UNNEST(event_params) WHERE key IN ('u_gender', 'gender', 'sex', 'user_gender') LIMIT 1)),
                LOWER((SELECT value.string_value FROM UNNEST(user_properties) WHERE key IN ('u_gender', 'gender', 'sex', 'user_gender') LIMIT 1)),
                ''
            ) as gender_raw,
            -- event_params에서 연령 추출 (여러 키 시도)
            COALESCE(
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key IN ('u_age', 'age', 'age_group', 'user_age') LIMIT 1),
                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key IN ('u_age', 'age', 'age_group', 'user_age') LIMIT 1),
                '미분류'
            ) as age_raw
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min_date}' AND '{max_date}'
        
    ),
    normalized_demographics AS (
        SELECT 
            suffix,
            user_pseudo_id,
            session_id,
            event_name,
            purchase_revenue,
            CASE 
                WHEN gender_raw IN ('male', 'm', '남성', '1') THEN '남성'
                WHEN gender_raw IN ('female', 'f', '여성', '2') THEN '여성'
                ELSE '미분류'
            END as gender_normalized,
            COALESCE(NULLIF(age_raw, ''), '미분류') as age_normalized
        FROM base_events
    ),
    aggregated AS (
        SELECT 
            CONCAT(
                COALESCE(gender_normalized, '미분류'), 
                ' / ', 
                COALESCE(age_normalized, '미분류')
            ) as demographic,
            -- 현재 기간 매출
            SUM(CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' AND event_name = 'purchase' THEN IFNULL(purchase_revenue, 0) ELSE 0 END) as current_revenue,
            -- 이전 기간 매출
            SUM(CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' AND event_name = 'purchase' THEN IFNULL(purchase_revenue, 0) ELSE 0 END) as previous_revenue,
            -- 현재 기간 세션
            COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_c}' AND '{e_c}' THEN CONCAT(user_pseudo_id, '-', CAST(session_id AS STRING)) END) as current_sessions,
            -- 이전 기간 세션
            COUNT(DISTINCT CASE WHEN suffix BETWEEN '{s_p}' AND '{e_p}' THEN CONCAT(user_pseudo_id, '-', CAST(session_id AS STRING)) END) as previous_sessions
        FROM normalized_demographics
        GROUP BY 1
    )
    SELECT 
        COALESCE(demographic, '미분류 / 미분류') as demographic,
        IFNULL(current_revenue, 0) as current_revenue,
        IFNULL(previous_revenue, 0) as previous_revenue,
        IFNULL(current_revenue - previous_revenue, 0) as revenue_change,
        ROUND(SAFE_DIVIDE((current_revenue - previous_revenue) * 100, NULLIF(previous_revenue, 0)), 1) as revenue_change_pct,
        IFNULL(current_sessions, 0) as current_sessions,
        IFNULL(previous_sessions, 0) as previous_sessions,
        IFNULL(current_sessions - previous_sessions, 0) as sessions_change,
        ROUND(SAFE_DIVIDE((current_sessions - previous_sessions) * 100, NULLIF(previous_sessions, 0)), 1) as sessions_change_pct
    FROM aggregated
    ORDER BY ABS(IFNULL(revenue_change, 0)) DESC
    LIMIT 10
    """.format(min_date=min_date, max_date=max_date, s_c=s_c, e_c=e_c, s_p=s_p, e_p=e_p)

    try:
        # 쿼리 실행
        results = {
            'product': client.query(product_query).to_dataframe(),
            'channel_combined': client.query(channel_combined_query).to_dataframe(),
            'demo': client.query(demo_query).to_dataframe(),
            'device': client.query(device_query).to_dataframe(),
            'demographics_combined': client.query(demographics_combined_query).to_dataframe()
        }
        
        # NaN을 0으로 명시적 변환
        for key in results:
            if results[key] is not None and not results[key].empty:
                numeric_cols = results[key].select_dtypes(include=['float64', 'int64']).columns
                results[key][numeric_cols] = results[key][numeric_cols].fillna(0)
        
        # 컬럼명 정확히 매칭
        results['product'].columns = ['제품명', '현재매출', '이전매출', '매출변화', '증감율', '현재세션', '이전세션', '현재수량', '이전수량']
        
        # SQL에서 이미 정규화 및 그룹화 완료 - 추가 처리만 수행
        if 'product' in results and not results['product'].empty:
            pdf = results['product']
            
            # 변화량 계산
            pdf['세션변화'] = pdf['현재세션'] - pdf['이전세션']
            pdf['수량변화'] = pdf['현재수량'] - pdf['이전수량']
            
            # 매출 비중 계산
            total_revenue = pdf['현재매출'].sum()
            pdf['매출비중'] = (pdf['현재매출'] / total_revenue * 100 if total_revenue > 0 else 0).round(1)
            
            # 매출 높은 순 정렬
            pdf = pdf.sort_values(by='현재매출', ascending=False).reset_index(drop=True)
            
            results['product'] = pdf
        
        results['channel_combined'].columns = ['채널', '현재매출', '이전매출', '매출변화', '매출증감율', '현재세션', '이전세션', '세션변화', '세션증감율']
        # 채널별 매출 높은 순 정렬
        if 'channel_combined' in results and not results['channel_combined'].empty:
            results['channel_combined'] = results['channel_combined'].sort_values(by='현재매출', ascending=False).reset_index(drop=True)
        
        results['demo'].columns = ['지역', '현재매출', '이전매출', '매출변화', '증감율']
        # 지역별 매출 높은 순 정렬
        if 'demo' in results and not results['demo'].empty:
            results['demo'] = results['demo'].sort_values(by='현재매출', ascending=False).reset_index(drop=True)
        
        results['device'].columns = ['디바이스', '현재매출', '이전매출', '매출변화', '증감율']
        # 디바이스별 매출 높은 순 정렬
        if 'device' in results and not results['device'].empty:
            results['device'] = results['device'].sort_values(by='현재매출', ascending=False).reset_index(drop=True)
        
        results['demographics_combined'].columns = ['인구통계', '현재매출', '이전매출', '매출변화', '매출증감율', '현재세션', '이전세션', '세션변화', '세션증감율']
        # 인구통계별 매출 높은 순 정렬
        if 'demographics_combined' in results and not results['demographics_combined'].empty:
            results['demographics_combined'] = results['demographics_combined'].sort_values(by='현재매출', ascending=False).reset_index(drop=True)
        
        return results
    except Exception as e:
        st.sidebar.error(f"❌ 쿼리 실행 오류: {str(e)}")
        st.error(f"⚠️ 인사이트 데이터 오류: {e}")
        import traceback
        st.sidebar.code(traceback.format_exc())
        return None

# -------------------------------------------------
# 4. 데이터 기반 인사이트 생성
# -------------------------------------------------
def generate_insights(curr, prev, insight_data):
    insights = []
    
    # insight_data 유효성 검사
    if not insight_data:
        return "📊 데이터 수집 중입니다. 잠시 후 다시 확인해주세요."
    
    # 1. 전체 매출 변동
    rev_change = curr['revenue'] - prev['revenue']
    rev_pct = (rev_change / prev['revenue'] * 100) if prev['revenue'] > 0 else 0
    
    if abs(rev_pct) > 3:
        direction = "증가" if rev_change > 0 else "감소"
        insights.append(f"### 📊 전체 매출 {direction}")
        insights.append(f"매출이 **₩{abs(rev_change):,.0f} ({abs(rev_pct):.1f}%) {direction}**했습니다.")
    
    # 2. 제품 영향 (TOP3)
    if 'product' in insight_data and insight_data['product'] is not None and not insight_data['product'].empty:
        insights.append(f"\n### 🏆 주요 제품 영향 TOP3")
        for idx, row in insight_data['product'].head(3).iterrows():
            if abs(row['매출변화']) > 500000:
                direction = "↑" if row['매출변화'] > 0 else "↓"
                insights.append(f"**{idx+1}. {row['제품명']}** {direction} ₩{abs(row['매출변화']):,.0f} ({row['증감율']:+.1f}%)")
    
    # 3. 채널 매출 영향 (TOP3)
    if 'channel_combined' in insight_data and insight_data['channel_combined'] is not None and not insight_data['channel_combined'].empty:
        insights.append(f"\n### 🎯 주요 채널 매출 영향 TOP3")
        for idx, row in insight_data['channel_combined'].head(3).iterrows():
            if abs(row['매출변화']) > 300000:
                direction = "↑" if row['매출변화'] > 0 else "↓"
                insights.append(f"**{idx+1}. {row['채널']}** {direction} ₩{abs(row['매출변화']):,.0f} ({row['매출증감율']:+.1f}%)")
    
    # 4. 채널 유입 영향 (TOP3)
    if 'channel_combined' in insight_data and insight_data['channel_combined'] is not None and not insight_data['channel_combined'].empty:
        insights.append(f"\n### 🚪 주요 채널 유입 영향 TOP3")
        # 세션 변화량 기준으로 정렬
        channel_sessions_top3 = insight_data['channel_combined'].sort_values('세션변화', ascending=False, key=abs).head(3)
        for idx, (i, row) in enumerate(channel_sessions_top3.iterrows()):
            if abs(row['세션변화']) > 100:
                direction = "↑" if row['세션변화'] > 0 else "↓"
                insights.append(f"**{idx+1}. {row['채널']}** {direction} {abs(row['세션변화']):,.0f}세션 ({row['세션증감율']:+.1f}%)")
    
    # 5. 인구통계 매출 영향 (TOP3) - 강화된 예외 처리
    if 'demographics_combined' in insight_data and insight_data['demographics_combined'] is not None and not insight_data['demographics_combined'].empty:
        try:
            # '미분류 / 미분류'가 아닌 데이터만 필터링
            demo_df = insight_data['demographics_combined']
            demo_df_filtered = demo_df[~demo_df['인구통계'].str.contains('미분류', na=False)]
            
            if not demo_df_filtered.empty and len(demo_df_filtered) > 0:
                insights.append(f"\n### 👥 인구통계 매출 영향 TOP3")
                for idx, row in demo_df_filtered.head(3).iterrows():
                    if abs(row['매출변화']) > 300000:
                        direction = "↑" if row['매출변화'] > 0 else "↓"
                        insights.append(f"**{idx+1}. {row['인구통계']}** {direction} ₩{abs(row['매출변화']):,.0f} ({row['매출증감율']:+.1f}%)")
        except Exception as e:
            pass  # 인구통계 데이터 오류 시 조용히 스킵
    
    # 6. 인구통계 유입 영향 (TOP3) - 강화된 예외 처리
    if 'demographics_combined' in insight_data and insight_data['demographics_combined'] is not None and not insight_data['demographics_combined'].empty:
        try:
            demo_df = insight_data['demographics_combined']
            demo_df_filtered = demo_df[~demo_df['인구통계'].str.contains('미분류', na=False)]
            
            if not demo_df_filtered.empty and len(demo_df_filtered) > 0:
                demo_ses_top3 = demo_df_filtered.sort_values('세션변화', ascending=False, key=abs).head(3)
                if not demo_ses_top3.empty:
                    insights.append(f"\n### 🚶 인구통계 유입 영향 TOP3")
                    for idx, (i, row) in enumerate(demo_ses_top3.iterrows()):
                        if abs(row['세션변화']) > 100:
                            direction = "↑" if row['세션변화'] > 0 else "↓"
                            insights.append(f"**{idx+1}. {row['인구통계']}** {direction} {abs(row['세션변화']):,.0f}세션 ({row['세션증감율']:+.1f}%)")
        except Exception as e:
            pass  # 인구통계 데이터 오류 시 조용히 스킵
    
    # 7. 대량 구매 영향
    bulk_change = curr['bulk_revenue'] - prev['bulk_revenue']
    bulk_pct = (bulk_change / prev['bulk_revenue'] * 100) if prev['bulk_revenue'] > 0 else 0
    
    if abs(bulk_pct) > 10 or abs(bulk_change) > 5000000:
        direction = "증가" if bulk_change > 0 else "감소"
        insights.append(f"\n### 💼 대량 구매 영향")
        insights.append(f"대량 구매(150만원↑) 매출이 **₩{abs(bulk_change):,.0f} ({abs(bulk_pct):.1f}%) {direction}**했습니다.")
    
    # 8. 지역 변화
    if 'demo' in insight_data and insight_data['demo'] is not None and not insight_data['demo'].empty:
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

today = datetime.now().date()

with st.sidebar:
    st.header("⚙️ 분석 설정")
    
    # 데이터 소스 선택 (3가지 옵션)
    data_source = st.selectbox(
        "📊 데이터 소스",
        options=["시디즈닷컴 (매장 제외)", "전체", "매장 전용"],
        index=0,  # 기본값: 시디즈닷컴 (매장 제외)
        help="시디즈닷컴: 온라인 전용 | 전체: 모든 데이터 | 매장 전용: 매장 QR 유입만"
    )
    
    # 날짜 입력
    curr_date = st.date_input("분석 기간", [today - timedelta(days=7), today - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [today - timedelta(days=14), today - timedelta(days=8)])
    
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_date) == 2 and len(comp_date) == 2:
    # 데이터 소스 상태 표시
    if data_source == "시디즈닷컴 (매장 제외)":
        st.info("🌐 시디즈닷컴 모드 - 온라인 전용 데이터만 표시됩니다")
    elif data_source == "매장 전용":
        st.info("🏪 매장 전용 모드 - 매장 QR 유입 데이터만 표시됩니다")
    
    summary_df, ts_df = get_dashboard_data(
        curr_date[0], curr_date[1], 
        comp_date[0], comp_date[1], 
        time_unit, 
        data_source  # exclude_store 대신 data_source 전달
    )
    
    if summary_df is not None and not summary_df.empty:
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else curr

        def get_delta(c, p):
            if p == 0:
                return "0%"
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
        
        # 대량 구매 상세 품목 (접기/펼치기)
        with st.expander("🔍 대량 구매 품목별 상세 보기"):
            bulk_detail_query = f"""
            SELECT 
                item.item_name as product_name,
                COUNT(DISTINCT event_timestamp) as order_count,
                SUM(item.quantity) as total_quantity,
                SUM(item.price * item.quantity) as item_revenue
            FROM `sidiz-458301.analytics_487246344.events_*`,
            UNNEST(items) as item
            WHERE _TABLE_SUFFIX BETWEEN '{curr_date[0].strftime('%Y%m%d')}' AND '{curr_date[1].strftime('%Y%m%d')}'
            AND event_name = 'purchase'
            AND ecommerce.purchase_revenue >= 1500000
            GROUP BY item.item_name
            ORDER BY item_revenue DESC
            LIMIT 20
            """
            try:
                bulk_detail = client.query(bulk_detail_query).to_dataframe()
                if not bulk_detail.empty:
                    bulk_detail.columns = ['제품명', '주문수', '수량', '매출액']
                    bulk_detail['매출비중'] = (bulk_detail['매출액'] / bulk_detail['매출액'].sum() * 100).round(1)
                    
                    # 포맷팅
                    display_bulk = bulk_detail.copy()
                    display_bulk.insert(0, '순위', range(1, len(display_bulk) + 1))
                    display_bulk['주문수'] = display_bulk['주문수'].apply(lambda x: f"{int(x)}건")
                    display_bulk['수량'] = display_bulk['수량'].apply(lambda x: f"{int(x)}개")
                    display_bulk['매출액'] = display_bulk['매출액'].apply(lambda x: f"₩{int(x):,}")
                    display_bulk['매출비중'] = display_bulk['매출비중'].apply(lambda x: f"{x:.1f}%")
                    
                    st.dataframe(display_bulk, use_container_width=True, height=400)
                else:
                    st.info("대량 구매 품목 데이터가 없습니다.")
            except Exception as e:
                st.error(f"대량 구매 상세 조회 오류: {e}")


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
            insight_data = get_insight_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], data_source)
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
                    
                    # 숫자 포맷 함수 (안전한 예외 처리 포함)
                    def format_currency(val):
                        try:
                            if pd.isna(val) or val == 0:
                                return "₩0"
                            return f"₩{val:,.0f}"
                        except:
                            return "₩0"
                    
                    def format_number(val):
                        try:
                            if pd.isna(val) or val == 0:
                                return "0"
                            return f"{val:,.0f}"
                        except:
                            return "0"
                    
                    def format_percent(val):
                        try:
                            if pd.isna(val):
                                return "-"
                            return f"{val:+.1f}%"
                        except:
                            return "-"
                    
                    with tab1:
                        if 'product' in insight_data and not insight_data['product'].empty:
                            # 가공을 위한 복사본 생성
                            display_df = insight_data['product'].copy()
                            
                            # 순위 추가 (1부터 시작)
                            display_df.insert(0, '순위', range(1, len(display_df) + 1))
                            
                            # 표시용 포맷팅 (순서 중요: 계산이 모두 끝난 후 문자열로 변환)
                            display_df['현재매출'] = display_df['현재매출'].apply(format_currency)
                            display_df['이전매출'] = display_df['이전매출'].apply(format_currency)
                            display_df['매출변화'] = display_df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                            display_df['증감율'] = display_df['증감율'].apply(lambda x: f"{x:+.1f}%")
                            display_df['매출비중'] = display_df['매출비중'].apply(lambda x: f"{x:.1f}%")
                            display_df['현재세션'] = display_df['현재세션'].apply(format_number)
                            display_df['이전세션'] = display_df['이전세션'].apply(format_number)
                            display_df['세션변화'] = display_df['세션변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_number(abs(x))}")
                            display_df['현재수량'] = display_df['현재수량'].apply(lambda x: f"{int(x)}개")
                            display_df['이전수량'] = display_df['이전수량'].apply(lambda x: f"{int(x)}개")
                            display_df['수량변화'] = display_df['수량변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {int(abs(x))}개")
                            
                            # 컬럼 선택 및 순서
                            cols_to_show = ['순위', '제품명', '현재매출', '매출비중', '이전매출', '매출변화', '증감율', 
                                          '현재세션', '이전세션', '세션변화', '현재수량', '이전수량', '수량변화']
                            st.dataframe(display_df[cols_to_show], use_container_width=True, height=600)
                        else:
                            st.info("데이터가 없습니다.")
                    
                    with tab2:
                        if 'channel_combined' in insight_data and not insight_data['channel_combined'].empty:
                            df = insight_data['channel_combined'].copy()
                            
                            # 매출 비중 계산
                            total_revenue = df['현재매출'].sum()
                            if total_revenue > 0:
                                df['매출비중'] = (df['현재매출'] / total_revenue * 100).round(1)
                            else:
                                df['매출비중'] = 0
                            
                            # 순위 추가 (1부터 시작)
                            df.insert(0, '순위', range(1, len(df) + 1))
                            
                            # 포맷 적용
                            df['현재매출'] = df['현재매출'].apply(format_currency)
                            df['이전매출'] = df['이전매출'].apply(format_currency)
                            df['매출변화'] = df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                            df['매출증감율'] = df['매출증감율'].apply(format_percent)
                            df['매출비중'] = df['매출비중'].apply(lambda x: f"{x:.1f}%")
                            df['현재세션'] = df['현재세션'].apply(format_number)
                            df['이전세션'] = df['이전세션'].apply(format_number)
                            df['세션변화'] = df['세션변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_number(abs(x))}")
                            df['세션증감율'] = df['세션증감율'].apply(format_percent)
                            
                            cols_to_show = ['순위', '채널', '현재매출', '매출비중', '이전매출', '매출변화', '매출증감율',
                                          '현재세션', '이전세션', '세션변화', '세션증감율']
                            st.dataframe(df[cols_to_show], use_container_width=True, height=600)
                        else:
                            st.info("데이터가 없습니다.")
                    
                    with tab3:
                        if 'demographics_combined' in insight_data and not insight_data['demographics_combined'].empty:
                            df = insight_data['demographics_combined'].copy()
                            
                            # 매출 비중 계산
                            total_revenue = df['현재매출'].sum()
                            if total_revenue > 0:
                                df['매출비중'] = (df['현재매출'] / total_revenue * 100).round(1)
                            else:
                                df['매출비중'] = 0
                            
                            # 순위 추가
                            df.insert(0, '순위', range(1, len(df) + 1))
                            
                            # 포맷 적용
                            df['현재매출'] = df['현재매출'].apply(format_currency)
                            df['이전매출'] = df['이전매출'].apply(format_currency)
                            df['매출변화'] = df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                            df['매출증감율'] = df['매출증감율'].apply(format_percent)
                            df['매출비중'] = df['매출비중'].apply(lambda x: f"{x:.1f}%")
                            df['현재세션'] = df['현재세션'].apply(format_number)
                            df['이전세션'] = df['이전세션'].apply(format_number)
                            df['세션변화'] = df['세션변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_number(abs(x))}")
                            df['세션증감율'] = df['세션증감율'].apply(format_percent)
                            
                            cols_to_show = ['순위', '인구통계', '현재매출', '매출비중', '이전매출', '매출변화', '매출증감율',
                                          '현재세션', '이전세션', '세션변화', '세션증감율']
                            st.dataframe(df[cols_to_show], use_container_width=True, height=600)
                        else:
                            st.info("데이터가 없습니다.")
                    
                    with tab4:
                        if 'demo' in insight_data and not insight_data['demo'].empty:
                            df = insight_data['demo'].copy()
                            
                            # 매출 비중 계산
                            total_revenue = df['현재매출'].sum()
                            if total_revenue > 0:
                                df['매출비중'] = (df['현재매출'] / total_revenue * 100).round(1)
                            else:
                                df['매출비중'] = 0
                            
                            # 순위 추가
                            df.insert(0, '순위', range(1, len(df) + 1))
                            
                            df['현재매출'] = df['현재매출'].apply(format_currency)
                            df['이전매출'] = df['이전매출'].apply(format_currency)
                            df['매출변화'] = df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                            df['증감율'] = df['증감율'].apply(format_percent)
                            df['매출비중'] = df['매출비중'].apply(lambda x: f"{x:.1f}%")
                            
                            cols_to_show = ['순위', '지역', '현재매출', '매출비중', '이전매출', '매출변화', '증감율']
                            st.dataframe(df[cols_to_show], use_container_width=True, height=600)
                        else:
                            st.info("데이터가 없습니다.")
                    
                    with tab5:
                        if 'device' in insight_data and not insight_data['device'].empty:
                            df = insight_data['device'].copy()
                            
                            # 매출 비중 계산
                            total_revenue = df['현재매출'].sum()
                            if total_revenue > 0:
                                df['매출비중'] = (df['현재매출'] / total_revenue * 100).round(1)
                            else:
                                df['매출비중'] = 0
                            
                            # 순위 추가
                            df.insert(0, '순위', range(1, len(df) + 1))
                            
                            df['현재매출'] = df['현재매출'].apply(format_currency)
                            df['이전매출'] = df['이전매출'].apply(format_currency)
                            df['매출변화'] = df['매출변화'].apply(lambda x: f"{'↑' if x > 0 else '↓'} {format_currency(abs(x))}")
                            df['증감율'] = df['증감율'].apply(format_percent)
                            df['매출비중'] = df['매출비중'].apply(lambda x: f"{x:.1f}%")
                            
                            cols_to_show = ['순위', '디바이스', '현재매출', '매출비중', '이전매출', '매출변화', '증감율']
                            st.dataframe(df[cols_to_show], use_container_width=True, height=600)
                        else:
                            st.info("데이터가 없습니다.")

else:
    st.info("💡 사이드바에서 기간을 선택해주세요.")
