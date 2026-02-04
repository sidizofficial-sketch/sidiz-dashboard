import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Intelligence Dashboard", layout="wide")

# 2. API 설정 (더 유연하게 수정)
HAS_GEMINI = False
if "gemini_api_key" in st.secrets:
    try:
        genai.configure(api_key=st.secrets["gemini_api_key"])
        model = genai.GenerativeModel('gemini-1.5-flash')
        HAS_GEMINI = True
    except:
        HAS_GEMINI = False

@st.cache_resource
def get_bq_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except:
        return None

client = get_bq_client()

# -------------------------------------------------
# 3. 데이터 추출 (대량구매 150만원 로직 포함)
# -------------------------------------------------
def get_dashboard_data(start_c, end_c, start_p, end_p):
    if client is None: return None
    
    query = f"""
    WITH base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id, event_name, ecommerce.purchase_revenue as rev,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min(start_c, start_p).strftime('%Y%m%d')}' AND '{max(end_c, end_p).strftime('%Y%m%d')}'
    )
    SELECT 
        CASE WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CASE WHEN s_num = 1 THEN user_pseudo_id END) as new_users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(IFNULL(rev, 0)) as revenue,
        COUNTIF(event_name = 'purchase' AND rev >= 1500000) as bulk_orders,
        SUM(CASE WHEN event_name = 'purchase' AND rev >= 1500000 THEN rev ELSE 0 END) as bulk_revenue
    FROM base GROUP BY 1 HAVING type IS NOT NULL
    """
    try:
        return client.query(query).to_dataframe()
    except:
        return None

# -------------------------------------------------
# 4. 하이브리드 인사이트 (규칙 분석 + AI 분석)
# -------------------------------------------------
def generate_hybrid_insights(curr, prev):
    # (1) 기본 규칙 분석 (API 실패해도 작동)
    rules = []
    c_cr = curr['orders']/curr['sessions'] if curr['sessions'] > 0 else 0
    p_cr = prev['orders']/prev['sessions'] if prev['sessions'] > 0 else 0
    
    if curr['bulk_orders'] > prev['bulk_orders']:
        rules.append({"title": "📦 대량 구매 증가", "content": "150만원 이상 고액 주문이 늘어났습니다. B2B 수요를 체크하세요."})
    if c_cr < p_cr:
        rules.append({"title": "📉 전환율 주의", "content": "유입 대비 구매 건수가 줄었습니다. 상세페이지 이탈을 확인하세요."})
    
    # (2) Gemini AI 분석 시도
    ai_text = ""
    if HAS_GEMINI:
        try:
            prompt = f"""시디즈 데이터 분석가로서 보고서를 작성해줘. 
            매출:{int(curr['revenue']):,}원, 대량구매:{int(curr['bulk_orders'])}건.
            이 데이터를 기반으로 성과 요약과 제안점을 3줄로 적어줘."""
            response = model.generate_content(prompt)
            ai_text = response.text
        except:
            ai_text = "🤖 AI 분석 서버 통신이 지연되고 있습니다. 하단의 규칙 분석을 참고하세요."
    else:
        ai_text = "🔑 API 키 설정이 필요합니다."

    return ai_text, rules

# -------------------------------------------------
# 5. UI 메인 (D-1부터 7일 자동설정)
# -------------------------------------------------
st.title("🪑 SIDIZ AI Intelligence Dashboard")

today = datetime.now()
with st.sidebar:
    st.header("⚙️ 분석 설정")
    # 자동 날짜 설정: 어제(D-1)부터 7일전까지
    curr_date = st.date_input("분석 기간", [today - timedelta(days=7), today - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [today - timedelta(days=14), today - timedelta(days=8)])

if len(curr_date) == 2:
    df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1])
    
    if df is not None and not df.empty:
        curr = df[df['type'] == 'Current'].iloc[0]
        prev = df[df['type'] == 'Previous'].iloc[0] if 'Previous' in df['type'].values else curr

        # [핵심 KPI 리스트]
        st.subheader("🎯 핵심 성과 (KPI)")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("활성 사용자", f"{int(curr['users']):,}")
        c2.metric("신규 방문율", f"{(curr['new_users']/curr['users']*100):.1f}%")
        c3.metric("총 매출액", f"₩{int(curr['revenue']):,}")
        c4.metric("대량 주문(150만↑)", f"{int(curr['bulk_orders'])}건")

        # [AI & 규칙 인사이트]
        st.markdown("---")
        st.subheader("🧠 데이터 인사이트")
        ai_msg, rule_list = generate_hybrid_insights(curr, prev)
        
        with st.expander("🤖 AI 분석 리포트", expanded=True):
            st.write(ai_msg)
            
        cols = st.columns(len(rule_list) if rule_list else 1)
        for i, rule in enumerate(rule_list):
            with cols[i]:
                st.info(f"**{rule['title']}**\n\n{rule['content']}")

else:
    st.info("사이드바에서 분석 기간을 확인해주세요.")
