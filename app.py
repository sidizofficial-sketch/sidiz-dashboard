import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ Intelligence Dashboard", layout="wide")

# -------------------------------------------------
# 2. AI API 설정 (강화된 인식 로직)
# -------------------------------------------------
HAS_GEMINI = False
# 여러 경로에서 키를 시도합니다.
api_key = (
    st.secrets.get("gemini_api_key") or 
    st.secrets.get("gemini", {}).get("gemini_api_key") or
    st.secrets.get("gemini_api_key", {}).get("gemini_api_key")
)

if api_key:
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        HAS_GEMINI = True
        st.sidebar.success("✅ AI 인사이트 준비 완료")
    except Exception as e:
        st.sidebar.error(f"❌ AI 연결 중 오류: {e}")
else:
    st.sidebar.warning("🔑 Secrets에서 API 키를 찾을 수 없습니다.")

# -------------------------------------------------
# 3. BigQuery 클라이언트
# -------------------------------------------------
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
# 4. 데이터 추출 함수 (대량구매 150만원 로직 포함)
# -------------------------------------------------
def get_dashboard_data(start_c, end_c, start_p, end_p):
    if client is None: return None
    
    # 날짜 포맷팅
    s_c, e_c = start_c.strftime('%Y%m%d'), end_c.strftime('%Y%m%d')
    s_p, e_p = start_p.strftime('%Y%m%d'), end_p.strftime('%Y%m%d')

    query = f"""
    WITH base AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) as date,
            user_pseudo_id, event_name, ecommerce.purchase_revenue as rev,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) as sid,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) as s_num
        FROM `sidiz-458301.analytics_487246344.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{min(s_c, s_p)}' AND '{max(e_c, e_p)}'
    )
    SELECT 
        CASE 
            WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' 
            WHEN date BETWEEN '{start_p}' AND '{end_p}' THEN 'Previous'
        END as type,
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
    except Exception as e:
        st.error(f"데이터 쿼리 실패: {e}")
        return None

# -------------------------------------------------
# 5. UI 및 대시보드 출력
# -------------------------------------------------
st.title("🪑 SIDIZ AI Intelligence Dashboard")

today = datetime.now()
with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간", [today - timedelta(days=7), today - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [today - timedelta(days=14), today - timedelta(days=8)])

if len(curr_date) == 2:
    df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1])
    
    if df is not None and not df.empty and 'Current' in df['type'].values:
        curr = df[df['type'] == 'Current'].iloc[0]
        prev = df[df['type'] == 'Previous'].iloc[0] if 'Previous' in df['type'].values else curr

        # [핵심 KPI 리스트]
        st.subheader("🎯 핵심 성과 (KPI)")
        def d(c, p): return f"{((c-p)/p*100):+.1f}%" if p > 0 else "0%"
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("활성 사용자", f"{int(curr['users']):,}", d(curr['users'], prev['users']))
        c2.metric("신규 방문율", f"{(curr['new_users']/curr['users']*100):.1f}%")
        c3.metric("총 매출액", f"₩{int(curr['revenue']):,}", d(curr['revenue'], prev['revenue']))
        c4.metric("구매전환율", f"{(curr['orders']/curr['sessions']*100):.2f}%")

        # [대량 구매 성과 전용 섹션]
        st.markdown("---")
        st.subheader("📦 대량 구매 리포트 (150만원↑)")
        b1, b2, b3 = st.columns(3)
        b1.metric("대량 주문 건수", f"{int(curr['bulk_orders'])}건", f"{int(curr['bulk_orders'] - prev['bulk_orders']):+}건")
        b2.metric("대량 구매 매출", f"₩{int(curr['bulk_revenue']):,}", d(curr['bulk_revenue'], prev['bulk_revenue']))
        b3.metric("대량 구매 매출 비중", f"{(curr['bulk_revenue']/curr['revenue']*100 if curr['revenue']>0 else 0):.1f}%")

        # [AI 인사이트 리포트]
        st.markdown("---")
        st.subheader("🧠 AI 전략 인사이트")
        if HAS_GEMINI:
            try:
                with st.spinner("AI가 데이터를 분석 중입니다..."):
                    prompt = f"""시디즈 데이터 분석가로서 보고서를 작성해줘. 
                    - 매출 {int(curr['revenue']):,}원 ({d(curr['revenue'], prev['revenue'])})
                    - 대량구매(150만원 이상) 건수: {int(curr['bulk_orders'])}건
                    위 성과를 기반으로 한 줄 요약과 B2B 성장을 위한 마케팅 제안을 3문장으로 적어줘."""
                    st.markdown(model.generate_content(prompt).text)
            except: st.write("🤖 AI 분석 기능 일시 지연")
        else:
            st.warning("🔑 AI 설정을 위해 Secrets에 'gemini_api_key'를 추가해주세요.")
    else:
        st.warning("⚠️ 해당 기간에 데이터가 없습니다. 날짜를 조금 더 과거로 설정해 보세요.")
