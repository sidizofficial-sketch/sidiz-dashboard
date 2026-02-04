import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go
import google.generativeai as genai

# 1. 페이지 설정 및 디자인 적용
st.set_page_config(page_title="SIDIZ AI Intelligence Dashboard", layout="wide")
st.markdown("""
    <style>
    .main { background-color: #f9f9f9; }
    div[data-testid="stMetricValue"] { font-size: 1.8rem; color: #0066cc; }
    </style>
    """, unsafe_allow_html=True)

# Gemini 설정
if "gemini_api_key" in st.secrets:
    genai.configure(api_key=st.secrets["gemini_api_key"])
    HAS_GEMINI = True
else:
    HAS_GEMINI = False

# 2. BigQuery 클라이언트 (에러 핸들링 보강)
@st.cache_resource
def get_bq_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    except Exception as e:
        st.error(f"❌ BigQuery 인증 실패: {e}")
        return None

client = get_bq_client()

# 3. 보정 함수: 상품명 정제 (옵션 제거)
def clean_product_name(name):
    if not name: return name
    for char in [' - ', ' / ', ' (']:
        if char in name: name = name.split(char)[0]
    return name.strip()

# 4. 통합 데이터 추출 로직
@st.cache_data(ttl=3600)
def get_all_dashboard_data(start_c, end_c, start_p, end_p, time_unit, item_identifiers):
    if client is None: return None, None, None, None
    
    # 시간 단위 SQL 설정
    group_dict = {
        "일별": "CAST(date AS STRING)",
        "주별": "FORMAT_DATE('%Y-%U', date)",
        "월별": "FORMAT_DATE('%Y-%m', date)"
    }
    group_sql = group_dict.get(time_unit, "date")

    id_list = [x.strip() for x in item_identifiers.split(',')]
    formatted_ids = ", ".join([f"'{i}'" for i in id_list])

    # [A] 메인 KPI 쿼리 (Current/Previous 통합)
    kpi_query = f"""
    WITH raw AS (
      SELECT PARSE_DATE('%Y%m%d', event_date) as date, user_pseudo_id, event_name, ecommerce.purchase_revenue,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as sid
      FROM `sidiz-458301.analytics_487246344.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{min(start_c, start_p).strftime('%Y%m%d')}' AND '{max(end_c, end_p).strftime('%Y%m%d')}'
    )
    SELECT 
        CASE WHEN date BETWEEN '{start_c}' AND '{end_c}' THEN 'Current' ELSE 'Previous' END as type,
        COUNT(DISTINCT user_pseudo_id) as users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(sid AS STRING))) as sessions,
        COUNTIF(event_name = 'purchase') as orders,
        SUM(purchase_revenue) as revenue
    FROM raw GROUP BY 1 HAVING type IS NOT NULL
    """

    # [C] 고유 상품 정밀 분석 쿼리
    product_query = f"""
    SELECT item.item_name,
    COUNTIF(event_name = 'view_item') as views,
    COUNTIF(event_name = 'purchase') as orders,
    SUM(item.item_revenue) as revenue
    FROM `sidiz-458301.analytics_487246344.events_*`, UNNEST(items) as item
    WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    AND (item.item_id IN ({formatted_ids}) OR item.item_name IN ({formatted_ids}))
    GROUP BY 1 ORDER BY revenue DESC
    """

    # [D] 시계열 쿼리
    ts_query = f"""
    SELECT {group_sql} as period_label, SUM(ecommerce.purchase_revenue) as revenue,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))) as sessions
    FROM `sidiz-458301.analytics_487246344.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{start_c.strftime('%Y%m%d')}' AND '{end_c.strftime('%Y%m%d')}'
    GROUP BY 1 ORDER BY 1
    """

    try:
        kpi_df = client.query(kpi_query).to_dataframe()
        prod_df = client.query(product_query).to_dataframe()
        ts_df = client.query(ts_query).to_dataframe()
        return kpi_df, prod_df, ts_df
    except Exception as e:
        st.error(f"데이터 추출 오류: {e}")
        return None, None, None

# 5. UI 메인 로직
st.title("🪑 SIDIZ AI Intelligence Dashboard")

with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_d = st.date_input("분석 기간", [datetime.now()-timedelta(days=8), datetime.now()-timedelta(days=1)])
    comp_d = st.date_input("비교 기간", [datetime.now()-timedelta(days=16), datetime.now()-timedelta(days=9)])
    time_unit = st.selectbox("추이 단위", ["일별", "주별", "월별"])
    item_input = st.text_area("고유 상품코드/명 (쉼표 구분)", value="T500HLDA, TN500HLDA")

if len(curr_d) == 2 and len(comp_d) == 2:
    kpi_df, prod_df, ts_df = get_all_dashboard_data(curr_d[0], curr_d[1], comp_d[0], comp_d[1], time_unit, item_input)
    
    if kpi_df is not None and not kpi_df.empty:
        curr = kpi_df[kpi_df['type']=='Current'].iloc[0]
        prev = kpi_df[kpi_df['type']=='Previous'].iloc[0] if 'Previous' in kpi_df['type'].values else curr

        # [섹션 1: 주요 KPI]
        st.subheader("🎯 핵심 성과 요약 (전기 대비)")
        k1, k2, k3, k4 = st.columns(4)
        def delta(c, p): return f"{((c-p)/p*100):+.1f}%" if p > 0 else None
        
        k1.metric("매출액", f"₩{int(curr['revenue'] or 0):,}", delta(curr['revenue'], prev['revenue']))
        k2.metric("주문수", f"{int(curr['orders']):,}", delta(curr['orders'], prev['orders']))
        k3.metric("세션", f"{int(curr['sessions']):,}", delta(curr['sessions'], prev['sessions']))
        
        cvr = (curr['orders']/curr['sessions']*100) if curr['sessions'] > 0 else 0
        prev_cvr = (prev['orders']/prev['sessions']*100) if prev['sessions'] > 0 else 0
        k4.metric("구매전환율(CVR)", f"{cvr:.2f}%", delta(cvr, prev_cvr))

        # [섹션 2: AI 인사이트]
        if HAS_GEMINI:
            with st.expander("🤖 AI 비즈니스 분석 리포트", expanded=True):
                if st.button("인사이트 생성"):
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    prompt = f"시디즈 매출 ₩{int(curr['revenue']):,}, 주문 {curr['orders']}건, CVR {cvr:.2f}%입니다. 전기 대비 매출 변동은 {delta(curr['revenue'], prev['revenue'])}입니다. 분석 결과를 요약해줘."
                    st.write(model.generate_content(prompt).text)

        # [섹션 3: 추이 그래프]
        st.markdown("---")
        st.subheader(f"📊 {time_unit} 매출 및 세션 추이")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=ts_df['period_label'], y=ts_df['revenue'], name='매출', marker_color='#2ca02c'))
        fig.add_trace(go.Scatter(x=ts_df['period_label'], y=ts_df['sessions'], name='세션', yaxis='y2', line=dict(color='#1f77b4', width=3)))
        fig.update_layout(
            yaxis=dict(title="매출액 (₩)"),
            yaxis2=dict(title="세션 수", overlaying='y', side='right'),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)

        # [섹션 4: 상세 분석]
        st.markdown("---")
        st.subheader("📍 고유 상품별 정밀 성과")
        if not prod_df.empty:
            prod_df['item_name'] = prod_df['item_name'].apply(clean_product_name)
            final_prod = prod_df.groupby('item_name').sum().reset_index().sort_values('revenue', ascending=False)
            st.dataframe(final_prod.style.format({'revenue': '₩{:,.0f}', 'orders': '{:,}'}), use_container_width=True)
