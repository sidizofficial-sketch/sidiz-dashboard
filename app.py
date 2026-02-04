import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
import html
import time

# 1. 페이지 설정
st.set_page_config(
    page_title="시디즈 UX 라이팅 & 데이터 인사이트",
    page_icon="✏️",
    layout="wide"
)

# 2. 스타일 설정 (한자/깨짐 방지 및 버튼 스타일)
st.markdown("""
<style>
    .kpi-card { background-color: #f8f9fa; padding: 20px; border-radius: 10px; border-left: 5px solid #0066cc; }
    .response-container { position: relative; padding: 10px; border: 1px solid #ddd; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# 3. API 및 클라이언트 설정
try:
    # Gemini 설정
    genai.configure(api_key=st.secrets["gemini"]["api_key"])
    model = genai.GenerativeModel('gemini-1.5-flash')
    
    # BigQuery 설정
    bq_client = bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])
except Exception as e:
    st.error(f"⚠️ 설정 로드 오류: {e}")
    st.stop()

# 4. 데이터 로드 함수 (에러 로깅 강화)
@st.cache_data(ttl=3600)
def get_dashboard_data(start_date, end_date):
    # 날짜 형식을 BQ STRING 형식('2026-02-04')으로 강제 변환
    s_date = start_date.strftime('%Y-%m-%d')
    e_date = end_date.strftime('%Y-%m-%d')
    
    # [수정 포인트] Canonical View(SSOT) 사용을 권장하며, 현재는 디버깅을 위해 간단한 쿼리로 예시
    # 실제 본인의 쿼리 템플릿으로 교체하세요.
    kpi_query = f"""
        SELECT 
            SUM(sessions) as total_sessions,
            SUM(active_users) as total_users,
            SAFE_DIVIDE(SUM(conversions), SUM(sessions)) * 100 as cvr
        FROM `your_project.analytics.canonical_daily_metrics`
        WHERE date BETWEEN '{s_date}' AND '{e_date}'
    """
    
    try:
        job = bq_client.query(kpi_query)
        result = job.to_dataframe()
        return result
    except Exception as e:
        # 가려진 에러(Redacted)를 방지하기 위해 상세 에러를 화면에 띄움
        st.error("❌ BigQuery 실행 상세 에러 발생")
        st.code(str(e)) # 한자 깨짐이나 SQL 문법 오류가 여기서 확인됨
        return pd.DataFrame()

# 5. 사이드바 - 글로벌 컨트롤
with st.sidebar:
    st.header("📅 기간 설정")
    today = datetime.now()
    d = st.date_input("조회 기간", [today - timedelta(days=7), today])
    
    st.divider()
    if st.button("🗑️ 세션 초기화"):
        st.session_state.clear()
        st.rerun()

# 6. 메인 UI - KPI 영역 (기존 지표 복구)
st.title("📊 시디즈 데이터 인사이트")

if len(d) == 2:
    data = get_dashboard_data(d[0], d[1])
    
    if not data.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("총 세션 수", f"{data['total_sessions'][0]:,}")
        with col2:
            st.metric("활성 사용자", f"{data['total_users'][0]:,}")
        with col3:
            st.metric("전환율(CVR)", f"{data['cvr'][0]:.2f}%")
    else:
        st.warning("선택한 기간에 데이터가 없거나 쿼리 오류가 있습니다.")

st.divider()

# 7. AI 분석 및 질문 영역 (Tab 구조)
tab1, tab2 = st.tabs(["💬 AI 어시스턴트", "📝 자동 리포트"])

with tab1:
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("데이터에 대해 궁금한 점을 물어보세요"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            try:
                # 데이터 컨텍스트 포함 (숫자 데이터 -> 텍스트)
                context = data.to_string() if not data.empty else "데이터 없음"
                full_prompt = f"다음 데이터를 바탕으로 질문에 답해줘:\n{context}\n\n질문: {prompt}"
                
                response = model.generate_content(full_prompt)
                st.markdown(response.text)
                st.session_state.messages.append({"role": "assistant", "content": response.text})
            except Exception as e:
                st.error(f"Gemini 오류: {e}")

with tab2:
    st.subheader("🤖 AI 종합 리포트")
    if st.button("리포트 생성"):
        with st.spinner("데이터 분석 중..."):
            # 리포트 생성 로직
            st.write("분석된 리포트 내용이 여기에 표시됩니다.")
