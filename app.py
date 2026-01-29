import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import re
import plotly.express as px
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Dashboard", page_icon="🪑", layout="wide")

# 2. 보안 및 모델 설정
try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    
    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        model_list = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        target = next((m for m in model_list if "1.5-flash" in m), model_list[0])
        model = genai.GenerativeModel(target)
    
    project_id = info['project_id']
    dataset_id = "analytics_487246344"
    table_path = f"{project_id}.{dataset_id}.events_*"  # 수정: 백틱(`) → 따옴표(")
    
    INSTRUCTION = f"""
    당신은 SIDIZ의 데이터 전문가입니다.
    1. SQL은 ```sql ... ``` 블록에 작성하고 테이블은 {table_path}를 사용하세요.
    2. 결과에 age, gender, source, revenue, quantity가 포함되게 하세요.
    3. 상품 필터링은 UNNEST(items)를 사용하세요.
    4. (1)데모그래픽 (2)유입경로 (3)성과 (4)행태 (5)전환율 5대 지표를 분석하세요.
    """
    
except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 3. UI 구성
st.title("🪑 SIDIZ AI Intelligence Dashboard")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 4. 분석 실행 로직
if prompt := st.chat_input("질문을 입력하세요 (예: T50 분석해줘)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        try:
            with st.spinner("AI 엔진 분석 중..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                
                # 인사이트 섹션
                st.markdown("### 💡 AI 인사이트 요약")
                insight = re.sub(r"```sql.*?```", "", answer, flags=re.DOTALL)
                st.info(insight.strip())
                
                # SQL 추출 및 실행
                sql_match = re.search(r"```sql\n(.*?)\n```", answer, re.DOTALL)
                
                if sql_match:
                    sql_query = sql_match.group(1).strip()
                    
                    st.markdown("### 📊 데이터 분석 결과")
                    
                    # BigQuery 실행
                    query_job = client.query(sql_query)
                    df = query_job.to_dataframe()
                    
                    if not df.empty:
                        # 데이터 테이블 표시
                        st.dataframe(df, use_container_width=True)
                        
                        # 시각화 자동 생성
                        st.markdown("### 📈 시각화")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # 성별 분석 (gender 컬럼이 있는 경우)
                            if 'gender' in df.columns:
                                fig_gender = px.pie(df, names='gender', values='revenue' if 'revenue' in df.columns else df.columns[0],
                                                   title='성별 분포')
                                st.plotly_chart(fig_gender, use_container_width=True)
                        
                        with col2:
                            # 유입경로 분석 (source 컬럼이 있는 경우)
                            if 'source' in df.columns:
                                fig_source = px.bar(df, x='source', y='revenue' if 'revenue' in df.columns else df.columns[0],
                                                   title='유입경로별 성과')
                                st.plotly_chart(fig_source, use_container_width=True)
                        
                        # 추가 차트
                        if 'age' in df.columns and len(df) > 1:
                            fig_age = px.histogram(df, x='age', title='연령대 분포')
                            st.plotly_chart(fig_age, use_container_width=True)
                        
                        # SQL 쿼리 표시 (확장 가능)
                        with st.expander("🔍 실행된 SQL 쿼리 보기"):
                            st.code(sql_query, language='sql')
                    else:
                        st.warning("조회된 데이터가 없습니다.")
                else:
                    st.warning("SQL 쿼리를 찾을 수 없습니다. AI가 SQL을 생성하도록 질문을 다시 작성해주세요.")
                
                # 메시지 저장
                st.session_state.messages.append({"role": "assistant", "content": answer})
                
        except Exception as e:
            error_msg = f"오류 발생: {str(e)}"
            st.error(error_msg)
            st.session_state.messages.append({"role": "assistant", "content": error_msg})

# 5. 사이드바 - 추가 정보
with st.sidebar:
    st.markdown("### 📌 사용 가이드")
    st.markdown("""
    - **T50 분석해줘**: T50 제품 분석
    - **최근 1주일 매출**: 기간별 매출 분석
    - **20대 여성 구매 패턴**: 세그먼트 분석
    """)
    
    if st.button("🗑️ 대화 기록 초기화"):
        st.session_state.messages = []
        st.rerun()
