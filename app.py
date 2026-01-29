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
    당신은 SIDIZ의 데이터 전문가입니다. BigQuery SQL을 생성할 때 다음 규칙을 따르세요:
    
    [필수 규칙]
    1. SQL은 반드시 ```sql ... ``` 코드 블록 안에 작성하세요.
    2. 테이블명: {table_path} (와일드카드 테이블)
    3. 날짜 필터링 예시: _TABLE_SUFFIX BETWEEN '20240101' AND '20240131'
    4. UNNEST를 사용할 때는 반드시 괄호를 닫으세요.
    5. 모든 서브쿼리와 괄호를 정확히 닫으세요.
    
    [데이터 구조]
    - event_date: 날짜 (YYYYMMDD)
    - user_pseudo_id: 사용자 ID
    - items: ARRAY<STRUCT<...>> (제품 정보)
    - traffic_source.source: 유입 경로
    - user_properties: ARRAY (사용자 속성)
    
    [분석 항목]
    (1) 데모그래픽: 연령, 성별
    (2) 유입경로: traffic_source
    (3) 성과: 매출, 수량
    (4) 행태: 이벤트 분석
    (5) 전환율: 구매율
    
    [SQL 예시]
    ```sql
    SELECT
      event_date,
      COUNT(DISTINCT user_pseudo_id) as users,
      SUM(ecommerce.purchase_revenue) as revenue
    FROM `{table_path}`
    WHERE _TABLE_SUFFIX BETWEEN '20240101' AND '20240131'
    GROUP BY event_date
    ORDER BY event_date DESC
    LIMIT 100
    ```
    
    중요: SQL 문법 오류가 없도록 모든 괄호를 정확히 닫고, 올바른 BigQuery 문법을 사용하세요.
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
                # 더 명확한 프롬프트로 SQL 생성 강제
                enhanced_prompt = f"""
{INSTRUCTION}

사용자 질문: {prompt}

반드시 다음 형식으로 답변하세요:

1. 먼저 간단한 분석 설명 (2-3문장)
2. 그 다음 반드시 ```sql 코드블록에 실행 가능한 BigQuery SQL 작성
3. 마지막으로 예상 결과 해석

예시:
최근 1주일 매출을 분석하겠습니다.

```sql
SELECT
  event_date,
  SUM(ecommerce.purchase_revenue) as revenue
FROM `{table_path}`
WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
  AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
  AND event_name = 'purchase'
GROUP BY event_date
ORDER BY event_date DESC
```

이 쿼리는 최근 7일간의 일별 매출을 보여줍니다.
"""
                
                response = model.generate_content(enhanced_prompt)
                answer = response.text
                
                # 인사이트 섹션
                st.markdown("### 💡 AI 인사이트 요약")
                insight = re.sub(r"```sql.*?```", "", answer, flags=re.DOTALL)
                st.info(insight.strip())
                
                # SQL 추출 및 실행 (여러 패턴 시도)
                sql_patterns = [
                    r"```sql\s*(.*?)\s*```",  # 기본 sql 블록
                    r"```SQL\s*(.*?)\s*```",  # 대문자 SQL
                    r"```\s*(SELECT.*?)\s*```",  # SELECT로 시작하는 쿼리
                ]
                
                sql_query = None
                for pattern in sql_patterns:
                    sql_match = re.search(pattern, answer, re.DOTALL | re.IGNORECASE)
                    if sql_match:
                        sql_query = sql_match.group(1).strip()
                        break
                
                if sql_query:
                    
                    # SQL 쿼리 먼저 표시 (디버깅용)
                    with st.expander("🔍 생성된 SQL 쿼리 확인", expanded=True):
                        st.code(sql_query, language='sql')
                    
                    st.markdown("### 📊 데이터 분석 결과")
                    
                    # BigQuery 실행
                    try:
                        query_job = client.query(sql_query)
                        df = query_job.to_dataframe()
                    except Exception as sql_error:
                        st.error(f"SQL 실행 오류: {str(sql_error)}")
                        st.warning("AI가 생성한 SQL에 오류가 있습니다. 쿼리를 수정하거나 질문을 다시 작성해주세요.")
                        raise
                    
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
                    else:
                        st.warning("조회된 데이터가 없습니다.")
                else:
                    st.warning("⚠️ AI가 SQL 쿼리를 생성하지 못했습니다.")
                    
                    # 샘플 쿼리 제공
                    st.info("💡 **샘플 쿼리로 시도해보시겠어요?**")
                    
                    sample_query = f"""
SELECT
  FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) as date,
  COUNT(DISTINCT user_pseudo_id) as users,
  COUNTIF(event_name = 'purchase') as purchases,
  ROUND(SUM(CASE WHEN event_name = 'purchase' THEN ecommerce.purchase_revenue END), 2) as revenue
FROM `{table_path}`
WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
  AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
GROUP BY date
ORDER BY date DESC
LIMIT 100
"""
                    
                    with st.expander("📝 샘플 쿼리 보기"):
                        st.code(sample_query.strip(), language='sql')
                    
                    if st.button("🔄 샘플 쿼리 실행하기"):
                        try:
                            query_job = client.query(sample_query.strip())
                            df = query_job.to_dataframe()
                            
                            if not df.empty:
                                st.success("✅ 샘플 쿼리 실행 완료!")
                                st.dataframe(df, use_container_width=True)
                                
                                # 간단한 차트
                                if 'date' in df.columns and 'revenue' in df.columns:
                                    fig = px.line(df, x='date', y='revenue', title='최근 7일 매출 추이')
                                    st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning("데이터가 없습니다.")
                        except Exception as e:
                            st.error(f"샘플 쿼리 실행 오류: {str(e)}")
                    
                    st.markdown("---")
                    st.markdown("**💬 다시 질문해보세요:**")
                    st.markdown("- '2024년 1월 매출 분석'")
                    st.markdown("- '어제 구매 데이터 보여줘'")
                    st.markdown("- '최근 30일 사용자 분석'")
                
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
