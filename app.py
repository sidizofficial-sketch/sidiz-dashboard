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
    당신은 SIDIZ의 BigQuery 데이터 분석가입니다.
    
    [중요: 간단한 SQL만 작성하세요]
    - 복잡한 서브쿼리, CTE, 윈도우 함수는 사용하지 마세요
    - 기본적인 SELECT, WHERE, GROUP BY, ORDER BY만 사용하세요
    - 모든 괄호를 정확히 닫으세요
    
    [테이블 정보]
    테이블: {table_path}
    날짜 필터: _TABLE_SUFFIX BETWEEN '20240101' AND '20240131'
    
    [GA4 이벤트 구조]
    - event_date: 이벤트 날짜 (STRING, YYYYMMDD)
    - event_name: 이벤트 이름 ('purchase', 'page_view' 등)
    - user_pseudo_id: 사용자 ID
    - items: 구매 상품 정보 (ARRAY)
    - ecommerce.purchase_revenue: 구매 금액
    
    [SQL 작성 규칙]
    1. 반드시 ```sql 코드블록 안에 작성
    2. 상품 필터링 시: WHERE EXISTS (SELECT 1 FROM UNNEST(items) AS item WHERE item.item_name LIKE '%상품명%')
    3. 날짜는 최근 7일: _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
    4. 항상 LIMIT 100 추가
    
    [올바른 SQL 예시]
    ```sql
    SELECT
      event_date,
      COUNT(DISTINCT user_pseudo_id) as users,
      COUNTIF(event_name = 'purchase') as purchases
    FROM `{table_path}`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
    GROUP BY event_date
    ORDER BY event_date DESC
    LIMIT 100
    ```
    
    중요: 복잡한 분석이 필요하면 여러 개의 간단한 쿼리로 나누세요.
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
            # 날짜 키워드 감지 및 기간 자동 설정
            import re
            period_detected = False
            
            if "최근" in prompt or "지난" in prompt:
                # 숫자 추출
                numbers = re.findall(r'\d+', prompt)
                if numbers:
                    days = int(numbers[0])
                    
                    from datetime import datetime, timedelta
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=days)
                    
                    st.session_state['start_date'] = start_date.strftime('%Y%m%d')
                    st.session_state['end_date'] = end_date.strftime('%Y%m%d')
                    st.session_state['period_label'] = f"최근 {days}일"
                    
                    st.info(f"📅 분석 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} ({days}일)")
                    period_detected = True
            
            # 기간이 설정되지 않은 경우 (선택 안함 상태)
            use_period_in_sql = 'start_date' in st.session_state
            
            # SQL에 사용할 임시 기간 설정
            if not use_period_in_sql:
                from datetime import datetime, timedelta
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)
                
                temp_start = start_date.strftime('%Y%m%d')
                temp_end = end_date.strftime('%Y%m%d')
                
                if not period_detected:
                    st.info(f"💡 AI가 질문에 맞춰 기간을 자동 설정합니다")
            else:
                temp_start = st.session_state['start_date']
                temp_end = st.session_state['end_date']
                if not period_detected:
                    st.info(f"📅 설정된 분석 기간: {st.session_state['period_label']}")
            
            with st.spinner("AI 엔진 분석 중..."):
                # 프롬프트 생성 (기간 설정 여부에 따라 다르게)
                if use_period_in_sql:
                    # 사용자가 기간을 명시적으로 선택한 경우
                    date_instruction = f"""
중요: WHERE 절에 다음 날짜 조건을 반드시 포함하세요:
WHERE _TABLE_SUFFIX BETWEEN '{temp_start}' AND '{temp_end}'
"""
                else:
                    # 선택 안함 - AI가 자유롭게 판단
                    date_instruction = f"""
날짜 필터링:
- 사용자가 "최근 N일" 같은 키워드를 사용하면 해당 기간 사용
- 그 외에는 질문 맥락에 맞는 적절한 기간 사용
- 기본 예시: WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
- 또는: WHERE _TABLE_SUFFIX BETWEEN '{temp_start}' AND '{temp_end}'
"""
                
                enhanced_prompt = f"""
{INSTRUCTION}

사용자 질문: {prompt}

{date_instruction}

반드시 다음 형식으로 답변하세요:

1. 먼저 간단한 분석 설명 (2-3문장)
2. 그 다음 반드시 ```sql 코드블록에 실행 가능한 BigQuery SQL 작성
3. 마지막으로 예상 결과 해석

예시:
매출을 분석하겠습니다.

```sql
SELECT
  PARSE_DATE('%Y%m%d', event_date) as date,
  COUNTIF(event_name = 'purchase') as purchases,
  ROUND(SUM(ecommerce.purchase_revenue), 2) as revenue
FROM `{table_path}`
WHERE _TABLE_SUFFIX BETWEEN '{temp_start}' AND '{temp_end}'
GROUP BY date
ORDER BY date DESC
```

이 쿼리는 지정된 기간의 일별 매출을 보여줍니다.
"""
GROUP BY date
ORDER BY date DESC
```

이 쿼리는 지정된 기간의 일별 매출을 보여줍니다.
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
                        
                        # SQL 복사 버튼
                        if st.button("📋 SQL 복사하기"):
                            st.code(sql_query, language='sql')
                            st.success("SQL을 선택해서 복사하세요!")
                    
                    st.markdown("### 📊 데이터 분석 결과")
                    
                    # BigQuery 실행 (재시도 로직 포함)
                    max_retries = 2
                    for attempt in range(max_retries):
                        try:
                            query_job = client.query(sql_query)
                            df = query_job.to_dataframe()
                            break  # 성공하면 루프 탈출
                            
                        except Exception as sql_error:
                            error_msg = str(sql_error)
                            
                            if attempt < max_retries - 1:
                                st.warning(f"⚠️ SQL 오류 발생. AI에게 수정 요청 중... (시도 {attempt + 1}/{max_retries})")
                                
                                # Gemini에게 SQL 수정 요청
                                fix_prompt = f"""
다음 BigQuery SQL에 오류가 발생했습니다:

```sql
{sql_query}
```

오류 메시지:
{error_msg}

이 오류를 수정한 올바른 SQL을 ```sql 코드블록 안에만 작성해주세요. 설명은 필요없고 오직 수정된 SQL만 제공하세요.
"""
                                fix_response = model.generate_content(fix_prompt)
                                fix_answer = fix_response.text
                                
                                # 수정된 SQL 추출
                                for pattern in sql_patterns:
                                    fix_match = re.search(pattern, fix_answer, re.DOTALL | re.IGNORECASE)
                                    if fix_match:
                                        sql_query = fix_match.group(1).strip()
                                        st.info("🔄 수정된 SQL로 재시도합니다...")
                                        with st.expander("🔧 수정된 SQL 보기"):
                                            st.code(sql_query, language='sql')
                                        break
                            else:
                                # 최종 실패
                                st.error(f"❌ SQL 실행 오류: {error_msg}")
                                st.warning("💡 **해결 방법:**")
                                st.markdown("1. 위의 'SQL 복사하기' 버튼으로 쿼리를 복사하세요")
                                st.markdown("2. [BigQuery 콘솔](https://console.cloud.google.com/bigquery)에서 직접 실행해보세요")
                                st.markdown("3. 질문을 더 구체적으로 바꿔서 다시 시도하세요")
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
  event_date,
  COUNT(DISTINCT user_pseudo_id) as users,
  COUNTIF(event_name = 'purchase') as purchases
FROM `{table_path}`
WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
  AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
GROUP BY event_date
ORDER BY event_date DESC
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
                                if 'event_date' in df.columns and 'users' in df.columns:
                                    fig = px.line(df, x='event_date', y='users', title='최근 7일 사용자 추이')
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
    st.markdown("### 📅 기간 선택")
    
    # 날짜 범위 선택
    date_option = st.radio(
        "분석 기간",
        ["선택 안함", "빠른 선택", "직접 선택"],
        horizontal=True,
        index=0  # 기본값: 선택 안함
    )
    
    if date_option == "선택 안함":
        # 기간 설정 초기화
        if 'start_date' in st.session_state:
            del st.session_state['start_date']
        if 'end_date' in st.session_state:
            del st.session_state['end_date']
        if 'period_label' in st.session_state:
            del st.session_state['period_label']
        
        st.info("💡 AI가 질문에 맞춰 자동으로 기간을 설정합니다")
        
    elif date_option == "빠른 선택":
        quick_period = st.selectbox(
            "기간",
            ["최근 7일", "최근 14일", "최근 30일", "최근 90일"]
        )
        
        period_map = {
            "최근 7일": 7,
            "최근 14일": 14,
            "최근 30일": 30,
            "최근 90일": 90
        }
        days = period_map[quick_period]
        
        # 계산된 날짜 표시
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        st.info(f"📆 {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
        
        st.session_state['analysis_days'] = days
        st.session_state['start_date'] = start_date.strftime('%Y%m%d')
        st.session_state['end_date'] = end_date.strftime('%Y%m%d')
        st.session_state['period_label'] = quick_period
        
    else:  # 직접 선택
        # 직접 날짜 선택
        from datetime import datetime, timedelta
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "시작일",
                value=datetime.now() - timedelta(days=7),
                max_value=datetime.now()
            )
        with col2:
            end_date = st.date_input(
                "종료일",
                value=datetime.now(),
                max_value=datetime.now()
            )
        
        if start_date and end_date:
            days_diff = (end_date - start_date).days + 1
            st.success(f"✅ 선택된 기간: **{days_diff}일**")
            
            st.session_state['start_date'] = start_date.strftime('%Y%m%d')
            st.session_state['end_date'] = end_date.strftime('%Y%m%d')
            st.session_state['period_label'] = f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}"
            st.session_state['analysis_days'] = days_diff
    
    st.markdown("---")
    st.markdown("### 📌 사용 가이드")
    
    # 빠른 분석 템플릿
    st.markdown("#### 🚀 빠른 분석")
    
    if st.button("📅 사용자 추이 분석"):
        # 기간이 설정되지 않았으면 기본값 사용
        if 'start_date' not in st.session_state:
            from datetime import datetime, timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            st.session_state['start_date'] = start_date.strftime('%Y%m%d')
            st.session_state['end_date'] = end_date.strftime('%Y%m%d')
            st.session_state['period_label'] = "최근 7일 (기본)"
        
        template_query = f"""
SELECT
  PARSE_DATE('%Y%m%d', event_date) as date,
  COUNT(DISTINCT user_pseudo_id) as users,
  COUNTIF(event_name = 'purchase') as purchases
FROM `{table_path}`
WHERE _TABLE_SUFFIX BETWEEN '{st.session_state['start_date']}' AND '{st.session_state['end_date']}'
GROUP BY date
ORDER BY date DESC
"""
        st.session_state['quick_query'] = template_query
        st.session_state['query_type'] = 'user_trend'
        st.rerun()
    
    if st.button("💰 매출 추이 분석"):
        # 기간이 설정되지 않았으면 기본값 사용
        if 'start_date' not in st.session_state:
            from datetime import datetime, timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            st.session_state['start_date'] = start_date.strftime('%Y%m%d')
            st.session_state['end_date'] = end_date.strftime('%Y%m%d')
            st.session_state['period_label'] = "최근 30일 (기본)"
        
        template_query = f"""
SELECT
  PARSE_DATE('%Y%m%d', event_date) as date,
  COUNTIF(event_name = 'purchase') as purchases,
  ROUND(SUM(ecommerce.purchase_revenue), 2) as revenue
FROM `{table_path}`
WHERE _TABLE_SUFFIX BETWEEN '{st.session_state['start_date']}' AND '{st.session_state['end_date']}'
GROUP BY date
ORDER BY date DESC
"""
        st.session_state['quick_query'] = template_query
        st.session_state['query_type'] = 'revenue_trend'
        st.rerun()
    
    if st.button("🪑 T50 제품 분석"):
        # 기간이 설정되지 않았으면 기본값 사용
        if 'start_date' not in st.session_state:
            from datetime import datetime, timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            st.session_state['start_date'] = start_date.strftime('%Y%m%d')
            st.session_state['end_date'] = end_date.strftime('%Y%m%d')
            st.session_state['period_label'] = "최근 30일 (기본)"
        
        template_query = f"""
SELECT
  PARSE_DATE('%Y%m%d', event_date) as date,
  COUNT(DISTINCT user_pseudo_id) as users,
  COUNTIF(event_name = 'purchase') as purchases
FROM `{table_path}`,
  UNNEST(items) AS item
WHERE _TABLE_SUFFIX BETWEEN '{st.session_state['start_date']}' AND '{st.session_state['end_date']}'
  AND item.item_name LIKE '%T50%'
GROUP BY date
ORDER BY date DESC
LIMIT 100
"""
        st.session_state['quick_query'] = template_query
        st.session_state['query_type'] = 't50_analysis'
        st.rerun()
    
    st.markdown("---")
    st.markdown("#### 💬 질문 예시")
    st.markdown("""
    - **최근 7일 매출 분석해줘**
    - **작년 12월 데이터 보여줘**
    - **T50 구매 추이**
    """)
    
    if st.button("🗑️ 대화 기록 초기화"):
        st.session_state.messages = []
        if 'quick_query' in st.session_state:
            del st.session_state['quick_query']
        st.rerun()

# 빠른 쿼리 실행
if 'quick_query' in st.session_state and st.session_state['quick_query']:
    with st.chat_message("assistant"):
        # 집계 기간 표시
        if 'period_label' in st.session_state:
            st.markdown(f"### 📊 분석 결과 | 📅 {st.session_state['period_label']}")
        else:
            st.markdown("### 📊 빠른 분석 결과")
        
        try:
            query_job = client.query(st.session_state['quick_query'])
            df = query_job.to_dataframe()
            
            if not df.empty:
                # 데이터를 날짜 순으로 정렬 (차트용)
                if 'date' in df.columns:
                    df = df.sort_values('date')
                
                # KPI 카드 (주요 지표) - 기간 정보 포함
                st.markdown(f"#### 핵심 지표")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    if 'users' in df.columns:
                        total_users = df['users'].sum()
                        st.metric("총 사용자", f"{total_users:,}")
                    elif 'purchases' in df.columns:
                        total_purchases = df['purchases'].sum()
                        st.metric("총 구매", f"{total_purchases:,}건")
                
                with col2:
                    if 'purchases' in df.columns:
                        total_purchases = df['purchases'].sum()
                        st.metric("총 구매", f"{total_purchases:,}건")
                
                with col3:
                    if 'revenue' in df.columns:
                        total_revenue = df['revenue'].sum()
                        st.metric("총 매출", f"₩{total_revenue:,.0f}")
                
                with col4:
                    if 'users' in df.columns and 'purchases' in df.columns:
                        conversion = (df['purchases'].sum() / df['users'].sum() * 100) if df['users'].sum() > 0 else 0
                        st.metric("평균 전환율", f"{conversion:.1f}%")
                    elif 'revenue' in df.columns and 'purchases' in df.columns:
                        avg_order_value = df['revenue'].sum() / df['purchases'].sum() if df['purchases'].sum() > 0 else 0
                        st.metric("평균 객단가", f"₩{avg_order_value:,.0f}")
                
                st.markdown("---")
                
                # 메인 차트들
                if len(df) > 1 and 'date' in df.columns:
                    
                    # 사용자 & 구매 추이 (듀얼 차트)
                    if 'users' in df.columns and 'purchases' in df.columns:
                        fig = go.Figure()
                        
                        fig.add_trace(go.Scatter(
                            x=df['date'], 
                            y=df['users'],
                            name='사용자',
                            mode='lines+markers',
                            line=dict(color='#1f77b4', width=3),
                            marker=dict(size=8)
                        ))
                        
                        fig.add_trace(go.Scatter(
                            x=df['date'], 
                            y=df['purchases'],
                            name='구매',
                            mode='lines+markers',
                            line=dict(color='#ff7f0e', width=3),
                            marker=dict(size=8),
                            yaxis='y2'
                        ))
                        
                        fig.update_layout(
                            title=f'일별 사용자 및 구매 추이 ({st.session_state.get("period_label", "")})',
                            xaxis=dict(title='날짜'),
                            yaxis=dict(title='사용자 수', side='left'),
                            yaxis2=dict(title='구매 건수', overlaying='y', side='right'),
                            hovermode='x unified',
                            height=400,
                            showlegend=True,
                            legend=dict(x=0.01, y=0.99)
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # 매출 추이
                    elif 'revenue' in df.columns:
                        fig = go.Figure()
                        
                        fig.add_trace(go.Bar(
                            x=df['date'],
                            y=df['revenue'],
                            marker=dict(
                                color=df['revenue'],
                                colorscale='Blues',
                                showscale=True,
                                colorbar=dict(title="매출(₩)")
                            ),
                            text=df['revenue'].apply(lambda x: f'₩{x:,.0f}'),
                            textposition='outside'
                        ))
                        
                        fig.update_layout(
                            title=f'일별 매출 추이 ({st.session_state.get("period_label", "")})',
                            xaxis=dict(title='날짜'),
                            yaxis=dict(title='매출 (₩)'),
                            height=400,
                            showlegend=False
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # 단일 지표 라인 차트
                    elif 'users' in df.columns:
                        fig = px.area(df, x='date', y='users', 
                                     title=f'일별 사용자 추이 ({st.session_state.get("period_label", "")})',
                                     color_discrete_sequence=['#636EFA'])
                        fig.update_traces(line=dict(width=3))
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                
                # 데이터 테이블
                with st.expander("📋 상세 데이터 보기"):
                    st.dataframe(df, use_container_width=True)
                
                # SQL 쿼리
                with st.expander("🔍 실행된 쿼리"):
                    st.code(st.session_state['quick_query'], language='sql')
            else:
                st.warning("⚠️ 데이터가 없습니다. 날짜 범위를 조정하거나 다른 분석을 시도해보세요.")
                
        except Exception as e:
            st.error(f"❌ 쿼리 실행 오류: {str(e)}")
            with st.expander("🔍 실행하려던 쿼리"):
                st.code(st.session_state['quick_query'], language='sql')
        
        # 쿼리 실행 후 세션에서 제거
        del st.session_state['quick_query']
