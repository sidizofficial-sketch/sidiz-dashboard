import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import re
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import datetime, timedelta

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Dashboard", page_icon="🪑", layout="wide")

# 2. 보안 및 모델 설정
try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    
    if "gemini" in st.secrets:
        try:
            genai.configure(api_key=st.secrets["gemini"]["api_key"])
            model_list = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
            target = next((m for m in model_list if "1.5-flash" in m), model_list[0])
            model = genai.GenerativeModel(target)
            gemini_available = True
        except Exception as e:
            st.warning(f"⚠️ Gemini API 사용 불가: {e}")
            model = None
            gemini_available = False
    else:
        model = None
        gemini_available = False
    
    # 네이버 API 설정
    naver_client_id = None
    naver_client_secret = None
    naver_ad_api_key = None
    naver_ad_secret_key = None
    naver_customer_id = None
    
    # [naver] 섹션 확인
    if "naver" in st.secrets:
        naver_client_id = st.secrets["naver"].get("client_id")
        naver_client_secret = st.secrets["naver"].get("client_secret")
        naver_ad_api_key = st.secrets["naver"].get("ad_api_key")
        naver_ad_secret_key = st.secrets["naver"].get("ad_secret_key")
        naver_customer_id = st.secrets["naver"].get("customer_id")
    
    # [naver_ads] 섹션도 확인 (하위 호환성)
    if "naver_ads" in st.secrets:
        naver_ad_api_key = naver_ad_api_key or st.secrets["naver_ads"].get("api_key")
        naver_ad_secret_key = naver_ad_secret_key or st.secrets["naver_ads"].get("secret_key")
        naver_customer_id = naver_customer_id or st.secrets["naver_ads"].get("customer_id")
    
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
    - event_name: 이벤트 이름 ('page_view', 'purchase' 등)
    - user_pseudo_id: 사용자 ID
    - event_params: 이벤트 파라미터 (ARRAY of STRUCT)
      - key: 파라미터 이름 (예: 'page_location', 'page_title')
      - value.string_value: 문자열 값
    - items: 구매 상품 정보 (ARRAY)
    - ecommerce.purchase_revenue: 구매 금액
    
    [event_params 접근 방법]
    페이지 정보는 event_params에 저장되어 있습니다:
    
    ```sql
    -- 페이지 URL 추출
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')
    
    -- 페이지 제목 추출  
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title')
    ```
    
    [제품 분석 예시]
    T50 제품 페이지 분석:
    
    ```sql
    SELECT
      event_date,
      COUNT(DISTINCT user_pseudo_id) as users
    FROM `{table_path}`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
      AND (
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%T50%'
        OR (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') LIKE '%T50%'
      )
    GROUP BY event_date
    ORDER BY event_date DESC
    LIMIT 100
    ```
    
    [SQL 작성 규칙]
    1. 반드시 ```sql 코드블록 안에 작성
    2. 제품 필터링: event_params의 page_location 또는 page_title 사용
    3. 날짜는 _TABLE_SUFFIX 사용
    4. 항상 LIMIT 100 추가
    
    중요: 복잡한 분석이 필요하면 여러 개의 간단한 쿼리로 나누세요.
    """
    
except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 네이버 검색량 조회 함수
def get_naver_search_trend(keywords, start_date, end_date, time_unit='date'):
    """
    네이버 데이터랩 검색어 트렌드 API 호출
    
    Args:
        keywords: 검색어 리스트 (최대 5개)
        start_date: 시작일 (YYYY-MM-DD)
        end_date: 종료일 (YYYY-MM-DD)
        time_unit: 'date', 'week', 'month'
    
    Returns:
        DataFrame with search trend data
    """
    if not naver_client_id or not naver_client_secret:
        return None, "네이버 API 키가 설정되지 않았습니다."
    
    url = "https://openapi.naver.com/v1/datalab/search"
    
    headers = {
        "X-Naver-Client-Id": naver_client_id,
        "X-Naver-Client-Secret": naver_client_secret,
        "Content-Type": "application/json"
    }
    
    keyword_groups = []
    for i, keyword in enumerate(keywords[:5]):  # 최대 5개
        keyword_groups.append({
            "groupName": keyword,
            "keywords": [keyword]
        })
    
    body = {
        "startDate": start_date.replace("-", ""),
        "endDate": end_date.replace("-", ""),
        "timeUnit": time_unit,
        "keywordGroups": keyword_groups
    }
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(body))
        
        if response.status_code == 200:
            data = response.json()
            
            # 데이터 파싱
            results = []
            for result in data['results']:
                keyword = result['title']
                for item in result['data']:
                    results.append({
                        '날짜': item['period'],
                        '키워드': keyword,
                        '검색량': item['ratio']
                    })
            
            df = pd.DataFrame(results)
            return df, None
        else:
            return None, f"API 오류: {response.status_code} - {response.text}"
    
    except Exception as e:
        return None, f"요청 오류: {str(e)}"

# 네이버 검색광고 API - 키워드 통계 조회
def get_naver_keyword_stats(keywords):
    """
    네이버 검색광고 API - 키워드 도구 (월간 검색량, 경쟁도 등)
    
    Args:
        keywords: 검색어 리스트
    
    Returns:
        DataFrame with keyword statistics
    """
    if not naver_ad_api_key or not naver_ad_secret_key or not naver_customer_id:
        return None, "네이버 검색광고 API 키가 설정되지 않았습니다."
    
    import hashlib
    import hmac
    import base64
    
    url = "https://api.naver.com/keywordstool"
    timestamp = str(int(datetime.now().timestamp() * 1000))
    
    # 서명 생성
    message = f"{timestamp}.GET./keywordstool"
    signature = base64.b64encode(
        hmac.new(
            naver_ad_secret_key.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
    ).decode('utf-8')
    
    headers = {
        "X-API-KEY": naver_ad_api_key,
        "X-Customer": naver_customer_id,
        "X-Timestamp": timestamp,
        "X-Signature": signature,
        "Content-Type": "application/json"
    }
    
    body = {
        "keywordList": keywords,
        "showDetail": "1"
    }
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(body))
        
        if response.status_code == 200:
            data = response.json()
            
            results = []
            for item in data.get('keywordList', []):
                results.append({
                    '키워드': item.get('relKeyword', ''),
                    '월간검색수_PC': item.get('monthlyPcQcCnt', 0),
                    '월간검색수_모바일': item.get('monthlyMobileQcCnt', 0),
                    '월간검색수_합계': item.get('monthlyPcQcCnt', 0) + item.get('monthlyMobileQcCnt', 0),
                    '경쟁도': item.get('compIdx', 'N/A'),
                    '월평균클릭수': item.get('monthlyAvePcClkCnt', 0) + item.get('monthlyAveMobileClkCnt', 0),
                    '월평균클릭비용': item.get('monthlyAvePcClkCnt', 0) * item.get('plAvgDepth', 0)
                })
            
            df = pd.DataFrame(results)
            return df, None
        else:
            return None, f"API 오류: {response.status_code} - {response.text}"
    
    except Exception as e:
        return None, f"요청 오류: {str(e)}"


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
        # 네이버 검색량 질문 감지 (개선)
        naver_keywords_detected = (
            ("네이버" in prompt and ("검색량" in prompt or "검색" in prompt or "키워드" in prompt))
            or ("검색량" in prompt and "비교" in prompt and any(keyword in prompt for keyword in ["T50", "T80", "의자", "책상"]))
            or ("검색" in prompt and "순위" in prompt)
        )
        
        if naver_keywords_detected:
                # 키워드 추출 시도
                keywords = []
                if "T50" in prompt or "t50" in prompt:
                    keywords.append("T50")
                if "T80" in prompt or "t80" in prompt:
                    keywords.append("T80")
                if "의자" in prompt:
                    keywords.append("의자")
                if "책상" in prompt:
                    keywords.append("책상")
                
                # 키워드가 없으면 사용자에게 요청
                if not keywords:
                    st.info("🔍 **네이버 검색 분석**을 요청하셨습니다!")
                    
                    # 빠른 버튼 제공
                    st.markdown("### 💡 빠른 실행")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button("🔍 T50 vs T80 비교", key="quick_t50_t80"):
                            st.session_state['naver_api_type'] = 'keyword_stats'
                            st.session_state['naver_keywords'] = ['T50', 'T80']
                            st.session_state['show_naver_result'] = True
                            st.rerun()
                    
                    with col2:
                        if st.button("🔍 의자 키워드 분석", key="quick_chair"):
                            st.session_state['naver_api_type'] = 'keyword_stats'
                            st.session_state['naver_keywords'] = ['의자', '사무용의자', '게이밍의자']
                            st.session_state['show_naver_result'] = True
                            st.rerun()
                    
                    st.markdown("---")
                    st.markdown("### 📝 또는 직접 입력")
                    st.markdown("사이드바의 '🔍 네이버 검색 분석' 섹션에서:")
                    st.markdown("1. API 선택: 데이터랩(트렌드) 또는 검색광고(통계)")
                    st.markdown("2. 검색어 입력 (예: T50,T80,의자)")
                    st.markdown("3. 조회 버튼 클릭")
                else:
                    # 검색광고 API 우선 사용 (AI 불필요)
                    st.info(f"🔍 네이버 키워드 통계 조회: {', '.join(keywords)}")
                    
                    with st.spinner("키워드 통계 조회 중..."):
                        df, error = get_naver_keyword_stats(keywords)
                        
                        if error:
                            st.warning(f"⚠️ 검색광고 API: {error}")
                            st.info("💡 데이터랩 API로 대체 조회...")
                            
                            # 데이터랩 API로 대체
                            from datetime import datetime, timedelta
                            end_date = datetime.now()
                            start_date = end_date - timedelta(days=30)
                            
                            df, error = get_naver_search_trend(
                                keywords,
                                start_date.strftime('%Y-%m-%d'),
                                end_date.strftime('%Y-%m-%d'),
                                'date'
                            )
                            
                            if not error and df is not None and not df.empty:
                                # 트렌드 차트
                                fig = go.Figure()
                                
                                for keyword in keywords:
                                    keyword_data = df[df['키워드'] == keyword]
                                    fig.add_trace(go.Scatter(
                                        x=keyword_data['날짜'],
                                        y=keyword_data['검색량'],
                                        name=keyword,
                                        mode='lines+markers',
                                        line=dict(width=3)
                                    ))
                                
                                fig.update_layout(
                                    title='최근 30일 검색량 추이',
                                    xaxis=dict(title='날짜'),
                                    yaxis=dict(title='검색량'),
                                    height=400
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                        
                        elif df is not None and not df.empty:
                            # 키워드 통계 표시
                            st.markdown("#### 📊 키워드 분석 결과")
                            
                            # KPI 카드
                            cols = st.columns(len(df))
                            for i, row in df.iterrows():
                                if i < len(cols):
                                    with cols[i]:
                                        st.metric(
                                            row['키워드'],
                                            f"{row['월간검색수_합계']:,}",
                                            f"경쟁도 {row['경쟁도']}"
                                        )
                            
                            # 비교 차트
                            fig = go.Figure()
                            
                            fig.add_trace(go.Bar(
                                x=df['키워드'],
                                y=df['월간검색수_PC'],
                                name='PC',
                                marker_color='#1f77b4'
                            ))
                            
                            fig.add_trace(go.Bar(
                                x=df['키워드'],
                                y=df['월간검색수_모바일'],
                                name='모바일',
                                marker_color='#ff7f0e'
                            ))
                            
                            fig.update_layout(
                                title='월간 검색수 비교',
                                barmode='stack',
                                height=400
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # 상세 데이터
                            with st.expander("📋 상세 통계"):
                                st.dataframe(df, use_container_width=True)
                
                # 네이버 검색량 처리 완료 - 여기서 종료
                st.session_state.messages.append({"role": "assistant", "content": f"네이버 검색 분석: {', '.join(keywords) if keywords else '사이드바에서 검색어 입력 필요'}"})
                # 네이버 분석 완료 - BigQuery 분석하지 않음
        
        else:
            # 일반 데이터 분석 (BigQuery)
            try:
                
                # Gemini 사용 불가시 안내
                if not gemini_available:
                    st.warning("⚠️ AI 분석 기능을 사용할 수 없습니다.")
                    st.info("💡 **대신 이렇게 이용하세요:**")
                    st.markdown("1. **사이드바 버튼 사용**: 📅 사용자 추이 분석, 💰 매출 추이 분석, 🪑 T50 제품 종합 분석")
                    st.markdown("2. **네이버 검색 분석**: 사이드바 → 🔍 네이버 검색 분석")
                    st.markdown("3. **직접 SQL 작성**: BigQuery 콘솔에서 직접 쿼리 실행")
                    
                    st.session_state.messages.append({
                        "role": "assistant", 
                        "content": "AI 분석 기능을 사용할 수 없습니다. 사이드바의 빠른 분석 버튼을 이용해주세요."
                    })
                    
                else:
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
                        # 기간 정보는 한 번만 표시하므로 중복 제거
                    
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
                        
                        try:
                            response = model.generate_content(enhanced_prompt)
                            answer = response.text
                        except Exception as gemini_error:
                            error_str = str(gemini_error)
                            
                            # 429 오류 (할당량 초과) 감지
                            if "429" in error_str or "quota" in error_str.lower():
                                # retry_delay 추출
                                import re as re2
                                retry_match = re2.search(r'retry_delay.*?seconds:\s*(\d+)', error_str)
                                
                                if retry_match:
                                    retry_seconds = int(retry_match.group(1))
                                    
                                    st.error("⏱️ **Gemini API 할당량 초과**")
                                    st.warning(f"🕐 **{retry_seconds}초 후** 다시 시도하시거나, 아래 대체 방법을 이용하세요.")
                                    
                                    # 카운트다운 타이머
                                    if retry_seconds < 120:  # 2분 미만이면 자동 재시도 제안
                                        if st.button(f"⏳ {retry_seconds}초 후 자동 재시도"):
                                            import time
                                            progress_bar = st.progress(0)
                                            status_text = st.empty()
                                            
                                            for i in range(retry_seconds):
                                                remaining = retry_seconds - i
                                                progress = (i + 1) / retry_seconds
                                                progress_bar.progress(progress)
                                                status_text.text(f"⏳ 재시도까지 {remaining}초 남음...")
                                                time.sleep(1)
                                            
                                            st.rerun()
                                else:
                                    st.error("⏱️ **Gemini API 할당량 초과**")
                                    st.warning("잠시 후 다시 시도해주세요.")
                                
                                # 대체 방법 안내
                                st.info("💡 **지금 바로 사용 가능한 기능:**")
                                st.markdown("1. **📅 사용자 추이 분석** - 사이드바 버튼")
                                st.markdown("2. **💰 매출 추이 분석** - 사이드바 버튼")
                                st.markdown("3. **🪑 T50 제품 종합 분석** - 사이드바 버튼")
                                st.markdown("4. **🔍 네이버 검색 분석** - 사이드바 (AI 불필요)")
                                
                                st.markdown("---")
                                st.markdown("**⏰ 할당량 정보:**")
                                st.markdown("- Gemini API 무료 티어: 하루 20회")
                                st.markdown("- 현재 상태: 할당량 초과")
                                st.markdown("- [API 사용량 확인하기](https://ai.dev/rate-limit)")
                                
                            else:
                                # 기타 오류
                                st.error(f"❌ Gemini API 오류: {error_str[:200]}")
                            
                            # 메시지 저장
                            st.session_state.messages.append({
                                "role": "assistant",
                                "content": "AI 기능을 일시적으로 사용할 수 없습니다. 사이드바의 빠른 분석 버튼을 이용해주세요."
                            })
                            
                            # 예외 발생시 여기서 종료 (나머지 코드 실행 안함)
                            raise gemini_error
                    
                    # 인사이트 섹션 (간결하게)
                    st.markdown("### 💡 AI 분석 요약")
                    insight = re.sub(r"```sql.*?```", "", answer, flags=re.DOTALL)
                    # 인사이트를 간결하게 표시 (최대 300자)
                    short_insight = insight.strip()[:300] + "..." if len(insight.strip()) > 300 else insight.strip()
                    st.info(short_insight)
                    
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
                    
                    # SQL 쿼리 표시 (기본 접힌 상태)
                    with st.expander("🔍 생성된 SQL 쿼리 확인", expanded=False):
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
                        # 데이터를 날짜 순으로 정렬
                        date_columns = [col for col in df.columns if 'date' in col.lower() or col == 'event_date']
                        if date_columns:
                            df = df.sort_values(date_columns[0])
                        
                        # 컬럼명 한글화
                        column_rename = {
                            'event_date': '날짜',
                            'date': '날짜',
                            'users': '사용자',
                            'distinct_users': '사용자',
                            't50_users': 'T50 사용자',
                            't80_users': 'T80 사용자',
                            'purchases': '구매',
                            'page_views': '페이지뷰',
                            'revenue': '매출',
                            'quantity': '수량',
                            'sessions': '세션',
                            'conversion_rate': '전환율',
                            'gender': '성별',
                            'source': '유입경로',
                            'age': '연령'
                        }
                        df_display = df.rename(columns=column_rename)
                        
                        # 데이터 테이블 표시 (헤더 없이)
                        st.dataframe(df_display, use_container_width=True)
                        
                        # KPI 카드
                        if len(df) > 1:
                            st.markdown("---")
                            st.markdown("#### 핵심 지표")
                            
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                if 'users' in df.columns or 'distinct_users' in df.columns:
                                    user_col = 'users' if 'users' in df.columns else 'distinct_users'
                                    total_users = df[user_col].sum()
                                    st.metric("총 사용자", f"{total_users:,}")
                            
                            with col2:
                                if 'purchases' in df.columns:
                                    total_purchases = df['purchases'].sum()
                                    st.metric("총 구매", f"{total_purchases:,}건")
                            
                            with col3:
                                if 'revenue' in df.columns:
                                    total_revenue = df['revenue'].sum()
                                    st.metric("총 매출", f"₩{total_revenue:,.0f}")
                            
                            with col4:
                                user_col = 'users' if 'users' in df.columns else ('distinct_users' if 'distinct_users' in df.columns else None)
                                if user_col and 'purchases' in df.columns:
                                    total_users = df[user_col].sum()
                                    total_purchases = df['purchases'].sum()
                                    conversion = (total_purchases / total_users * 100) if total_users > 0 else 0
                                    st.metric("평균 전환율", f"{conversion:.1f}%")
                        
                        # 시각화 (데이터가 2개 이상일 때)
                        if len(df) > 1:
                            st.markdown("---")
                            st.markdown("### 📈 시각화")
                            
                            # 날짜 컬럼 찾기
                            date_col = None
                            for col in df.columns:
                                if 'date' in col.lower() or col == 'event_date':
                                    date_col = col
                                    break
                            
                            if date_col:
                                # 사용자 + 구매/페이지뷰 듀얼 차트
                                user_col = 'users' if 'users' in df.columns else ('distinct_users' if 'distinct_users' in df.columns else None)
                                
                                if user_col and ('purchases' in df.columns or 'page_views' in df.columns):
                                    fig = go.Figure()
                                    
                                    fig.add_trace(go.Scatter(
                                        x=df[date_col], 
                                        y=df[user_col],
                                        name='사용자',
                                        mode='lines+markers',
                                        line=dict(color='#1f77b4', width=3),
                                        marker=dict(size=8)
                                    ))
                                    
                                    # 구매 또는 페이지뷰 추가
                                    second_metric = 'purchases' if 'purchases' in df.columns else 'page_views'
                                    second_label = '구매' if second_metric == 'purchases' else '페이지뷰'
                                    
                                    fig.add_trace(go.Scatter(
                                        x=df[date_col], 
                                        y=df[second_metric],
                                        name=second_label,
                                        mode='lines+markers',
                                        line=dict(color='#ff7f0e', width=3),
                                        marker=dict(size=8),
                                        yaxis='y2'
                                    ))
                                    
                                    fig.update_layout(
                                        title=f'일별 사용자 및 {second_label} 추이',
                                        xaxis=dict(title='날짜'),
                                        yaxis=dict(title='사용자 수', side='left'),
                                        yaxis2=dict(title=f'{second_label} 수', overlaying='y', side='right'),
                                        hovermode='x unified',
                                        height=400,
                                        showlegend=True,
                                        legend=dict(x=0.01, y=0.99)
                                    )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                                
                                # 매출 차트
                                elif 'revenue' in df.columns:
                                    fig = go.Figure()
                                    
                                    fig.add_trace(go.Bar(
                                        x=df[date_col],
                                        y=df['revenue'],
                                        marker=dict(
                                            color=df['revenue'],
                                            colorscale='Blues',
                                            showscale=False
                                        ),
                                        text=df['revenue'].apply(lambda x: f'₩{x:,.0f}'),
                                        textposition='outside'
                                    ))
                                    
                                    fig.update_layout(
                                        title='일별 매출 추이',
                                        xaxis=dict(title='날짜'),
                                        yaxis=dict(title='매출 (₩)'),
                                        height=400
                                    )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                                
                                # 사용자 단일 차트
                                elif user_col:
                                    fig = px.area(df, x=date_col, y=user_col, 
                                                 title='일별 사용자 추이',
                                                 color_discrete_sequence=['#636EFA'])
                                    fig.update_traces(line=dict(width=3))
                                    fig.update_layout(
                                        height=400,
                                        xaxis_title='날짜',
                                        yaxis_title='사용자 수'
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                            
                            # 성별/유입경로 차트 (날짜가 없을 때)
                            else:
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    if 'gender' in df.columns:
                                        fig_gender = px.pie(df, names='gender', values='revenue' if 'revenue' in df.columns else df.columns[1],
                                                           title='성별 분포')
                                        st.plotly_chart(fig_gender, use_container_width=True)
                                
                                with col2:
                                    if 'source' in df.columns:
                                        fig_source = px.bar(df, x='source', y='revenue' if 'revenue' in df.columns else df.columns[1],
                                                           title='유입경로별 성과')
                                        st.plotly_chart(fig_source, use_container_width=True)
                        
                        # SQL 쿼리 표시
                        with st.expander("🔍 실행된 SQL 쿼리 보기"):
                            st.code(sql_query, language='sql')
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
    
    # 네이버 검색량 빠른 버튼
    if st.button("🔍 네이버 키워드 검색량"):
        if naver_client_id or naver_ad_api_key:
            # 키워드 입력 받기
            st.info("💡 사이드바 아래 '🔍 네이버 검색 분석' 섹션에서 검색어를 입력하세요.")
        else:
            st.error("⚠️ 네이버 API 키가 설정되지 않았습니다.")
            
            # 디버깅 정보
            with st.expander("🔍 설정 상태 확인"):
                st.write("Secrets 확인:")
                st.write(f"- 'naver' in secrets: {'naver' in st.secrets}")
                if 'naver' in st.secrets:
                    st.write(f"- client_id 존재: {'client_id' in st.secrets['naver']}")
                    st.write(f"- client_secret 존재: {'client_secret' in st.secrets['naver']}")
                    st.write(f"- ad_api_key 존재: {'ad_api_key' in st.secrets['naver']}")
                    if 'client_id' in st.secrets['naver']:
                        st.write(f"- client_id 값: {st.secrets['naver']['client_id'][:10]}...")
                else:
                    st.write("❌ 'naver' 섹션이 secrets에 없습니다.")
            
            st.info("**Secrets 설정 방법:**")
            st.markdown("1. Streamlit Cloud → 앱 선택")
            st.markdown("2. Settings → Secrets")
            st.markdown("3. 아래 형식으로 입력:")
            st.code("""[naver]
client_id = "YOUR_CLIENT_ID"
client_secret = "YOUR_CLIENT_SECRET"

# 검색광고 API (선택)
ad_api_key = "YOUR_AD_API_KEY"
ad_secret_key = "YOUR_AD_SECRET_KEY"
customer_id = "YOUR_CUSTOMER_ID"
            """)
            st.warning("⚠️ **주의:** 따옴표 사용 및 띄어쓰기 정확히 확인!")
    
    st.markdown("---")
    
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
  COUNTIF(event_name = 'page_view') as page_views,
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
    
    if st.button("🪑 T50 제품 종합 분석"):
        # 기간이 설정되지 않았으면 기본값 사용
        if 'start_date' not in st.session_state:
            from datetime import datetime, timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            st.session_state['start_date'] = start_date.strftime('%Y%m%d')
            st.session_state['end_date'] = end_date.strftime('%Y%m%d')
            st.session_state['period_label'] = "최근 30일 (기본)"
        
        st.session_state['show_product_analysis'] = True
        st.session_state['product_name'] = 'T50'
        st.rerun()
    
    st.markdown("---")
    st.markdown("#### 💬 질문 예시")
    st.markdown("""
    - **최근 7일 매출 분석해줘**
    - **작년 12월 데이터 보여줘**
    - **T50 구매 추이**
    """)
    
    # 네이버 검색량 분석
    if naver_client_id or naver_ad_api_key:
        st.markdown("---")
        st.markdown("### 🔍 네이버 검색 분석")
        
        # API 선택
        api_type = st.radio(
            "API 선택",
            ["데이터랩 (트렌드)", "검색광고 (키워드 통계)"],
            help="데이터랩: 시간별 검색량 추이 / 검색광고: 월간 검색량, 경쟁도 등"
        )
        
        # 검색어 입력
        keywords_input = st.text_input(
            "검색어 입력 (쉼표로 구분)",
            placeholder="예: T50,T80,의자"
        )
        
        if api_type == "데이터랩 (트렌드)":
            # 기간 선택
            col1, col2 = st.columns(2)
            with col1:
                search_start = st.date_input(
                    "시작일",
                    value=datetime.now() - timedelta(days=30),
                    key="naver_start"
                )
            with col2:
                search_end = st.date_input(
                    "종료일",
                    value=datetime.now(),
                    key="naver_end"
                )
            
            time_unit = st.selectbox(
                "집계 단위",
                ["date", "week", "month"],
                format_func=lambda x: {"date": "일별", "week": "주별", "month": "월별"}[x]
            )
            
            if st.button("🔍 트렌드 조회"):
                if keywords_input:
                    keywords = [k.strip() for k in keywords_input.split(",") if k.strip()][:5]
                    
                    st.session_state['naver_api_type'] = 'trend'
                    st.session_state['naver_keywords'] = keywords
                    st.session_state['naver_start'] = search_start.strftime('%Y-%m-%d')
                    st.session_state['naver_end'] = search_end.strftime('%Y-%m-%d')
                    st.session_state['naver_time_unit'] = time_unit
                    st.session_state['show_naver_result'] = True
                    st.rerun()
                else:
                    st.warning("검색어를 입력하세요!")
        
        else:  # 검색광고
            if st.button("📊 키워드 통계 조회"):
                if keywords_input:
                    keywords = [k.strip() for k in keywords_input.split(",") if k.strip()]
                    
                    st.session_state['naver_api_type'] = 'keyword_stats'
                    st.session_state['naver_keywords'] = keywords
                    st.session_state['show_naver_result'] = True
                    st.rerun()
                else:
                    st.warning("검색어를 입력하세요!")
    
    st.markdown("---")
    
    if st.button("🗑️ 대화 기록 초기화"):
        st.session_state.messages = []
        if 'quick_query' in st.session_state:
            del st.session_state['quick_query']
        if 'show_naver_result' in st.session_state:
            del st.session_state['show_naver_result']
        if 'show_product_analysis' in st.session_state:
            del st.session_state['show_product_analysis']
        st.rerun()

# 제품 종합 분석 대시보드
if 'show_product_analysis' in st.session_state and st.session_state['show_product_analysis']:
    product_name = st.session_state.get('product_name', 'T50')
    start_date = st.session_state['start_date']
    end_date = st.session_state['end_date']
    period_label = st.session_state.get('period_label', f"{start_date} ~ {end_date}")
    
    with st.chat_message("assistant"):
        st.markdown(f"### 🪑 {product_name} 제품 종합 분석")
        st.info(f"📅 분석 기간: {period_label}")
        
        with st.spinner("데이터 분석 중..."):
            try:
                # 1. 정확한 제품명 목록 추출
                product_query = f"""
SELECT DISTINCT
  item.item_name as product_name,
  COUNT(*) as event_count
FROM `{table_path}`,
  UNNEST(items) as item
WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
  AND item.item_name LIKE '%{product_name}%'
GROUP BY item.item_name
ORDER BY event_count DESC
LIMIT 10
"""
                product_df = client.query(product_query).to_dataframe()
                
                if product_df.empty:
                    st.warning(f"⚠️ '{product_name}' 관련 제품을 찾을 수 없습니다.")
                else:
                    # 제품 선택
                    st.markdown("#### 📦 검색된 제품")
                    selected_products = st.multiselect(
                        "분석할 제품 선택",
                        product_df['product_name'].tolist(),
                        default=product_df['product_name'].tolist()[:3]
                    )
                    
                    if not selected_products:
                        st.warning("최소 1개 이상의 제품을 선택하세요.")
                    else:
                        product_condition = " OR ".join([f"item.item_name = '{p}'" for p in selected_products])
                        
                        # 2. 종합 분석 쿼리
                        analysis_query = f"""
WITH product_events AS (
  SELECT
    user_pseudo_id,
    event_name,
    event_date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_url,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_id') as session_id,
    geo.country,
    device.category as device_category,
    traffic_source.source as traffic_source,
    traffic_source.medium as traffic_medium,
    ecommerce.purchase_revenue as revenue,
    item.item_name,
    item.quantity
  FROM `{table_path}`,
    UNNEST(items) as item
  WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
    AND ({product_condition})
)
SELECT
  -- 기본 지표
  COUNT(DISTINCT user_pseudo_id) as total_visitors,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) as total_buyers,
  COUNTIF(event_name = 'purchase') as total_purchases,
  
  -- 매출 지표
  SUM(CASE WHEN event_name = 'purchase' THEN revenue END) as total_revenue,
  AVG(CASE WHEN event_name = 'purchase' THEN revenue END) as avg_order_value,
  SUM(CASE WHEN event_name = 'purchase' THEN quantity END) as total_quantity,
  AVG(CASE WHEN event_name = 'purchase' THEN quantity END) as avg_quantity_per_order,
  
  -- 전환율
  ROUND(COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2) as conversion_rate,
  
  -- 디바이스
  COUNTIF(device_category = 'mobile') as mobile_users,
  COUNTIF(device_category = 'desktop') as desktop_users,
  COUNTIF(device_category = 'tablet') as tablet_users
FROM product_events
"""
                        
                        main_df = client.query(analysis_query).to_dataframe()
                        
                        if not main_df.empty:
                            row = main_df.iloc[0]
                            
                            # KPI 카드
                            st.markdown("---")
                            st.markdown("#### 📊 핵심 지표")
                            
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric(
                                    "페이지 방문자",
                                    f"{int(row['total_visitors']):,}명",
                                    help="제품 페이지를 방문한 순 사용자 수"
                                )
                            
                            with col2:
                                st.metric(
                                    "구매자",
                                    f"{int(row['total_buyers']):,}명",
                                    help="실제로 구매한 순 사용자 수"
                                )
                            
                            with col3:
                                st.metric(
                                    "구매 건수",
                                    f"{int(row['total_purchases']):,}건",
                                    help="총 구매 트랜잭션 수 (중복 구매 포함)"
                                )
                            
                            with col4:
                                st.metric(
                                    "전환율",
                                    f"{row['conversion_rate']:.2f}%",
                                    help="구매자 수 / 방문자 수"
                                )
                            
                            col5, col6, col7, col8 = st.columns(4)
                            
                            with col5:
                                st.metric(
                                    "총 매출",
                                    f"₩{int(row['total_revenue']):,}" if pd.notna(row['total_revenue']) else "N/A"
                                )
                            
                            with col6:
                                st.metric(
                                    "평균 주문 금액",
                                    f"₩{int(row['avg_order_value']):,}" if pd.notna(row['avg_order_value']) else "N/A"
                                )
                            
                            with col7:
                                st.metric(
                                    "총 판매 수량",
                                    f"{int(row['total_quantity']):,}개" if pd.notna(row['total_quantity']) else "N/A"
                                )
                            
                            with col8:
                                st.metric(
                                    "평균 구매 수량",
                                    f"{row['avg_quantity_per_order']:.1f}개" if pd.notna(row['avg_quantity_per_order']) else "N/A"
                                )
                            
                            st.markdown("---")
                            
                            # 상세 분석
                            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                                "👥 인구통계", "🌐 유입경로", "💰 매출분석", "📱 이용행태", "📈 전환율"
                            ])
                            
                            with tab1:
                                st.markdown("#### 👥 인구통계학적 정보")
                                # 디바이스 분포
                                device_data = {
                                    '디바이스': ['모바일', '데스크톱', '태블릿'],
                                    '사용자수': [
                                        int(row['mobile_users']),
                                        int(row['desktop_users']),
                                        int(row['tablet_users'])
                                    ]
                                }
                                device_df = pd.DataFrame(device_data)
                                
                                fig_device = px.pie(device_df, names='디바이스', values='사용자수', 
                                                   title='디바이스별 사용자 분포')
                                st.plotly_chart(fig_device, use_container_width=True)
                            
                            with tab2:
                                st.markdown("#### 🌐 유입 경로 분석")
                                
                                traffic_query = f"""
SELECT
  traffic_source.source as source,
  traffic_source.medium as medium,
  COUNT(DISTINCT user_pseudo_id) as users,
  COUNTIF(event_name = 'purchase') as purchases
FROM `{table_path}`,
  UNNEST(items) as item
WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
  AND ({product_condition})
GROUP BY source, medium
ORDER BY users DESC
LIMIT 10
"""
                                traffic_df = client.query(traffic_query).to_dataframe()
                                
                                if not traffic_df.empty:
                                    traffic_df['유입경로'] = traffic_df['source'] + ' / ' + traffic_df['medium']
                                    
                                    fig_traffic = px.bar(traffic_df, x='유입경로', y='users',
                                                        title='유입 경로별 방문자 수',
                                                        labels={'users': '방문자 수'})
                                    st.plotly_chart(fig_traffic, use_container_width=True)
                                    
                                    st.dataframe(traffic_df, use_container_width=True)
                            
                            with tab3:
                                st.markdown("#### 💰 매출 및 구매 분석")
                                
                                # 일별 매출 추이
                                daily_query = f"""
SELECT
  event_date,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) as buyers,
  COUNTIF(event_name = 'purchase') as purchases,
  SUM(CASE WHEN event_name = 'purchase' THEN ecommerce.purchase_revenue END) as revenue
FROM `{table_path}`,
  UNNEST(items) as item
WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
  AND ({product_condition})
GROUP BY event_date
ORDER BY event_date
"""
                                daily_df = client.query(daily_query).to_dataframe()
                                
                                if not daily_df.empty:
                                    daily_df['날짜'] = pd.to_datetime(daily_df['event_date'])
                                    
                                    fig_daily = go.Figure()
                                    fig_daily.add_trace(go.Scatter(
                                        x=daily_df['날짜'],
                                        y=daily_df['revenue'],
                                        name='매출',
                                        line=dict(color='#1f77b4', width=3)
                                    ))
                                    fig_daily.update_layout(
                                        title='일별 매출 추이',
                                        xaxis_title='날짜',
                                        yaxis_title='매출 (₩)',
                                        height=400
                                    )
                                    st.plotly_chart(fig_daily, use_container_width=True)
                            
                            with tab4:
                                st.markdown("#### 📱 서비스 이용 행태")
                                
                                # 제품별 상세
                                product_detail_df = pd.DataFrame({
                                    '제품명': selected_products,
                                    '선택됨': ['✓'] * len(selected_products)
                                })
                                st.dataframe(product_detail_df, use_container_width=True)
                                
                                st.info("💡 페이지 탐색 퍼널 및 제품 비교 데이터는 추가 event_params 분석이 필요합니다.")
                            
                            with tab5:
                                st.markdown("#### 📈 전환율 비교")
                                
                                # 전체 평균 전환율
                                avg_conversion_query = f"""
SELECT
  ROUND(COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2) as avg_conversion
FROM `{table_path}`
WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
"""
                                avg_df = client.query(avg_conversion_query).to_dataframe()
                                
                                if not avg_df.empty:
                                    avg_conversion = avg_df.iloc[0]['avg_conversion']
                                    product_conversion = row['conversion_rate']
                                    
                                    comparison_df = pd.DataFrame({
                                        '구분': [f'{product_name} 제품', '전체 평균'],
                                        '전환율': [product_conversion, avg_conversion]
                                    })
                                    
                                    fig_comparison = px.bar(comparison_df, x='구분', y='전환율',
                                                           title='전환율 비교',
                                                           text='전환율',
                                                           color='구분')
                                    fig_comparison.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
                                    st.plotly_chart(fig_comparison, use_container_width=True)
                                    
                                    if product_conversion > avg_conversion:
                                        st.success(f"✅ {product_name} 제품의 전환율이 평균보다 {product_conversion - avg_conversion:.2f}%p 높습니다!")
                                    else:
                                        st.warning(f"⚠️ {product_name} 제품의 전환율이 평균보다 {avg_conversion - product_conversion:.2f}%p 낮습니다.")
                            
                            # 종합 인사이트
                            st.markdown("---")
                            st.markdown("### 💡 AI 인사이트 요약")
                            
                            insights = f"""
**{product_name} 제품 분석 요약** ({period_label})

**핵심 지표:**
- 총 방문자: {int(row['total_visitors']):,}명
- 구매자: {int(row['total_buyers']):,}명 (전환율 {row['conversion_rate']:.2f}%)
- 총 구매 건수: {int(row['total_purchases']):,}건
- 총 매출: ₩{int(row['total_revenue']):,} (평균 주문금액 ₩{int(row['avg_order_value']):,})

**주요 발견사항:**
1. **인구통계:** 모바일 사용자가 {int(row['mobile_users'] / row['total_visitors'] * 100)}% 를 차지
2. **구매 행태:** 평균 {row['avg_quantity_per_order']:.1f}개 구매
3. **전환율:** 전체 평균 대비 {'높은' if row['conversion_rate'] > avg_conversion else '낮은'} 전환율

**제안사항:**
- 모바일 최적화 {'강화' if row['mobile_users'] > row['desktop_users'] else '필요'}
- 전환율 개선을 위한 {'장바구니 이탈 방지' if row['conversion_rate'] < avg_conversion else 'VIP 고객 관리'} 전략 수립
"""
                            st.info(insights)
                        
                        else:
                            st.warning("분석 데이터가 없습니다.")
                        
            except Exception as e:
                st.error(f"❌ 분석 오류: {str(e)}")
                import traceback
                with st.expander("상세 오류"):
                    st.code(traceback.format_exc())
        
        # 분석 완료 후 플래그 제거
        del st.session_state['show_product_analysis']

# 네이버 검색량 결과 표시
if 'show_naver_result' in st.session_state and st.session_state['show_naver_result']:
    with st.chat_message("assistant"):
        api_type = st.session_state.get('naver_api_type', 'trend')
        keywords = st.session_state['naver_keywords']
        
        if api_type == 'keyword_stats':
            # 검색광고 API - 키워드 통계
            st.markdown("### 📊 네이버 키워드 통계")
            st.info(f"🔍 키워드: {', '.join(keywords)}")
            
            with st.spinner("키워드 통계 조회 중..."):
                df, error = get_naver_keyword_stats(keywords)
                
                if error:
                    st.error(f"❌ {error}")
                    st.markdown("**Secrets 설정이 필요합니다:**")
                    st.code("""
[naver]
ad_api_key = "your_api_key"
ad_secret_key = "your_secret_key"
customer_id = "your_customer_id"
                    """)
                elif df is not None and not df.empty:
                    # KPI 카드
                    st.markdown("#### 핵심 지표")
                    
                    cols = st.columns(len(df))
                    for i, row in df.iterrows():
                        if i < len(cols):
                            with cols[i]:
                                st.metric(
                                    row['키워드'],
                                    f"{row['월간검색수_합계']:,}",
                                    f"경쟁도 {row['경쟁도']}"
                                )
                    
                    st.markdown("---")
                    
                    # 월간 검색수 비교 차트
                    fig = go.Figure()
                    
                    fig.add_trace(go.Bar(
                        x=df['키워드'],
                        y=df['월간검색수_PC'],
                        name='PC',
                        marker_color='#1f77b4'
                    ))
                    
                    fig.add_trace(go.Bar(
                        x=df['키워드'],
                        y=df['월간검색수_모바일'],
                        name='모바일',
                        marker_color='#ff7f0e'
                    ))
                    
                    fig.update_layout(
                        title='월간 검색수 비교 (PC vs 모바일)',
                        xaxis=dict(title='키워드'),
                        yaxis=dict(title='검색수'),
                        barmode='stack',
                        height=400
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # 상세 데이터
                    with st.expander("📋 상세 통계 보기"):
                        st.dataframe(df, use_container_width=True)
                    
                    st.success("✅ 키워드 통계 조회 완료!")
                else:
                    st.warning("데이터가 없습니다.")
        
        else:
            # 데이터랩 API - 트렌드
            start_date = st.session_state['naver_start']
            end_date = st.session_state['naver_end']
            time_unit = st.session_state['naver_time_unit']
            
            st.markdown("### 🔍 네이버 검색량 추이")
            st.info(f"📅 분석 기간: {start_date} ~ {end_date} | 키워드: {', '.join(keywords)}")
            
            with st.spinner("네이버 검색량 조회 중..."):
                df, error = get_naver_search_trend(keywords, start_date, end_date, time_unit)
                
                if error:
                    st.error(f"❌ {error}")
                elif df is not None and not df.empty:
                    # KPI 카드
                    st.markdown("#### 핵심 지표")
                    cols = st.columns(len(keywords))
                    
                    for i, keyword in enumerate(keywords):
                        with cols[i]:
                            keyword_data = df[df['키워드'] == keyword]
                            if not keyword_data.empty:
                                avg_search = keyword_data['검색량'].mean()
                                max_search = keyword_data['검색량'].max()
                                st.metric(
                                    keyword,
                                    f"{avg_search:.1f}",
                                    f"최대 {max_search:.1f}"
                                )
                    
                    st.markdown("---")
                    
                    # 검색량 추이 차트
                    fig = go.Figure()
                    
                    for keyword in keywords:
                        keyword_data = df[df['키워드'] == keyword]
                        fig.add_trace(go.Scatter(
                            x=keyword_data['날짜'],
                            y=keyword_data['검색량'],
                            name=keyword,
                            mode='lines+markers',
                            line=dict(width=3),
                            marker=dict(size=6)
                        ))
                    
                    fig.update_layout(
                        title='검색량 추이 비교',
                        xaxis=dict(title='날짜'),
                        yaxis=dict(title='검색량 (상대값)'),
                        hovermode='x unified',
                        height=450,
                        showlegend=True,
                        legend=dict(x=0.01, y=0.99)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # 상세 데이터
                    with st.expander("📋 상세 데이터 보기"):
                        # Pivot 테이블로 변환
                        pivot_df = df.pivot(index='날짜', columns='키워드', values='검색량')
                        st.dataframe(pivot_df, use_container_width=True)
                    
                    st.success("✅ 네이버 검색량 조회 완료!")
                else:
                    st.warning("데이터가 없습니다.")
        
        # 결과 표시 후 플래그 제거
        del st.session_state['show_naver_result']


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
