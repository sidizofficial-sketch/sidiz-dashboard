import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime
import re

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI", page_icon="🪑", layout="wide")

# 2. 보안 및 모델 설정
try:
    # Secrets에서 GCP 정보 및 Gemini API 키 로드
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    
    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        
        # 사용 가능한 모델 목록 스캔 및 할당
        available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        target_model = 'models/gemini-1.5-flash' 
        if target_model not in available_models:
             target_model = available_models[0] if available_models else 'gemini-pro'

        model = genai.GenerativeModel(target_model)
        st.sidebar.success(f"✅ 엔진 연결 완료: {target_model}")

    # 분석을 위한 기본 정보 설정
    today = datetime.date.today().strftime('%Y%m%d')
    project_id = info['project_id']
    dataset_id = "analytics_324424314"
    
    INSTRUCTION = f"""
    당신은 시디즈(SIDIZ)의 데이터 분석 전문가입니다. 
    Google Analytics 4(GA4) BigQuery 데이터를 기반으로 사용자의 질문에 답하세요.
    
    [환경 정보]
    - 프로젝트 ID: {project_id}
    - 데이터셋: {dataset_id}
    - 테이블 형식: events_YYYYMMDD
    - 오늘 날짜: {today}
    
    [답변 규칙]
    1. 질문을 해결할 수 있는 SQL을 반드시 포함하세요. 
    2. SQL은 반드시 ```sql ... ``` 블록 안에 작성하세요.
    3. 테이블명은 반드시 `{project_id}.{dataset_id}.events_YYYYMMDD` 형식을 지키세요.
    """

except Exception as e:
    st.error(f"초기 설정 오류: {e}")
    st.stop()

# 3. UI 구성
st.title("🪑 SIDIZ Data Intelligence")
st.markdown("---")

if "messages" not in st.session_state:
    st.session_state.messages = []

# 대화 기록 표시
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 4. 질문 처리 및 데이터 실행
if prompt := st.chat_input("데이터에게 궁금한 점을 물어보세요 (예: 어제 유입수 얼마야?)"):
    # 유저 메시지 표시
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 비서 메시지 생성
    with st.chat_message("assistant"):
        try:
            with st.spinner("AI가 분석 쿼리를 생성 중입니다..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                st.markdown(answer)
            
            # 답변에서 SQL 추출 시도
            sql_match = re.search(r"```sql\n(.*?)```", answer, re.DOTALL)
            if not sql_match:
                sql_match = re.search(r"```\n(.*?)```", answer, re.DOTALL)
            
            if sql_match:
                query = sql_match.group(1).strip()
                with st.spinner("💾 BigQuery에서 실제 데이터를 조회하는 중..."):
                    # 쿼리 실행 및 데이터프레임 변환
                    df = client.query(query).to_dataframe()
                    
                    st.markdown("### 📊 데이터 조회 결과")
                    st.dataframe(df, use_container_width=True)
                    
                    # 단일 수치 데이터일 경우 강조 표시 (Metric)
                    if not df.empty and len(df.columns) == 1 and len(df) == 1:
                        label_name = df.columns[0]
                        value = df.iloc[0, 0]
                        st.metric(label=label_name, value=f"{value:,}")

            st.session_state.messages.append({"role": "assistant", "content": answer})
            
        except Exception as e:
            st.error(f"데이터 조회 중 오류가 발생했습니다: {e}")
