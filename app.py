import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime
import re
import plotly.express as px

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Intelligence", page_icon="🪑", layout="wide")

# 2. 시디즈 데이터 엔진 (명세서 기반)
SIDIZ_ENGINE = {
    "METRICS": {
        "구매전환율(CVR)": "(count(purchase) / count(session_start)) * 100",
        "B2B수주율": "(수주건수 / submit_business_inquiry) * 100",
        "평균주문금액(AOV)": "sum(value) / count(purchase)"
    },
    "EVENT_SPECS": {
        "submit_business_inquiry": {"desc": "B2B 대량구매 문의", "params": ["business_info", "ce_item_name", "expected_quantity"]},
        "register_warranty": {"desc": "정품 등록", "params": ["ce_item_id", "ce_item_name"]},
        "purchase": {"desc": "결제 완료", "params": ["transaction_id", "value", "item_name"]}
    }
}

# 3. 보안 및 모델 설정 (404 오류 방지 로직)
try:
    # GCP BigQuery 설정
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")

    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        
        # [해결책] 사용 가능한 모델 리스트를 실제로 가져와서 첫 번째 모델을 사용합니다.
        # 이렇게 하면 모델명이 바뀌어도 404 에러가 나지 않습니다.
        models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        
        if not models:
            st.error("사용 가능한 Gemini 모델이 없습니다. API 키 권한을 확인하세요.")
            st.stop()
            
        # 가장 성능이 좋은 1.5-flash를 우선 찾고, 없으면 리스트의 첫 모델 사용
        target_model = next((m for m in models if "1.5-flash" in m), models[0])
        model = genai.GenerativeModel(target_model)
        st.sidebar.success(f"✅ 엔진 가동 중: {target_model}")

    today = datetime.date.today().strftime('%Y%m%d')
    project_id = info['project_id']
    dataset_id = "analytics_487246344"

    INSTRUCTION = f"""
    당신은 SIDIZ GA4 전문가입니다.
    1. 테이블: {project_id}.{dataset_id}.events_*
    2. 명세: {SIDIZ_ENGINE}
    3. 반드시 ```sql ... ``` 형식으로 SQL을 제공하세요.
    """

except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 4. UI 및 챗봇 로직
st.title("🪑 SIDIZ AI Dashboard")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

if prompt := st.chat_input("데이터에게 질문하세요"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("분석 중..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                st.markdown(answer)

            # SQL 추출 및 실행
            sql_match = re.search(r"```sql\s*(.*?)\s*```", answer, re.DOTALL | re.IGNORECASE)
            if sql_match:
                query = sql_match.group(1).strip()
                df = client.query(query).to_dataframe()
                
                if not df.empty:
                    st.dataframe(df, use_container_width=True)
                    if len(df.columns) >= 2:
                        st.plotly_chart(px.bar(df, x=df.columns[0], y=df.columns[1]))
            
            st.session_state.messages.append({"role": "assistant", "content": answer})
        except Exception as e:
            st.error(f"오류 발생: {e}")
