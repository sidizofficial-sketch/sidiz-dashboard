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

# 2. 시디즈 전용 데이터 맵핑 엔진
SIDIZ_ENGINE = {
    "METRICS": {
        "구매전환율(CVR)": "(count(purchase) / count(session_start)) * 100",
        "B2B수주율": "(수주완료건수 / submit_business_inquiry) * 100",
        "평균주문금액(AOV)": "sum(value) / count(purchase)"
    },
    "EVENT_SPECS": {
        "submit_business_inquiry": {"desc": "B2B 대량구매 문의", "params": ["business_info", "ce_item_name", "expected_quantity"]},
        "register_warranty": {"desc": "정품 등록", "params": ["ce_item_id", "ce_item_name"]},
        "view_item": {"desc": "제품 상세 조회", "params": ["item_id", "item_name"]},
        "purchase": {"desc": "결제 완료", "params": ["transaction_id", "value", "item_name"]}
    }
}

# 3. 보안 및 모델 설정 (들여쓰기 오류 수정 완료)
try:
    # Secrets 로드 및 BigQuery 클라이언트 설정
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")

    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        
        # 사용 가능한 모델 리스트 확인 및 가용 모델 선택
        model_list = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        
        if 'models/gemini-1.5-flash' in model_list:
            model_name = 'models/gemini-1.5-flash'
        elif 'models/gemini-1.5-pro' in model_list:
            model_name = 'models/gemini-1.5-pro'
        else:
            model_name = 'gemini-pro'
            
        model = genai.GenerativeModel(model_name)
        st.sidebar.success(f"✅ 엔진 연결 완료: {model_name}")

    today = datetime.date.today().strftime('%Y%m%d')
    project_id = info['project_id']
    dataset_id = "analytics_487246344"

    INSTRUCTION = f"""
    당신은 시디즈(SIDIZ)의 데이터 분석 전문가입니다.
    - 프로젝트: {project_id}, 데이터셋: {dataset_id}
    - 명세: {SIDIZ_ENGINE}
    - 규칙: SQL은 반드시 ```sql ... ``` 블록 안에 작성하세요.
    """

except Exception as e:
    st.error(f"초기 설정 오류 (들여쓰기나 보안 키를 확인하세요): {e}")
    st.stop()

# 4. UI 구성
st.title("🪑 SIDIZ Data Intelligence")
st.caption("시디즈 GA4 데이터 명세서 기반 AI 대시보드")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 5. 질문 처리 및 실행
if prompt := st.chat_input("질문을 입력하세요"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("AI가 쿼리를 생성하고 있습니다..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                st.markdown(answer)

            # SQL 추출 로직 (정규표현식)
            sql_match = re.search(r"```sql\s*(.*?)\s*```", answer, re.DOTALL | re.IGNORECASE)
            if sql_match:
                query = sql_match.group(1).strip()
                with st.spinner("💾 BigQuery 조회 중..."):
                    df = client.query(query).to_dataframe()
                
                if not df.empty:
                    st.markdown("### 📊 조회 결과")
                    st.dataframe(df, use_container_width=True)
                    if len(df.columns) >= 2:
                        st.plotly_chart(px.bar(df, x=df.columns[0], y=df.columns[1], color_discrete_sequence=['#FF4B4B']))
                else:
                    st.warning("데이터가 없습니다.")

            st.session_state.messages.append({"role": "assistant", "content": answer})

        except Exception as e:
            st.error(f"실행 오류: {e}")
