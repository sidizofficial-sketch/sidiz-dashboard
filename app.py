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

# 2. 시디즈 데이터 엔진 (명세서 기반 가이드)
SIDIZ_ENGINE = {
    "METRICS": {
        "구매전환율(CVR)": "(count(purchase) / count(session_start)) * 100",
        "AOV": "sum(value) / count(purchase)"
    },
    "USER_PROPS": ["gender", "age", "login_status"],
    "IMPORTANT_NOTE": "상품 구매 정보는 items 배열의 item_name을 참조하세요."
}

# 3. 보안 및 모델 설정
try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")

    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        target_model = next((m for m in models if "1.5-flash" in m), models[0])
        model = genai.GenerativeModel(target_model)
        st.sidebar.success(f"✅ 엔진 가동 중: {target_model}")

    project_id = info['project_id']
    dataset_id = "analytics_487246344"

    # AI에게 대시보드 구성을 위한 페르소나 부여
    INSTRUCTION = f"""
    당신은 SIDIZ 데이터 분석가입니다. 사용자의 질문에 대해:
    1. 반드시 ```sql ... ``` 블록에 BigQuery SQL을 포함하세요. (테이블: {project_id}.{dataset_id}.events_*)
    2. SQL 결과 데이터를 기반으로 비즈니스 인사이트를 3줄 요약해서 설명하세요.
    3. 상품명 필터링 시 items.item_name을 UNNEST해서 사용하세요.
    """

except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 4. UI 구성
st.title("🪑 SIDIZ AI Data Dashboard")
st.caption("데이터를 기반으로 실시간 시각화 및 인사이트 리포트를 생성합니다.")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 5. 실행 로직
if prompt := st.chat_input("질문을 입력하세요 (예: T50 구매자 특징 분석해줘)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("AI 분석 및 대시보드 구성 중..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                
                # 줄글 설명과 SQL 분리 출력
                st.markdown("### 💡 AI 인사이트 요약")
                st.markdown(re.sub(r"```sql.*?```", "", answer, flags=re.DOTALL))

            # SQL 추출 및 실행
            sql_match = re.search(r"```sql\s*(.*?)\s*```", answer, re.DOTALL | re.IGNORECASE)
            if sql_match:
                query = sql_match.group(1).strip()
                df = client.query(query).to_dataframe()
                
                if not df.empty:
                    st.divider()
                    st.subheader("📊 데이터 시각화 리포트")
                    
                    # 지표 카드 (첫 번째 숫자형 데이터 활용)
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    if not numeric_cols.empty:
                        cols = st.columns(len(numeric_cols[:3]))
                        for i, col_name in enumerate(numeric_cols[:3]):
                            with cols[i]:
                                total_val = df[col_name].sum()
                                st.metric(label=col_name, value=f"{total_val:,.0f}")

                    # 차트 대시보드
                    tab1, tab2 = st.tabs(["주요 시각화", "상세 데이터"])
                    
                    with tab1:
                        # 데이터 형태에 따른 자동 차트 생성
                        if len(df.columns) >= 2:
                            c1, c2 = st.columns(2)
                            with c1:
                                fig1 = px.pie(df, names=df.columns[0], values=df.columns[-1], title="항목별 비중")
                                st.plotly_chart(fig1, use_container_width=True)
                            with c2:
                                fig2 = px.bar(df, x=df.columns[0], y=df.columns[-1], color=df.columns[0], title="항목별 비교")
                                st.plotly_chart(fig2, use_container_width=True)
                    
                    with tab2:
                        st.dataframe(df, use_container_width=True)
                        st.code(query, language="sql") # 개발자용 쿼리 확인

            st.session_state.messages.append({"role": "assistant", "content": answer})

        except Exception as e:
            st.error(f"오류 발생: {e}")
