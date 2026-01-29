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

# 2. [핵심] 시디즈 전용 데이터 맵핑 엔진 (Knowledge Base)
# 엑셀의 33개 시트 핵심 내용을 함축했습니다.
SIDIZ_ENGINE = {
    "METRICS": {
        "구매전환율(CVR)": "(count(purchase) / count(session_start)) * 100",
        "B2B수주율": "(수주완료건수 / submit_business_inquiry) * 100",
        "평균주문금액(AOV)": "sum(value) / count(purchase)",
        "조회대비구매율": "(count(purchase) / count(view_item)) * 100"
    },
    "EVENT_SPECS": {
        "submit_business_inquiry": {"desc": "B2B 대량구매 문의", "params": ["business_info", "ce_item_name", "expected_quantity"]},
        "register_warranty": {"desc": "정품 등록", "params": ["ce_item_id", "ce_item_name", "ce_item_category"]},
        "quiz_results": {"desc": "의자 찾기 결과", "params": ["product_name1", "product_name2", "product_name3"]},
        "write_review": {"desc": "리뷰 작성", "params": ["ce_item_name", "review_ratings", "review_type"]},
        "click_banner": {"desc": "배너 클릭", "params": ["click_type", "click_text", "click_url"]},
        "view_item": {"desc": "제품 상세 조회", "params": ["item_id", "item_name", "item_category"]},
        "purchase": {"desc": "결제 완료", "params": ["transaction_id", "value", "item_name", "payment_type"]}
    },
    "USER_PROPERTIES": ["gender", "age", "login_status", "total_purchase_count", "method"]
}

# 3. 보안 및 모델 설정
try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")

    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        model = genai.GenerativeModel('gemini-1.5-flash')
        st.sidebar.success("✅ SIDIZ AI 엔진 연결 완료")

    today = datetime.date.today().strftime('%Y%m%d')
    project_id = info['project_id']
    dataset_id = "analytics_487246344"

    # AI에게 주입할 정교한 페르소나와 명세 지침
    INSTRUCTION = f"""
    당신은 시디즈(SIDIZ)의 데이터 분석 전문가입니다.
    다음 [시디즈 전용 데이터 명세]를 숙지하고 SQL을 작성하세요.

    [시디즈 전용 데이터 명세]
    - 지표 공식: {SIDIZ_ENGINE['METRICS']}
    - 핵심 이벤트 및 파라미터: {SIDIZ_ENGINE['EVENT_SPECS']}
    - 유저 속성: {SIDIZ_ENGINE['USER_PROPERTIES']}

    [SQL 작성 규칙]
    1. 테이블: {project_id}.{dataset_id}.events_YYYYMMDD (오늘 날짜: {today})
    2. 파라미터 추출: (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'key_name') 형식을 사용하세요.
    3. 숫자인 파라미터(예: expected_quantity)는 CAST(... AS INT64)를 잊지 마세요.
    4. 분석 기간은 사용자의 별도 요청이 없으면 최근 7일로 설정하세요.
    """

except Exception as e:
    st.error(f"초기 설정 오류: {e}")
    st.stop()

# 4. UI 구성
st.title("🪑 SIDIZ Data Intelligence")
st.caption("시디즈 GA4 명세서(V1.3) 로직이 적용된 AI 분석 도구입니다.")
st.markdown("---")

# 대화 기록 관리
if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 5. 질문 처리 및 데이터 실행
if prompt := st.chat_input("질문 예: 어제 B2B 문의에서 가장 인기 있었던 모델과 예상 수량 합계는?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("시디즈 명세서를 분석하여 쿼리를 생성 중..."):
                full_prompt = f"{INSTRUCTION}\n\n사용자 질문: {prompt}"
                response = model.generate_content(full_prompt)
                answer = response.text
                st.markdown(answer)

            # SQL 추출 (```sql ... ``` 또는 ``` ... ``` 블록 대응)
            sql_match = re.search(r"```sql\s*(.*?)\s*```", answer, re.DOTALL | re.IGNORECASE)
            if not sql_match:
                sql_match = re.search(r"```\s*(.*?)\s*```", answer, re.DOTALL | re.IGNORECASE)

            if sql_match:
                query = sql_match.group(1).strip()
                with st.spinner("💾 BigQuery 데이터 조회 중..."):
                    df = client.query(query).to_dataframe()

                if not df.empty:
                    st.markdown("### 📊 분석 결과 데이터")
                    st.dataframe(df, use_container_width=True)

                    # 시각화 추가 (컬럼이 2개 이상일 때 자동 그래프)
                    if len(df.columns) >= 2:
                        fig = px.bar(df, x=df.columns[0], y=df.columns[1], 
                                     title=f"'{df.columns[0]}' 기준 분석 리포트",
                                     color_discrete_sequence=['#FF4B4B'])
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("조회된 데이터가 없습니다. 날짜나 필터를 확인해 주세요.")

            st.session_state.messages.append({"role": "assistant", "content": answer})

        except Exception as e:
            st.error(f"실행 중 오류 발생: {e}")
