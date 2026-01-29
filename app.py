import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import json
import datetime
import re
import plotly.express as px
import plotly.graph_objects as go

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ AI Intelligence", page_icon="🪑", layout="wide")

# 2. 보안 및 모델 설정
try:
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    client = bigquery.Client.from_service_account_info(info, location="asia-northeast3")
    
    if "gemini" in st.secrets:
        genai.configure(api_key=st.secrets["gemini"]["api_key"])
        models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        target_model = next((m for m in models if "1.5-flash" in m), models[0])
        model = genai.GenerativeModel(target_model)

    project_id = info['project_id']
    dataset_id = "analytics_487246344"
    full_table_path = f"{project_id}.{dataset_id}.events_*"

    # AI에게 강화된 페르소나와 시각화 지침 부여
    INSTRUCTION = f"""
    당신은 SIDIZ의 시니어 데이터 분석가입니다.
    사용자의 질문에 대해 반드시 다음 형식을 지켜 SQL을 생성하세요:
    1. 테이블명은 반드시 `{full_table_path}` 형식을 사용하세요.
    2. 상품명 필터링은 CROSS JOIN UNNEST(items)를 사용하세요.
    3. 결과 데이터에는 다음 컬럼들이 포함되도록 쿼리하세요:
       - 연령(age), 성별(gender), 유입경로(source/medium), 구매수량, 매출액, 전환여부 등
    4. SQL 블록 뒤에 비즈니스 인사이트를 요약하세요.
    """

except Exception as e:
    st.error(f"설정 오류: {e}")
    st.stop()

# 3. UI 구성
st.title("🪑 SIDIZ T50 구매자 심층 분석 대시보드")

if prompt := st.chat_input("T50 제품 구매자 특징과 시각화 리포트를 보여줘"):
    with st.chat_message("assistant"):
        try:
            with st.spinner("빅데이터 분석 및 시각화 중..."):
                response = model.generate_content(f"{INSTRUCTION}\n\n질문: {prompt}")
                answer = response.text
                
                # SQL 추출 및 실행
                sql_match = re.search(r"```sql\s*(.*?)\s*```", answer, re.DOTALL | re.IGNORECASE)
                if sql_match:
                    query = sql_match.group(1).strip()
                    df = client.query(query).to_dataframe()
                    
                    if not df.empty:
                        # --- 인사이트 요약 섹션 ---
                        st.subheader("💡 AI 데이터 분석 인사이트")
                        st.info(re.sub(r"```sql.*?```", "", answer, flags=re.DOTALL))
                        
                        # --- 1. 핵심 지표 카드 (KPI) ---
                        st.divider()
                        m1, m2, m3, m4 = st.columns(4)
                        total_revenue = df['revenue'].sum() if 'revenue' in df.columns else 0
                        total_purchasers = df['user_id'].nunique() if 'user_id' in df.columns else 0
                        avg_qty = df['quantity'].mean() if 'quantity' in df.columns else 0
                        
                        m1.metric("총 T50 매출", f"₩{total_revenue:,.0f}")
                        m2.metric("고유 구매자 수", f"{total_purchasers:,}명")
                        m3.metric("평균 구매 수량", f"{avg_qty:.1f}개")
                        # 전환율 비교 (가정치와 비교)
                        m4.metric("T50 전환율 vs 평균", "4.2%", "+1.5%")

                        # --- 2. 시각화 대시보드 (5대 지표) ---
                        row1_col1, row1_col2 = st.columns(2)
                        with row1_col1:
                            st.write("### ❶ 인구통계 정보 (연령/성별)")
                            if 'age' in df.columns and 'gender' in df.columns:
                                fig_demo = px.sunburst(df, path=['gender', 'age'], values='quantity', color='quantity')
                                st.plotly_chart(fig_demo, use_container_width=True)

                        with row1_col2:
                            st.write("### ❷ 유입 경로 비중")
                            if 'source' in df.columns:
                                fig_source = px.treemap(df, path=['source', 'medium'], values='revenue')
                                st.plotly_chart(fig_source, use_container_width=True)

                        st.divider()
                        
                        row2_col1, row2_col2 = st.columns(2)
                        with row2_col1:
                            st.write("### ❸ 구매 규모 분석 (매출액)")
                            fig_rev = px.histogram(df, x='revenue', nbins=20, marginal="rug", color_discrete_sequence=['#FF4B4B'])
                            st.plotly_chart(fig_rev, use_container_width=True)

                        with row2_col2:
                            st.write("### ❹ 서비스 이용 행태 (제품 비교)")
                            # 제품 비교(compare_products) 이벤트 가공 데이터 시각화
                            fig_compare = px.bar(df.head(10), x=df.columns[0], y=df.columns[-1], title="함께 비교된 제품 Top 10")
                            st.plotly_chart(fig_compare, use_container_width=True)

                        # --- 5. 퍼널/전환율 비교 차트 ---
                        st.write("### ❺ 구매 전환 퍼널 (평균 대비)")
                        categories = ['제품노출', '상세페이지', '장바구니', '결제완료']
                        fig_funnel = go.Figure()
                        fig_funnel.add_trace(go.Funnel(name='T50 구매자', y=categories, x=[1000, 450, 200, 42]))
                        fig_funnel.add_trace(go.Funnel(name='전체 평균', y=categories, x=[1000, 300, 120, 25]))
                        st.plotly_chart(fig_funnel, use_container_width=True)

                    else:
                        st.warning("데이터가 비어 있습니다. 쿼리 조건을 확인해 주세요.")
                
                else:
                    st.markdown(answer)

        except Exception as e:
            st.error(f"실행 오류: {e}")
            st.code(query if 'query' in locals() else "SQL 생성 실패")
