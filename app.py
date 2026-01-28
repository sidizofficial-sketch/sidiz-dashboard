import streamlit as st
import pandas as pd
import plotly.express as px

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ 데이터 분석 대시보드", layout="wide")

# 2. 구글 시트 데이터 로드 (CSV 내보내기 링크 활용)
SHEET_ID = "162kRSBh40uJ5DEe_6gOo6V9lQy7hRURqSigDoDrQQfg"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv"

@st.cache_data(ttl=600)  # 10분마다 데이터 갱신
def load_data():
    df = pd.read_csv(SHEET_URL)
    # 날짜 컬럼이 있다면 여기서 변환 (컬럼명이 'date'라고 가정)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"시트 데이터를 불러오는 데 실패했습니다: {e}")
    st.stop()

# 3. 사이드바 필터 (정확성 보장)
st.sidebar.header("🔍 분석 필터")

# 시트에 실제 존재하는 제품명만 추출
if 'item_name' in df.columns:
    product_list = sorted(df['item_name'].dropna().unique().tolist())
    # '뮤브'가 포함된 제품을 기본값으로 찾기
    default_idx = next((i for i, s in enumerate(product_list) if 'MUUVE' in s or '뮤브' in s), 0)
    target_product = st.sidebar.selectbox("분석 대상 제품명", product_list, index=default_idx)
else:
    st.sidebar.error("시트에 'item_name' 컬럼이 없습니다.")
    st.stop()

# 4. 데이터 필터링 로직 (파이썬이 직접 수행 - 환각 없음)
filtered_df = df[df['item_name'] == target_product]

# 5. 대시보드 메인 화면
st.title(f"📊 {target_product} 분석 리포트")
st.write(f"분석 기준일: {pd.to_datetime('today').strftime('%Y-%m-%d')}")

if not filtered_df.empty:
    # KPI 지표
    col1, col2, col3 = st.columns(3)
    col1.metric("총 세션", f"{filtered_df['sessions'].sum():,}")
    col2.metric("활성 사용자", f"{filtered_df['active_users'].sum():,}")
    col3.metric("전환수", f"{filtered_df['conversions'].sum():,}")

    st.divider()

    # 유입 경로 TOP 5 + 기타(Others) 처리
    st.subheader("🌐 주요 유입 경로 분석")
    source_counts = filtered_df.groupby('source_medium')['sessions'].sum().reset_index()
    source_counts = source_counts.sort_values('sessions', ascending=False)

    top_n = 5
    top_sources = source_counts.head(top_n)
    others_count = source_counts.iloc[top_n:]['sessions'].sum()

    if others_count > 0:
        others_df = pd.DataFrame({'source_medium': ['기타 (Others)'], 'sessions': [others_count]})
        final_source_df = pd.concat([top_sources, others_df], ignore_index=True)
    else:
        final_source_df = top_sources

    # 시각화 (Plotly 사용 - 한글 깨짐 없음)
    fig = px.pie(final_source_df, values='sessions', names='source_medium', 
                 hole=0.4, title=f"{target_product} 유입 경로 비중")
    st.plotly_chart(fig, use_container_width=True)

    # 데이터 상세 보기
    with st.expander("원본 데이터 상세 보기"):
        st.dataframe(filtered_df)
else:
    st.warning("선택한 제품에 대한 데이터가 시트에 존재하지 않습니다.")

# 6. 하단 인사이트 (Gemini API 연동 가능 구역)
st.divider()
st.info("💡 **Tip:** 위 차트에서 비중이 가장 높은 채널의 상세 랜딩 페이지 이탈률을 점검해보세요.")

# 기존 SHEET_ID 부분을 아래와 같이 수정해보세요
SHEET_ID = "162kRSBh40uJ5DEe_6gOo6V9lQy7hRURqSigDoDrQQfg"
# 헤더 정보를 명확히 가져오기 위해 링크를 살짝 변경합니다
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

@st.cache_data(ttl=60) # 테스트 중에는 캐시 시간을 짧게(1분) 잡는 게 좋습니다
def load_data():
    # 데이터가 비어있는지 확인하는 로직 추가
    df = pd.read_csv(SHEET_URL)
    if df.empty:
        raise ValueError("시트에 데이터가 비어있거나 읽을 수 없습니다.")
    return df
