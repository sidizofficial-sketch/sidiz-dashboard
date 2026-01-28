import streamlit as st
import pandas as pd
import plotly.express as px

# 1. 페이지 설정
st.set_page_config(page_title="SIDIZ 데이터 분석 대시보드", layout="wide")

# 2. 구글 시트 주소 설정 (가장 안정적인 CSV 추출 방식)
# 링크 뒤의 /edit... 부분을 /export?format=csv로 강제 치환합니다.
sheet_url = "https://docs.google.com/spreadsheets/d/162kRSBh40uJ5DEe_6gOo6V9lQy7hRURqSigDoDrQQfg/export?format=csv"

@st.cache_data(ttl=60)
def load_data(url):
    # 주소에서 직접 읽어오기
    df = pd.read_csv(url)
    return df

st.title("📊 SIDIZ 실시간 데이터 대시보드")

try:
    df = load_data(sheet_url)
    
    # 데이터가 제대로 로드되었는지 상단에 살짝 표시
    st.success("✅ 시트 연결 성공!")
    
    # 사이드바 필터
    st.sidebar.header("🔍 분석 필터")
    
    # 실제 시트의 컬럼명을 확인하기 위한 로직
    cols = df.columns.tolist()
    
    # 만약 시트에 item_name이라는 컬럼이 있다면
    if 'item_name' in df:
        product_list = sorted(df['item_name'].dropna().unique().tolist())
        target_product = st.sidebar.selectbox("분석 대상 제품명", product_list)
        
        filtered_df = df[df['item_name'] == target_product]
        
        # 지표 출력
        col1, col2, col3 = st.columns(3)
        col1.metric("총 세션", f"{filtered_df.get('sessions', pd.Series([0])).sum():,}")
        col2.metric("활성 사용자", f"{filtered_df.get('active_users', pd.Series([0])).sum():,}")
        col3.metric("전환수", f"{filtered_df.get('conversions', pd.Series([0])).sum():,}")
        
        st.divider()
        st.subheader(f"📌 {target_product} 상세 데이터")
        st.write(filtered_df)
    else:
        st.warning("시트에서 'item_name' 컬럼을 찾을 수 없습니다. 컬럼명을 확인해주세요.")
        st.write("현재 시트의 컬럼들:", cols)

except Exception as e:
    st.error(f"❌ 데이터를 불러올 수 없습니다.")
    st.info("원인: 구글 시트의 [공유] 설정이 '링크가 있는 모든 사용자'에게 '뷰어' 권한으로 열려있는지 확인해주세요.")
    st.write(f"상세 에러 내용: {e}")
