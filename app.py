# -------------------------------------------------
# 4. 메인 UI 및 출력 (수정 버전)
# -------------------------------------------------
st.title("🪑 SIDIZ Intelligence Dashboard")

today = datetime.now()
with st.sidebar:
    st.header("⚙️ 분석 설정")
    curr_date = st.date_input("분석 기간", [today - timedelta(days=7), today - timedelta(days=1)])
    comp_date = st.date_input("비교 기간", [today - timedelta(days=14), today - timedelta(days=8)])
    time_unit = st.selectbox("추이 분석 단위", ["일별", "주별", "월별"])

if len(curr_date) == 2 and len(comp_date) == 2:
    summary_df, ts_df, source_df = get_dashboard_data(curr_date[0], curr_date[1], comp_date[0], comp_date[1], time_unit)
    
    if summary_df is not None and not summary_df.empty:
        curr = summary_df[summary_df['type'] == 'Current'].iloc[0]
        prev = summary_df[summary_df['type'] == 'Previous'].iloc[0] if 'Previous' in summary_df['type'].values else curr

        # [8대 지표 출력]
        def get_delta(c, p):
            if p == 0: return "0%"
            return f"{((c - p) / p * 100):+.1f}%"

        st.subheader("🎯 핵심 성과 요약")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("활성 사용자", f"{int(curr['users']):,}", get_delta(curr['users'], prev['users']))
        c1.metric("세션 수", f"{int(curr['sessions']):,}", get_delta(curr['sessions'], prev['sessions']))
        
        c2.metric("신규 사용자", f"{int(curr['new_users']):,}", get_delta(curr['new_users'], prev['new_users']))
        c2.metric("주문 수", f"{int(curr['orders']):,}", get_delta(curr['orders'], prev['orders']))
        
        c_nv = (curr['new_users']/curr['users']*100) if curr['users'] > 0 else 0
        p_nv = (prev['new_users']/prev['users']*100) if prev['users'] > 0 else 0
        c3.metric("신규 방문율", f"{c_nv:.1f}%", f"{c_nv-p_nv:+.1f}%p")
        c3.metric("구매전환율", f"{(curr['orders']/curr['sessions']*100):.2f}%", f"{(curr['orders']/curr['sessions']*100 - prev['orders']/prev['sessions']*100):+.2f}%p")
        
        c4.metric("총 매출액", f"₩{int(curr['revenue']):,}", get_delta(curr['revenue'], prev['revenue']))
        c_aov = (curr['revenue']/curr['orders']) if curr['orders'] > 0 else 0
        p_aov = (prev['revenue']/prev['orders']) if prev['orders'] > 0 else 0
        c4.metric("평균 객단가(AOV)", f"₩{int(c_aov):,}", get_delta(c_aov, p_aov))

        # [대량 구매 성과 섹션]
        st.markdown("---")
        st.subheader("📦 대량 구매 세그먼트 (150만 원↑)")
        b1, b2, b3 = st.columns(3)
        b1.metric("대량 주문 건수", f"{int(curr['bulk_orders'])}건", f"{int(curr['bulk_orders'] - prev['bulk_orders']):+}건")
        b2.metric("대량 구매 매출", f"₩{int(curr['bulk_revenue']):,}", get_delta(curr['bulk_revenue'], prev['bulk_revenue']))
        b3.metric("대량 구매 매출 비중", f"{(curr['bulk_revenue']/curr['revenue']*100 if curr['revenue']>0 else 0):.1f}%")

        # [차트 섹션]
        st.markdown("---")
        st.subheader(f"📊 {time_unit} 매출 추이")
        fig = go.Figure()
        fig.add_bar(x=ts_df['period_label'], y=ts_df['revenue'], name="전체 매출", marker_color='#2ca02c')
        fig.add_scatter(x=ts_df['period_label'], y=ts_df['bulk_orders'], name="대량 주문수", yaxis="y2", line=dict(color='#FF4B4B'))
        fig.update_layout(yaxis2=dict(overlaying="y", side="right"), template="plotly_white", hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # [데이터 기반 인사이트 요약 섹션]
        st.markdown("---")
        st.subheader("💡 데이터 기반 인사이트 요약")

        # 주요 지표 변화
        insights = []
        if curr['revenue'] > prev['revenue']:
            insights.append(f"총 매출액이 전기 대비 {get_delta(curr['revenue'], prev['revenue'])} 증가했습니다.")
        else:
            insights.append(f"총 매출액이 전기 대비 {get_delta(curr['revenue'], prev['revenue'])} 감소했습니다.")

        if curr['orders'] > prev['orders']:
            insights.append(f"주문 수가 전기 대비 {get_delta(curr['orders'], prev['orders'])} 증가했습니다.")
        else:
            insights.append(f"주문 수가 전기 대비 {get_delta(curr['orders'], prev['orders'])} 감소했습니다.")

        # 대량 구매 영향
        insights.append(f"대량 구매 매출 비중은 {b3.metric('dummy', 0)[0]}%로 전체 매출에 미치는 영향 참고 필요.")

        # 주요 유입 채널 요약
        if source_df is not None and not source_df.empty:
            top_sources = ", ".join(source_df['source'].head(3).tolist())
            insights.append(f"주요 유입 채널: {top_sources} (매출 기준)")

        # 카드 형태 출력
        for insight in insights:
            st.info(insight)

else:
    st.info("💡 사이드바에서 기간을 선택해주세요.")
