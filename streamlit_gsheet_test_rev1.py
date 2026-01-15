# -*- coding: utf-8 -*-
import streamlit as st
from streamlit_option_menu import option_menu #pip install streamlit-option-menu
from streamlit_gsheets import GSheetsConnection  #st-gsheets-connection
import pandas as pd

st.set_page_config(page_title="자료조회 시스템", layout="wide")
st.markdown("""
    <style>
    /* 데이터프레임의 헤더 글자 크기 */
    .stDataFrame div[data-testid="stTableHD"] {
        font-size: 18px !important;
    }
    /* 데이터프레임의 본문 셀 글자 크기 */
    .stDataFrame div[data-testid="stTableCD"] {
        font-size: 18px !important;
    }
    /* 일반 테이블(st.table)의 글자 크기도 변경하고 싶을 경우 */
    .stTable td, .stTable th {
        font-size: 18px !important;
    }
    /* AG-Grid 기반 텍스트 크기 조정 */
    .common-value-class {
        font-size: 18px !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 1. 공통 함수 정의 (효율성 및 중복 제거) ---
# Google Sheets 연결
conn = st.connection("gsheets", type=GSheetsConnection)

@st.cache_data(ttl=600)  # 10분간 캐시 유지
def load_and_clean_data(url, cols=None, numeric_col=None):
    """데이터를 로드하고 특정 컬럼의 콤마를 제거하여 숫자로 변환합니다."""
    try:
        df = conn.read(spreadsheet=url, usecols=cols)
        if numeric_col and numeric_col in df.columns:
            df[numeric_col] = df[numeric_col].astype(str).str.replace(',', '')
            df[numeric_col] = pd.to_numeric(df[numeric_col], errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")
        return pd.DataFrame()

def display_search_result(df, cond, target_col=None, is_transpose=False):
    """필터링된 결과를 출력하고 합계를 표시합니다."""
    dff = df[cond]
    if not dff.empty:
        if is_transpose:
            st.dataframe(dff.T.reset_index(), use_container_width=True, hide_index=True)
        else:
            st.dataframe(dff, use_container_width=True, hide_index=True)
        
        if target_col and target_col in dff.columns:
            total_val = dff[target_col].sum()
            st.metric(label=f"💰 검색 결과 {target_col} 합계", value=f"{total_val:,.0f} 원")
    else:
        st.warning("조회된 결과가 없습니다.")


# --- 2. 사이드바 구성 ---
with st.sidebar:
    menu = option_menu(
        "메인 메뉴",
        ["사업개요","분양","PF현황", "중도금결산", "중도금", "채권"],
        icons=["info-circle", "house", "bank", "bank", "bank","bank"],
        menu_icon="cast", default_index=0,
        )    
# =============================================================================
#     menu1 = option_menu(
#         menu_title="Menu1", #required
#         options=["opt1","opt2","opt3"], #required
#         icons=["house", "book", "envelope", "envelope", "envelope", "envelope"], #optional
#         menu_icon="cast", #optional
#         #default_index=0, #optional
#         )
#     st.info("기타 옵션은 준비 중입니다.") # menu1, menu3 등 미사용 메뉴 정리
# =============================================================================


# --- 3. 메뉴별 로직 ---
if menu == "사업개요":
    st.subheader('📊 사업개요')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=0#gid=0"    
    # 1. 입력부
    pj = st.text_input('사업명 입력 (미입력 시 전체 조회):')
    search_clicked = st.button('조회')        
# =============================================================================
#     acol1, acol2, acol3, acol4 = st.columns(4)
#     with acol1:
#         pj = st.text_input('사업명 입력',
#             placeholder="사업명 입력 (미입력 시 전체 조회)", 
#             label_visibility="collapsed" # 라벨 공간 제거
#         )    
#     with acol2:
#         search_clicked = st.button("조회", use_container_width=True)
# =============================================================================
    
    # 2. 버튼 클릭 시 로직 실행
    if search_clicked:        
        data = load_and_clean_data(url, cols=list(range(15)))    
        data2 = load_and_clean_data(url, cols=[1] + list(range(16, 40)))        
        
        if pj:
            cond = data['사업명'].str.contains(pj, na=False, case=False)
            cond2 = data2['사업명'].str.contains(pj, na=False, case=False)            
            dff = data[cond]
            dff2 = data2[cond2]            
            # 검색 결과가 있는 경우에만 일정 매핑 진행
            if not dff2.empty:
                row = dff2.iloc[0]
                schedule_list = []
                
                for i in range(1, 13):
                    col_date = f'일정{i}'
                    col_name = f'일정명{i}'                    
                    # 컬럼 존재 여부 확인 후 진행
                    if col_date in row and col_name in row:
                        val_date = row[col_date]
                        
                        if pd.notna(val_date):
                            try:
                                dt_obj = pd.to_datetime(val_date)
                                display_date = dt_obj.strftime('%Y-%m')
                            except:
                                display_date = val_date
                                
                            schedule_list.append({
                                "날짜": display_date,
                                "일정명": row[col_name]
                            })
                
                df_schedule = pd.DataFrame(schedule_list)
                # 불필요한 날짜 필터링
                if not df_schedule.empty:
                    df_schedule = df_schedule[df_schedule['날짜'] != '1900-01-00']
            else:
                df_schedule = pd.DataFrame()
        else:
            # 사업명 입력 없이 조회 버튼 누른 경우 전체 조회
            dff = data
            df_schedule = pd.DataFrame() # 전체 조회 시에는 일정을 표시하지 않거나 별도 로직 필요

        # 3. 결과 표시
        if not dff.empty:                        
            if pj:
                col1, col2 = st.columns([3,2])
                with col1:
                    st.subheader('개요')
                    dfft = dff.T.reset_index()
                    dfft.columns=['구분','내용']
                    
                    st.dataframe(dfft, use_container_width=True, hide_index=True, height=500,
                                 column_config={
                                     "발주처": st.column_config.Column(width="large"),
                                     "공사개요": st.column_config.Column(width="large")
                                 })
                with col2:            
                    st.subheader('추진일정')
                    if not df_schedule.empty:
                        st.dataframe(df_schedule, use_container_width=True, hide_index=True)
                    else:
                        st.info("등록된 추진 일정이 없습니다.")
                
                        
            else:
                # 전체 조회 모드
                st.subheader('📊 전체 사업 목록')
                st.dataframe(dff, use_container_width=True, hide_index=True, height=500,
                             column_config={
                                 "발주처": st.column_config.Column(width="large"),
                                 "공사개요": st.column_config.Column(width="large")
                             })
        else:
            st.warning("조회된 결과가 없습니다.")
            
elif menu == "PF현황":
    st.subheader('📊 PF현황 조회')
    url = "https://docs.google.com/spreadsheets/d/1G4GJIXw36pKUoPgAR2I8yQ0zcTKoscwAoNW5nu7oNPI/edit?gid=0#gid=0"
    data = load_and_clean_data(url, cols=[0,1,2,3,4,5,6,7,11,12,14], numeric_col='잔액')

    col1, col2 = st.columns(2)
    with col1: pj = st.text_input('사업명 입력:')
    with col2: dday = st.selectbox('기준월 선택', sorted(data['기준월'].unique(), reverse=True))
    
    if st.button('조회'):
        cond = (data['기준월'] == dday)
        if pj:
            cond &= data['PJ명'].str.contains(pj, na=False, case=False)
        display_search_result(data, cond, target_col='잔액')

elif menu == "중도금":
    st.subheader('🏠 중도금 관리')
    mid_tab = st.selectbox("PJ선택", ["서면", "트라반트", "시민공원"])
    urls = {
        "서면": 'https://docs.google.com/spreadsheets/d/1P-f6lZCK7ln1iJEPBUtQqVGWNy-g7G_5iBDYLnWZB-E/edit?gid=943639489',
        "트라반트": 'https://docs.google.com/spreadsheets/d/1P-f6lZCK7ln1iJEPBUtQqVGWNy-g7G_5iBDYLnWZB-E/edit?gid=453535398',
        "시민공원": 'https://docs.google.com/spreadsheets/d/1P-f6lZCK7ln1iJEPBUtQqVGWNy-g7G_5iBDYLnWZB-E/edit?gid=668236831'
    }
    data = load_and_clean_data(urls[mid_tab], cols=list(range(10)), numeric_col='대출잔액')
    display_search_result(data, [True] * len(data), target_col='대출잔액')

elif menu == "채권":
    st.subheader('📊 채권현황 조회')
    url = "https://docs.google.com/spreadsheets/d/1RlNYrWWezvHQfceEgmHIkC-c7dnIxRIWZTM3fWdqDWQ/edit?gid=0#gid=0"
    data = load_and_clean_data(url, numeric_col='채권')

    col1, col2 = st.columns(2)
    with col1: pj = st.text_input('PJ명 입력 (손익센터명):')
    with col2: dday = st.selectbox('기준월 선택 ', sorted(data['기준월'].unique(), reverse=True))

    if st.button('조회'):
        cond = (data['기준월'] == dday)
        if pj: cond &= data['손익센터명'].str.contains(pj, na=False, case=False)  #cond = cond & (새로운 조건) 과 동일, case : 대소문자 구분안함.
        
        dff = data[cond]
        if not dff.empty:
            st.dataframe(dff, use_container_width=True, hide_index=True)
            
            st.divider()
            st.subheader('📊 계정별 채권 합계 요약')
            grouped_df = dff.groupby(['계정대분류', '계정소분류'], as_index=False)['채권'].sum()
            
            c1, c2 = st.columns([2, 1])
            with c1: st.table(grouped_df) # 요약은 테이블이 깔끔함
            with c2: st.metric(label="💰 총 채권 합계", value=f"{dff['채권'].sum():,.0f} 원")
        else:
            st.warning("조회된 결과가 없습니다.")

elif menu == "중도금결산":
    st.subheader('🏠 중도금결산자료')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=67742981"
    data = load_and_clean_data(url, cols=[0,1,3,5,14,17,27], numeric_col='잔액')

    pj = st.text_input('사업명 입력:')
    if st.button('조회 '):
        cond = data['사업명'].str.contains(pj, na=False, case=False) if pj else [True] * len(data)
        display_search_result(data, cond, target_col='잔액')


elif menu == "분양":
    st.subheader('📊 분양현황 조회')
    #url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=875634826#gid=875634826" #분양s
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=391839077#gid=391839077"
    
    data = load_and_clean_data(url, numeric_col='총분양금')

    pj = st.text_input('사업명 입력')
    search_btn = st.button('조회')
    #with col2: dday = st.selectbox('기준월 선택 ', sorted(data['기준월'].unique(), reverse=True))

    if search_btn:
        #cond = (data['기준월'] == dday)
        if pj:
            cond = data['사업명'].str.contains(pj, na=False, case=False)        
            data['입주증번호']=pd.to_numeric(data['입주증번호'],errors='coerce').fillna(0)            
            dff = data[cond]    
            ibju = dff['입주증번호'].sum()
            lawsuit = (dff['소송']=='소송').sum() #소송 개수
            if not dff.empty:                
                # 1. 피벗 테이블 생성 (계약여부별 동호수 개수)
                dfp = dff.pivot_table(
                    index='상품', 
                    columns='계약여부', 
                    values='동호수', 
                    aggfunc='count',
                    fill_value=0)        
                # 2. 비율(%) 계산 및 컬럼 추가 # 행 단위 합계(전체 물량) 계산
                dfp['공급'] = dfp.sum(axis=1)
                # 3. (선택사항) '계약' 칼럼 기준 내림차순 정렬
                if '공급' in dfp.columns:
                    dfp = dfp.sort_values(by='공급', ascending=False)
            
                # 4. 비율(%) 계산 (합계 칼럼을 기준으로 계산)
                original_cols = [c for c in dfp.columns if c != '공급'] # 합계 제외한 원래 칼럼들
                for col in original_cols:
                    dfp[f'{col}(%)'] = (dfp[col] / dfp['공급'] * 100).round(0).fillna(0)
                # 3. 데이터프레임 정리 (인덱스 초기화)                
                dfp = dfp[['공급','계약','미계약','계약(%)','미계약(%)']]
                dfp = dfp.reset_index()
                
                
                dfp2 = dff.pivot_table(
                    index='상품', 
                    columns='계약여부', 
                    values='총분양금', 
                    aggfunc='sum',
                    fill_value=0)        
                # 2. 백만 단위 변환 및 소수점 처리
                # 모든 수치형 데이터를 1,000,000으로 나눕니다.
                dfp2['공급'] = dfp2.sum(axis=1)
                dfp2 = (dfp2/ 1_000_000).round(0) 
                
                if '공급' in dfp2.columns:
                    dfp2 = dfp2.sort_values(by='공급', ascending=False)

                original_cols2 = [c for c in dfp2.columns if c != '공급'] # 합계 제외한 원래 칼럼들
                for col in original_cols2:
                    dfp2[f'{col}(%)'] = (dfp2[col] / dfp2['공급'] * 100).round(0).fillna(0)
                # 3. 데이터프레임 정리 (인덱스 초기화)                
                dfp2 = dfp2[['공급','계약','미계약','계약(%)','미계약(%)']]
                dfp2 = dfp2.reset_index()               
        
                #c1, c2 = st.columns([3, 1]) # %가 추가되었으므로 비율을 조금 조정
                c1, c2 = st.columns(2) # %가 추가되었으므로 비율을 조금 조정
                with c1:
                    st.subheader('동호기준')                
                    #st.write('(단위 : 세대(실), %)')
                    st.markdown('<div style="text-align: right;">(단위 : 세대(실), %)</div>', unsafe_allow_html=True)                
                    st.dataframe(dfp, use_container_width=True, hide_index=True)
                    
                with c2:
                    st.subheader('금액기준')
                    #st.write('(단위 : 백만원, %)')
                    st.markdown('<div style="text-align: right;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)                    
                    st.dataframe(dfp2, use_container_width=True, hide_index=True)
                
                st.divider()               
                
                
                if ibju > 0:
                    st.subheader('📊 입주현황')                                        
                    dfp3 = dff.pivot_table(
                        index='상품',
                        columns='완납여부', 
                        values='동호수', 
                        aggfunc='count',
                        fill_value=0)        
                    # [추가] 필수 컬럼('완납', '미납')이 없을 경우를 대비해 0으로 생성
# =============================================================================
#                     for col in ['완납', '미납']:
#                         if col not in dfp3.columns:
#                             dfp3[col] = 0
# =============================================================================
                
                    # 2. 합계 및 정렬 (초기 공급량 기준 정렬)
                    dfp3['공급'] = dfp3.sum(axis=1)
                    dfp3 = dfp3.sort_values(by='공급', ascending=False)
                
                    # 3. 비율(%) 계산
                    # '공급'을 제외한 원래의 컬럼들(완납, 미납)에 대해 루프
                    # 만약 컬럼이 더 많아질 수 있다면 이 방식이 안전합니다.
                    target_cols = [c for c in ['완납', '미납'] if c in dfp3.columns]
                    for col in target_cols:
                        dfp3[f'{col}(%)'] = (dfp3[col] / dfp3['공급'] * 100).round(0).fillna(0)
                
                    # 4. 데이터프레임 정리 및 인덱스 초기화
                    dfp3 = dfp3.reset_index()
                    
                    # 5. 사용자 지정 순서 정렬
                    custom_order = ['아파트', '오피스텔', '생활숙박시설','지식산업센터','판매시설', '상가']
                    # 데이터에 존재하는 '상품'만 카테고리로 설정 (데이터 유실 방지)
                    dfp3['상품'] = pd.Categorical(dfp3['상품'], categories=custom_order, ordered=True)
                    
                    # 6. 최종 컬럼 선택 및 정렬
                    # 컬럼 존재 여부를 다시 확인하며 슬라이싱
                    final_cols = ['상품', '공급', '완납', '미납', '완납(%)', '미납(%)']
                    dfp3 = dfp3[final_cols].sort_values(by='상품')                
                    
                                        
                    dfp4 = dff.pivot_table(
                        index='상품', 
                        columns='완납여부', 
                        values='총분양금', 
                        aggfunc='sum',
                        fill_value=0)        
                    # 2. 백만 단위 변환 및 소수점 처리
                    # 모든 수치형 데이터를 1,000,000으로 나눕니다.
                    dfp4['공급'] = dfp4.sum(axis=1)
                    dfp4 = (dfp4/ 1_000_000).round(0) 
                    
                    if '공급' in dfp4.columns:
                        dfp4 = dfp4.sort_values(by='공급', ascending=False)

                    original_cols4 = [c for c in dfp4.columns if c != '공급'] # 합계 제외한 원래 칼럼들
                    for col in original_cols4:
                        dfp4[f'{col}(%)'] = (dfp4[col] / dfp4['공급'] * 100).round(0).fillna(0)
                    # 3. 데이터프레임 정리 (인덱스 초기화)                
                    dfp4 = dfp4[['공급','완납','미납','완납(%)','미납(%)']]
                    dfp4 = dfp4.reset_index()
                    
                    c3, c4 = st.columns(2) # %가 추가되었으므로 비율을 조금 조정
                    
                    with c3:
                        st.subheader('동호기준')                
                        #st.write('(단위 : 세대(실), %)')
                        st.markdown('<div style="text-align: right;">(단위 : 세대(실), %)</div>', unsafe_allow_html=True)                
                        st.dataframe(dfp3, use_container_width=True, hide_index=True) #hide_index를 하면 인덱스 숨김
                    
                    with c4:
                        st.subheader('금액기준')
                        #st.write('(단위 : 백만원, %)')
                        st.markdown('<div style="text-align: right;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)                    
                        st.dataframe(dfp4, use_container_width=True, hide_index=True)
                        
                st.divider()                
                
                if lawsuit > 0:
                    st.subheader('📊 소송현황')                                        
                    dfp5 = dff.pivot_table(
                        index='상품',
                        columns='소송', 
                        values='동호수', 
                        aggfunc='count',
                        fill_value=0)        
                    # 2. 백만 단위 변환 및 소수점 처리
                    # 모든 수치형 데이터를 1,000,000으로 나눕니다.
                    dfp5['세대'] = dfp5.sum(axis=1)
                    #dfp2 = (dfp2/ 1_000_000).round(0) 
                    
                    dfp5 = dfp5.sort_values(by='세대', ascending=False)

                    original_cols5 = [c for c in dfp5.columns if c != '세대'] # 합계 제외한 원래 칼럼들
                    for col in original_cols5:
                        dfp5[f'{col}(%)'] = (dfp5[col] / dfp5['세대'] * 100).round(0).fillna(0)
                    # 3. 데이터프레임 정리 (인덱스 초기화)                                    
                    dfp5 = dfp5[['세대','소송','미소송','소송(%)','미소송(%)']]
                    dfp5 = dfp5.reset_index()
                    
                    custom_order = ['아파트', '오피스텔', '생활숙박시설','지식산업센터','판매시설', '상가']                    
                    # 해당 컬럼을 Categorical 타입으로 변환 (ordered=True가 핵심)
                    dfp5['상품'] = pd.Categorical(dfp5['상품'], categories=custom_order, ordered=True)
                    # 정렬 실행
                    dfp5 = dfp5.sort_values(by='상품')     
                    
                    c5, c6 = st.columns(2) # %가 추가되었으므로 비율을 조금 조정
                    
                    with c5:
                        st.subheader('동호기준')                
                        #st.write('(단위 : 세대(실), %)')
                        st.markdown('<div style="text-align: right;">(단위 : 세대(실), %)</div>', unsafe_allow_html=True)                
                        st.dataframe(dfp5, use_container_width=True, hide_index=True) #hide_index를 하면 인덱스 숨김
                    
# =============================================================================
#                     with c4:
#                         st.subheader('금액기준')
#                         #st.write('(단위 : 백만원, %)')
#                         st.markdown('<div style="text-align: right;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)                    
#                         st.dataframe(dfp4, use_container_width=True, hide_index=True)
# =============================================================================
                    
                    
            else:
                st.warning("조회된 결과가 없습니다.")            
