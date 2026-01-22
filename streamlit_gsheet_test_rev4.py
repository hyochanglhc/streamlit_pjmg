# -*- coding: utf-8 -*-
import streamlit as st
from streamlit_option_menu import option_menu 
from streamlit_gsheets import GSheetsConnection 
import pandas as pd
import io, os
import datetime
from pandas.tseries.offsets import MonthEnd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import plotly.express as px

# --- 설정 및 스타일 ---
st.set_page_config(page_title="구글시트조회", layout="wide")
st.markdown("""
    <style>    
    .stDataFrame div[data-testid="stTableHD"] {font-size: 14px !important;}    
    .stDataFrame div[data-testid="stTableCD"] {font-size: 14px !important;}
    .stTable td, .stTable th {font-size: 14px !important;}
    [data-testid="stMetricLabel"] {font-size: 16px !important;}
    [data-testid="stMetricValue"] {font-size: 18px !important;}
    </style>
    """, unsafe_allow_html=True) 

# --- 공통 연결 객체 및 함수 ---
conn = st.connection("gsheets", type=GSheetsConnection)

def get_engine():
    load_dotenv()
    db_user = os.getenv("DB_USER")
    db_pw = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_url = f"mysql+pymysql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url)

def display_search_result(df, cond, target_col=None):
    """필터링된 결과를 출력하고 합계를 표시합니다."""
    dff = df[cond].copy()
    if not dff.empty:
        num_cols = dff.select_dtypes(include=['number']).columns
        config = {col: st.column_config.NumberColumn(format="%d") for col in num_cols}
        st.dataframe(dff, use_container_width=True, hide_index=True, column_config=config)
        
        if target_col and target_col in dff.columns:
            total_val = dff[target_col].sum()            
            st.metric(label=f"💰 {target_col} 합계", value=f"{total_val:,.0f} 원")
    else:
        st.warning("조회된 결과가 없습니다.")

@st.cache_data
def load_location_data():    
    file_path = "file_content.txt"
    if not os.path.exists(file_path): return {}
    
    try:
        with open(file_path, "r", encoding="cp949") as f:
            file_content = f.read()
    except:
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()
            
    data = {}
    lines = file_content.strip().split('\n')
    big_city = ['성남시','수원시','고양시','부천시','안양시','안산시','용인시','창원시','천안시','포항시','청주시','전주시']
    
    for line in lines[1:]:
        parts = line.split('\t')
        if len(parts) < 3 or parts[2].strip() != '존재': continue        
        full_address = parts[1].strip()
        address_parts = full_address.split()
        if len(address_parts) < 2: continue
        
        sido = address_parts[0]
        sigungu, dong = "", ""
        
        if len(address_parts) == 2 and address_parts[1] in big_city: continue
        elif address_parts[0] =='세종특별자치시':
            sigungu, dong = "세종시", ' '.join(address_parts[1:])
        elif len(address_parts) > 2 and address_parts[1] in big_city:
            sigungu, dong = " ".join(address_parts[1:3]), ' '.join(address_parts[3:])
        else:
            sigungu = address_parts[1]
            dong = ' '.join(address_parts[2:]) if len(address_parts) > 2 else ""
            
        if sido not in data: data[sido] = {}
        if sigungu and sigungu not in data[sido]: data[sido][sigungu] = []
        if dong and dong not in data[sido][sigungu]: data[sido][sigungu].append(dong)
    return data

# --- 사이드바 ---
with st.sidebar:
    menu = option_menu("사업관리", ["옵션선택","사업개요","분양","PF현황", "채권", "중도금결산", "중도금", "실거래조회"],
                       icons=["dash","info-circle", "bank", "bank", "bank", "bank","bank","house"],
                       menu_icon="cast", default_index=0)

num_cols = ['채권', '불량', '잔액', '총분양금', '대출잔액']

# --- 메뉴별 로직 ---
if menu == "사업개요":
    st.subheader('📊 사업개요')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=0#gid=0"        
    pj = st.text_input('사업명 입력 (미입력 시 전체 조회)')
    
    if st.button('조회'):
        data = conn.read(spreadsheet=url, usecols=list(range(15))).fillna("")
        data2 = conn.read(spreadsheet=url, usecols=[1] + list(range(17, 41))).fillna("")
        
        dff = data[data['사업명'].str.contains(pj, na=False, case=False)] if pj else data
        
        if pj and not dff.empty:
            col1, col2 = st.columns([3,2])
            with col1:
                st.subheader('개요')
                dfft = dff.iloc[[0]].T.reset_index()
                dfft.columns=['구분','내용']
                st.dataframe(dfft, use_container_width=True, hide_index=True, height=500)
            with col2:
                st.subheader('추진일정')
                row2 = data2[data2['사업명'].str.contains(pj, na=False, case=False)]
                
                if not row2.empty:
                    row = row2.iloc[0]
                    schedules = []
                    for i in range(1, 13):
                        d_col, n_col = f'일정{i}', f'일정명{i}'
                        
                        # 값이 존재하고, 일정명도 비어있지 않은지 확인
                        if row.get(d_col) and row.get(n_col):
                            date_val = str(row[d_col])[:8]  # 'YYYY-MM' 형식 추출
                            
                            # [수정] 날짜가 '1900-01'이 아닌 경우에만 리스트에 추가
                            if date_val != '1900-01-':
                                schedules.append({
                                    "날짜": date_val, 
                                    "일정명": row[n_col]
                                })
                    
                    df_schedule = pd.DataFrame(schedules)
                    
                    if not df_schedule.empty:
                        st.dataframe(df_schedule, use_container_width=True, hide_index=True)
                    else:
                        st.info("등록된 유효한 추진 일정이 없습니다.")                    

elif menu == "PF현황":
    st.subheader('📊 PF현황 조회')
    url = "https://docs.google.com/spreadsheets/d/1G4GJIXw36pKUoPgAR2I8yQ0zcTKoscwAoNW5nu7oNPI/edit?gid=0#gid=0"
    data = conn.read(spreadsheet=url, usecols=[0,1,2,3,4,5,6,7,11,12,14])
    
    for col in num_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    
    
    col1, col2 = st.columns(2)
    with col1: pj = st.text_input('사업명 입력')
    with col2: dday = st.selectbox('기준월 선택', sorted(data['기준월'].unique(), reverse=True))       
        
    if st.button('조회'):
        cond = (data['기준월'] == dday)
        if pj: cond &= data['PJ명'].str.contains(pj, na=False, case=False)
        display_search_result(data, cond, target_col='잔액')
        

elif menu == "채권":
    st.subheader('📊 채권현황 조회')
    url = "https://docs.google.com/spreadsheets/d/1RlNYrWWezvHQfceEgmHIkC-c7dnIxRIWZTM3fWdqDWQ/edit?gid=0#gid=0"
    data = conn.read(spreadsheet=url)
    
    for col in num_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    
    
    col1, col2 = st.columns(2)
    with col1: pj = st.text_input('사업명 입력')
    with col2: dday = st.selectbox('기준월 선택 ', sorted(data['기준월'].unique(), reverse=True))

    if st.button('조회'):
        cond = (data['기준월'] == dday)
        if pj: cond &= data['손익센터명'].str.contains(pj, na=False, case=False)
        dff = data[cond]
        
        if not dff.empty:
            st.dataframe(dff, use_container_width=True, hide_index=True, 
                         column_config={"채권": st.column_config.NumberColumn(format="%d")})
            st.divider()
            grouped = dff.groupby(['계정대분류', '계정소분류'], as_index=False)['채권'].sum()
            c1, c2 = st.columns([2, 1])
            with c1: st.dataframe(grouped, use_container_width=True, hide_index=True,
                                  column_config={"채권": st.column_config.NumberColumn(format="%d")})
            with c2: st.metric(label="💰 총 채권 합계", value=f"{dff['채권'].sum():,.0f} 원")
        else:
            st.warning("조회 결과 없음")

elif menu == "중도금":
    st.subheader('🏠 중도금 관리')
    mid_tab = st.selectbox("PJ선택", ["서면", "트라반트", "시민공원"])
    urls = {
        "서면": 'https://docs.google.com/spreadsheets/d/1P-f6lZCK7ln1iJEPBUtQqVGWNy-g7G_5iBDYLnWZB-E/edit?gid=943639489',
        "트라반트": 'https://docs.google.com/spreadsheets/d/1P-f6lZCK7ln1iJEPBUtQqVGWNy-g7G_5iBDYLnWZB-E/edit?gid=453535398',
        "시민공원": 'https://docs.google.com/spreadsheets/d/1P-f6lZCK7ln1iJEPBUtQqVGWNy-g7G_5iBDYLnWZB-E/edit?gid=668236831'
    }
    data = conn.read(spreadsheet=urls[mid_tab])      
    for col in num_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    if not data.empty:
        # 숫자 컬럼에 대해 콤마 포맷 적용 (%d는 정수형)
        col_config = {col: st.column_config.NumberColumn(format="%d") for col in data.select_dtypes(include=['number']).columns}
        
        st.dataframe(data, use_container_width=True, hide_index=True, column_config=col_config)
        
        # 4. 하단 합계 표시 (Metric)
        if '대출잔액' in data.columns:
            total_loan = data['대출잔액'].sum()
            st.metric(label=f"💰 {mid_tab} 대출잔액 합계", value=f"{total_loan:,.0f} 원")
    else:
        st.warning("조회된 데이터가 없습니다.")
    



elif menu == "중도금결산":
    st.subheader('🏠 중도금결산자료')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=67742981"
    pj = st.text_input('사업명 입력:')
    
    if st.button('조회'):
        data = conn.read(spreadsheet=url)        
        # 잔액 수치화
        data['잔액'] = pd.to_numeric(data['잔액'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
        cond = data['사업명'].str.contains(pj, na=False, case=False) if pj else [True] * len(data)
        dff = data[cond][['사업명','상품유형','대출기관','잔액','대출만기일']]
        
        dfp = dff.pivot_table(index=['사업명','상품유형', '대출기관'], columns='대출만기일', values='잔액', 
                              aggfunc='sum', fill_value=0, margins=True, margins_name='합계')
        st.dataframe(dfp.style.format("{:,.0f}"))

elif menu == "분양":
    st.subheader('📊 분양현황')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=391839077#gid=391839077"
    pj = st.text_input('사업명 입력')
    
    search_btn = st.button('조회')
    #with col2: dday = st.selectbox('기준월 선택 ', sorted(data['기준월'].unique(), reverse=True))
    data = conn.read(spreadsheet=url)
    for col in num_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    

    if search_btn:
        #cond = (data['기준월'] == dday)
        if pj:            
            cond = data['사업명'].str.contains(pj, na=False, case=False)        
            data['입주증번호']=pd.to_numeric(data['입주증번호'],errors='coerce').fillna(0)            
            dff = data[cond]    
            ibju = dff['입주증번호'].sum()
            lawsuit = (dff['소송']=='소송').sum() #소송 개수
            if not dff.empty:                
                dff_total = dff.groupby('상품')['동호수'].count().reset_index(name='총공급')
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
                    st.write('동호기준')                                    
                    st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 세대(실), %)</div>', unsafe_allow_html=True)                    
                    st.dataframe(dfp, use_container_width=True, hide_index=True)
                    
                with c2:
                    st.write('금액기준')                    
                    st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)                    
                    st.dataframe(dfp2.style.format(thousands=",", precision=0), use_container_width=True, hide_index=True)                
                
                # 1. 계약 데이터 정리
                # 2. 계약 데이터 정리 (날짜 변환 및 dropna)
                dffg = dff.groupby(['상품', '계약월'])['동호수'].count().reset_index(name='계약건수')
                dffg['날짜'] = pd.to_datetime(dffg['계약월'], errors='coerce')
                dffg = dffg.dropna(subset=['날짜']).sort_values(['상품', '날짜'])
                dffg['누적계약'] = dffg.groupby('상품')['계약건수'].cumsum()
                
                # 3. 완납 데이터 정리 (ibju > 0 조건은 dff 필터링 단계에서 이미 ibju 합계로 확인 가능)
                dff_paid = dff[dff['완납여부'] == '완납'].copy()
                if not dff_paid.empty:
                    dffg_paid = dff_paid.groupby(['상품', '완납월'])['동호수'].count().reset_index(name='완납건수')
                    dffg_paid['날짜'] = pd.to_datetime(dffg_paid['완납월'], errors='coerce')
                    dffg_paid = dffg_paid.dropna(subset=['날짜']).sort_values(['상품', '날짜'])
                    dffg_paid['누적완납'] = dffg_paid.groupby('상품')['완납건수'].cumsum()
                else:
                    dffg_paid = pd.DataFrame(columns=['상품', '날짜', '누적완납'])
            
                # 4. 데이터 통합 및 정렬 (날짜순 정렬 필수)
                combined = pd.merge(
                    dffg[['상품', '날짜', '누적계약']], 
                    dffg_paid[['상품', '날짜', '누적완납']], 
                    on=['상품', '날짜'], 
                    how='outer')
                
            
                # 🌟 [핵심 수정] 정렬 순서를 '날짜' 우선으로 명확히 지정
                # 날짜 기준으로 먼저 줄을 세워야 '날짜표시'를 만들었을 때 순서가 꼬이지 않습니다.
                combined = combined.sort_values(by=['날짜', '상품']).reset_index(drop=True)
                
                # 5. 비율 계산 및 수치 보간
                combined['누적계약'] = combined.groupby('상품')['누적계약'].ffill().fillna(0)
                combined['누적완납'] = combined.groupby('상품')['누적완납'].ffill().fillna(0)
                combined = pd.merge(combined, dff_total, on='상품', how='left')
                
                combined['계약률'] = combined['누적계약'] / combined['총공급']
                combined['완납률'] = combined['누적완납'] / combined['총공급']
                
                sorted_date_labels = sorted(combined['날짜'].dropna().unique())
                sorted_date_strings = [d.strftime('%Y-%m') for d in sorted_date_labels]
                
                combined['날짜표시'] = combined['날짜'].dt.strftime('%Y-%m')
                
                # --- 그래프 그리기 ---
                st.markdown("#### 📈 상품별 월 누적계약률")
                if not combined.empty:
                    fig1 = px.line(combined, x='날짜표시', y='계약률', color='상품', 
                                   markers=True, template="plotly_white")                   
                    
                    # 🌟 [핵심 수정] X축의 순서를 강제로 정렬된 날짜 리스트로 고정합니다.
                    fig1.update_xaxes(
                        type='category', 
                        categoryorder='array', 
                        categoryarray=sorted_date_strings,
                        title="계약월"
                    )
                    fig1.update_yaxes(
                        tickformat=".0%", 
                        range=[0, 1.1],
                        # 보조 눈금선 설정
                        minor=dict(showgrid=True, nticks=10), 
                        gridcolor='lightgray',       # 주요 눈금선
                        #minor_gridcolor='whitesmoke' # 보조 눈금선 (더 밝은 색)
                    )
                    
                    fig1.update_layout(
                        yaxis=dict(tickformat=".0%", range=[0, 1.1])
                    )
                    st.plotly_chart(fig1, use_container_width=True)
                                
                #입주현황표시
                if ibju > 0:
                    st.divider()               
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
                        st.write('동호기준')                
                        st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 세대(실), %)</div>', unsafe_allow_html=True)
                        st.dataframe(dfp3, use_container_width=True, hide_index=True) #hide_index를 하면 인덱스 숨김                    
                    with c4:                        
                        st.write('금액기준')                
                        st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)
                        st.dataframe(dfp4.style.format(thousands=",", precision=0), use_container_width=True, hide_index=True)                        
                    
                    st.markdown("#### ✅ 상품별 월 누적 완납률")
                    # 완납 데이터가 있는 행만 추출
                    paid_plot_df = combined[combined['누적완납'] > 0].copy()
                    
                    if not paid_plot_df.empty:
                        fig2 = px.line(paid_plot_df, x='날짜표시', y='완납률', color='상품', 
                                       markers=True, template="plotly_white",
                                       line_dash='상품') # 계약률과 구분하기 위해 선 스타일 차별화 가능
                        fig2.update_layout(yaxis=dict(tickformat=".0%", range=[0, 1.1]), xaxis=dict(type='category', title="완납월"))
                        st.plotly_chart(fig2, use_container_width=True)
                    else:
                        st.info("입주증 발급 기록은 있으나, 매칭되는 월별 완납 데이터가 없습니다.")
                else:
                    st.write("ℹ️ 입주 전 사업지")
                
                #소송현황표시
                if lawsuit > 0:
                    st.divider()
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
                    dfp5 = dfp5.sort_values(by='상품')                   
                    
                    dfp6 = dff.pivot_table(
                        index='상품', 
                        columns='소송', 
                        values='총분양금', 
                        aggfunc='sum',
                        fill_value=0)        
                    # 2. 백만 단위 변환 및 소수점 처리
                    # 모든 수치형 데이터를 1,000,000으로 나눕니다.
                    dfp6['세대'] = dfp6.sum(axis=1)
                    dfp6 = (dfp6/ 1_000_000).round(0) 
                    
                    if '세대' in dfp6.columns:
                        dfp6 = dfp6.sort_values(by='세대', ascending=False)

                    original_cols6 = [c for c in dfp6.columns if c != '세대'] # 합계 제외한 원래 칼럼들
                    for col in original_cols6:
                        dfp6[f'{col}(%)'] = (dfp6[col] / dfp6['세대'] * 100).round(0).fillna(0)
                    # 3. 데이터프레임 정리 (인덱스 초기화)                
                    dfp6 = dfp6[['세대','소송','미소송','소송(%)','미소송(%)']]
                    dfp6 = dfp6.reset_index()
                    
                    c5, c6 = st.columns(2) # %가 추가되었으므로 비율을 조금 조정                    
                    with c5:
                        st.write('동호기준')                
                        #st.write('(단위 : 세대(실), %)')
                        st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 세대(실), %)</div>', unsafe_allow_html=True)
                        st.dataframe(dfp5, use_container_width=True, hide_index=True) #hide_index를 하면 인덱스 숨김                
                    with c6:                        
                        st.write('금액기준')                
                        st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)
                        st.dataframe(dfp6.style.format(thousands=",", precision=0), use_container_width=True, hide_index=True)
                    
                    
                    st.divider()
                    st.subheader('📊 전체현황')                                        
                    dff_final = dff.groupby(['상품', '소송','완납여부', '계약여부2']).agg({
                        '동호수': 'count',
                        '총분양금': 'sum'
                        }).reset_index()
                    
                    # 해당 컬럼을 Categorical 타입으로 변환 (ordered=True가 핵심)
                    dff_final['상품'] = pd.Categorical(dff_final['상품'], categories=custom_order, ordered=True)                    
                    # '상품'은 오름차순(True), '완납여부'와 '소송'은 내림차순(False)
                    dff_final = dff_final.sort_values(
                        by=['상품', '완납여부', '소송'], 
                        ascending=[True, False, False])
                    st.dataframe(dff_final, use_container_width=True, hide_index=True)                                        
                    
            else:
                st.warning("조회된 결과가 없습니다.")            

elif menu == "실거래조회":        
    if "result_df" not in st.session_state:
        st.session_state.result_df = None  # 또는 pd.DataFrame()
    st.subheader('📊 실거래DB 조회')
    sido_data = load_location_data()

    # --- 3. 사이드바/상단: 검색 조건 설정 ---
    #st.title("실거래 데이터 조회")
    st.markdown('<h3 style="font-size: 18px;">실거래데이터 조회</h3>', unsafe_allow_html=True)

    # URL 선택 (라디오 버튼)
    URL_KEYS = ["분양권", "아파트 매매", "아파트 전월세", "오피스텔 매매", "오피스텔 전월세", "연립/다세대 매매", "연립/다세대 전월세"]
    selected_type = st.radio("🔍 검색 항목 선택", URL_KEYS, horizontal=True, index=1)

    # 입력 프레임 (기존 input_frame_2 재현)
    with st.container():
        col1, col2, col3, col4, col5 = st.columns([1.2, 1.2, 1.2, 1.5, 1.5])
        
        with col1:
            sido = st.selectbox("시도", options=sorted(list(sido_data.keys())), index=8) # 8=서울
        with col2:
            sigungu_options = sorted(list(sido_data[sido].keys())) if sido in sido_data else []
            sigungu = st.selectbox("시군구", options=sigungu_options)
        with col3:
            dong_options = ["전체"] + sorted(sido_data[sido][sigungu]) if sigungu in sido_data.get(sido, {}) else ["전체"]
            dong = st.selectbox("읍면동", options=dong_options)
        with col4:
            sub_col1, sub_col2 = st.columns(2)
            ex_min = sub_col1.selectbox("전용(min)", [10, 20, 30, 40, 59, 84], index=4)
            ex_max = sub_col2.selectbox("전용(max)", [60, 75, 85, 100, 120, 150], index=2)
        with col5:
            default_date = (datetime.date.today() + MonthEnd(-2))
            deal_ymd = st.date_input("기준월(월말)>=", default_date)

    # 조회 및 다운로드 버튼
    btn_col, space, excel_col, etc_col = st.columns([1, 1, 1, 7])

    with btn_col:
        search_clicked = st.button("🚀 조회", use_container_width=True)

    # --- 4. 데이터 조회 로직 (조회 버튼 클릭 시 실행) ---

    if search_clicked:
        try:
            # SQLAlchemy 엔진 생성
            load_dotenv()
            #db_url = f"mysql+pymysql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
            
                      
            engine = get_engine()

            # 지역 그룹 정의
            sma = ['서울특별시', '인천광역시', '경기도']
            big6 = ['부산광역시', '대구광역시', '대전광역시', '광주광역시', '울산광역시', '세종특별자치시']
            dodo = ['강원특별자치도', '충청북도', '충청남도', '전라특별자치도', '전라남도', '경상북도', '경상남도', '제주특별자치도']
            
            table_map = {
                "분양권": "bunyang", "아파트 매매": "sale_sma", "아파트 전월세": "rent_sma",
                "오피스텔 매매": "ot_sale", "오피스텔 전월세": "ot_rent",
                "연립/다세대 매매": "villa_sale", "연립/다세대 전월세": "villa_rent"
            }

            # 테이블 분기 로직
            if selected_type == '아파트 매매':
                if sido in big6:
                    table_name = 'sale_big6'
                elif sido in dodo:
                    table_name = 'sale_dodo'
                else:
                    table_name = 'sale_sma'
            elif selected_type == '아파트 전월세' and sido not in sma:
                table_name = 'rent_notsma'
            else:
                table_name = table_map.get(selected_type, "sale_sma")

            # 쿼리 및 파라미터 구성 (딕셔너리 바인딩 방식)
            query = f"SELECT * FROM {table_name} WHERE 광역시도 = :sido AND 시자치구 = :sigungu AND 기준월 >= :deal_ymd"
            params = {
                "sido": sido, "sigungu": sigungu, 
                "deal_ymd": deal_ymd.strftime('%Y-%m-%d'),
                "ex_min": ex_min, "ex_max": ex_max
            }
            
            if dong != "전체":
                query += " AND 법정동 = :dong"
                params["dong"] = dong
            query += " AND 전용면적 >= :ex_min AND 전용면적 <= :ex_max LIMIT 5000"

            with st.spinner('테이블 조회 중...'):
                with engine.connect() as conn:
                    df = pd.read_sql(text(query), conn, params=params)
            
            # 데이터 정제 및 세션 저장
            if not df.empty:
                df.drop('id', axis=1, inplace=True)                
                st.session_state.result_df = df.reset_index(drop=True)
            else:
                st.session_state.result_df = pd.DataFrame() # 빈 결과 저장        
            engine.dispose()

        except Exception as e:
            st.error(f"조회 중 오류 발생: {e}")

    # --- 5. 결과 출력 (세션 상태를 확인하여 상시 유지) ---
    if st.session_state.result_df is not None:
        df = st.session_state.result_df
        
        if not df.empty:
            st.dataframe(df, use_container_width=True, height=500)
            
            # 검색건수 표시
            st.markdown(f"""
                <div class="status-bar">
                    <span style='font-size: 16px; font-weight: bold;'>📊 검색 결과: </span>
                    <span style='font-size: 26px; color: blue; font-weight: bold;'>{len(df):,}건</span>
                </div>
            """, unsafe_allow_html=True)

            # 엑셀 다운로드 버튼 (제일 오른쪽에 배치)
            with excel_col:
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name='Sheet1')
                buffer.seek(0)
                
                st.download_button(
                    label="📥 엑셀 다운로드",
                    data=buffer,
                    file_name=f"{selected_type}_{deal_ymd}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
        else:
            st.warning("조회된 데이터가 없습니다. 기준월을 과거 날짜로 변경해 보세요.")

# --- 하단 안내 ---
if menu == "옵션선택":

    st.info("왼쪽 사이드바에서 메뉴를 선택해 주세요.")
