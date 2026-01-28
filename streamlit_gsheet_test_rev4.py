# -*- coding: utf-8 -*-
import streamlit as st
from streamlit_option_menu import option_menu 
from streamlit_gsheets import GSheetsConnection 
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import io, os
import datetime
import requests as rq
from pandas.tseries.offsets import MonthEnd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import plotly.express as px

current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)  # 현재 디렉토리로 이동

# --- 설정 및 스타일 ---
st.set_page_config(page_title="구글시트조회", layout="wide")
def check_login():
    """사용자 인증 상태를 확인하고 로그인 화면을 출력합니다."""
    # 세션 상태 초기화
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    # 로그인 되어있지 않은 경우 양식 출력
    if not st.session_state.logged_in:
        st.markdown("""
            <style>
            .login-box {
                max-width: 400px;
                padding: 2rem;
                margin: auto;
                border: 1px solid #ddd;
                border-radius: 10px;
                background-color: #f9f9f9;
            }
            </style>
        """, unsafe_allow_html=True)

        _, col, _ = st.columns([1, 1.5, 1])
        with col:
            st.write("## 🔒 시스템 로그인")
            admin_id = st.text_input("아이디(ID)", placeholder="admin_id 입력")
            admin_pw = st.text_input("비밀번호(Password)", type="password", placeholder="admin_password 입력")
            
            if st.button("로그인", use_container_width=True):
                # ID/PW 검증 (실제 운영 시 st.secrets나 환경변수 사용 권장)
                if admin_id == "admin" and admin_pw == "1234":
                    st.session_state.logged_in = True
                    st.success("인증되었습니다. 대시보드로 이동합니다.")
                    st.rerun()
                else:
                    st.error("아이디 또는 비밀번호가 틀렸습니다.")
        return False
    return True

# 로그인 체크 실행 (성공하지 못하면 아래 코드 실행 안 함)
if not check_login():
    st.stop()
st.markdown("""
    <style>    
    .stDataFrame div[data-testid="stTableHD"] {font-size: 16px !important;}    
    .stDataFrame div[data-testid="stTableCD"] {font-size: 16px !important;}
    .stTable td, .stTable th {font-size: 16px !important;}
    [data-testid="stMetricLabel"] {font-size: 18px !important;}
    [data-testid="stMetricValue"] {font-size: 20px !important;}
    </style>
    """, unsafe_allow_html=True) 
    
# --- 공통 연결 객체 및 함수 ---
conn = st.connection("gsheets", type=GSheetsConnection)

def get_gspread_client():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    # 기존에 사용하시던 JSON 키 경로를 그대로 입력하세요.
    SERVICE_ACCOUNT_FILE = r'K:/pyenv/py311/py_gsheet/python-gsheet-484713-be4d9602c973.json'
    #SERVICE_ACCOUNT_FILE = 'python-gsheet-484713-be4d9602c973.json'
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    return gspread.authorize(creds)

def get_engine():
    load_dotenv()
    db_user = os.getenv("DB_USER")
    db_pw = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_url = f"mysql+pymysql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url)

def style_fill_col(col):    
    style = ['' for _ in col]    
    if col.name in ['계약(%)','완납(%)','소송(%)']:
        #style = ['background-color: yellow' for _ in col]
        style = ['color: yellow' for _ in col] # 컬럼명이 '계약(%)'인 경우에만 노란색 또는 배경 적용
    return style

def style_fill_row(row):
    name = row.name #해당 row(행)의 인덱스(Index) 이름    
    #isinstance(name, tuple): 만약 인덱스가 멀티 인덱스라면 ('대분류', '항목명')처럼 튜플(Tuple) 형태가 됩니다.
    #이 경우 name[1]을 선택해 실제 항목명인 '항목명'만 가져옵니다.
    item_name = name[1] if isinstance(name, tuple) else name        
    if item_name in ['영업이익','원가율','경상이익']:
        return ['background-color: lightgreen'] * len(row)    
    return [''] * len(row)
    #조건에 맞지 않으면: 빈 문자열('')을 반환하여 기본 스타일을 유지합니다.
    # * len(row)를 하는 이유는 행의 모든 칸(Cell) 개수만큼 스타일 정보를 리스트로 전달해야 하기 때문입니다.
def style_by_date(col):
    # 컬럼 이름이 '상품'이나 '구분'이면 스타일 적용 안 함
    if col.name in ['상품', '구분']:
        return [''] * len(col)            
    try:
        # 컬럼명(약정월)을 날짜 객체로 변환
        col_date = pd.to_datetime(col.name)
        # 전월 말일보다 작거나 같으면 회색(#9E9E9E), 아니면 검정색
        color = 'color: #9E9E9E;' if col_date <= threshold_date else 'color: white;'
        return [color] * len(col)
    except:
        return [''] * len(col)


@st.cache_data
def load_sigungu():    
    file_path = "file_content.txt"
    if not os.path.exists(file_path):
        return {}    
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

def get_applyhome_list(area, date):
    load_dotenv()
    mykey1 = os.getenv("mykey1")
    #st.write(mykey1)
    url = (
        "https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getAPTLttotPblancDetail?"
        f"page=1&perPage=3000&cond%5BRCRIT_PBLANC_DE%3A%3AGTE%5D={date}&serviceKey={mykey1}"
    )
    res = rq.get(url)
    if res.status_code == 200:
        data = res.json().get("data", [])
        df = pd.DataFrame(data)
        if not df.empty:
            # 필요한 컬럼 매핑 및 필터링
            col_map = {
                "HOUSE_MANAGE_NO": "주택관리번호",
                "RCRIT_PBLANC_DE": "모집공고일",
                "HOUSE_NM": "주택명",
                "TOT_SUPLY_HSHLDCO": "공급규모",
                "HSSPLY_ADRES": "공급위치",
                "BSNS_MBY_NM": "사업주체명(시행사)",
                "MVN_PREARNGE_YM": "입주예정월",
                "PARCPRC_ULS_AT": "상한제",
                "SUBSCRPT_AREA_CODE_NM": "공급지역명"
            }
            df = df.rename(columns=col_map)
            # 지역 및 날짜 필터링
            df = df[df["공급지역명"] == area]
            return df[["모집공고일", "주택관리번호", "주택명", "공급규모", "공급위치", "사업주체명(시행사)", "입주예정월", "상한제"]]
    return pd.DataFrame()

# 2. API 호출 함수 (상세 타입별 조회)
def get_applyhome_detail(manage_no):
    load_dotenv()
    mykey1 = os.getenv("mykey1")        
    url = (
        "https://api.odcloud.kr/api/ApplyhomeInfoDetailSvc/v1/getAPTLttotPblancMdl?"
        f"page=1&perPage=100&cond%5BHOUSE_MANAGE_NO%3A%3AEQ%5D={manage_no}&serviceKey={mykey1}"
    )
    res = rq.get(url)
    if res.status_code == 200:
        data = res.json().get("data", [])
        df = pd.DataFrame(data)
        if not df.empty:
            df = df[["HOUSE_TY", "SUPLY_AR", "LTTOT_TOP_AMOUNT"]]
            df.columns = ["주택형", "공급면적", "타입최고가"]
            # 1. '타입최고가'에서 숫자가 아닌 모든 문자 제거 후 숫자로 변환
            df['타입최고가'] = pd.to_numeric(df['타입최고가'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce').fillna(0).astype(int)
            df['공급면적'] = pd.to_numeric(df['공급면적'], errors='coerce').fillna(0.0)            
            return df.sort_values(by="공급면적")
            
    return pd.DataFrame()

@st.cache_data
def alv_data():
    url = "https://docs.google.com/spreadsheets/d/1uoL2CDVEi_KPV74eT5VEOjVB7ucCrEYnl9TkNQvstsM/edit?gid=0#gid=0"   
    #url = "https://docs.google.com/spreadsheets/d/1uoL2CDVEi_KPV74eT5VEOjVB7ucCrEYnl9TkNQvstsM/edit?gid=1989275734#gid=1989275734"
    #str_txt = '프로젝트,프로젝트 내역,당월매출,금년매출,누계매출,당월매원,금년매원,누계매원,당사업경비,금사업경비,누사업경비,당용지비,금누계비,누용지비,당월판관비(수주후),금년판관비(수주후),누계판관비(수주후),당월판관비(수주전),금년판관비(수주전),누계판관비(수주전),당월금융비,금년금융비,누계금융비,당현장원가,금현장원가,누현장원가,당공손충,연공손충,누공손충,당월(실)하자보수비,금년(실)하자보수비,누계(실)하자보수비,당기타영업수익,금기타영업수익,누기타영업수익,당기타영업비용,금기타영업비용,누기타영업비용,당이자수익,금이자수익,누이자수익,당이자비용,금이자비용,누이자비용,기준월'
    #col_list = list(str_txt.split(","))
    
    ddf = conn.read(spreadsheet=url)      
    df = pd.DataFrame(ddf)       
    #df = df[col_list]
    # 문자열 컬럼 제외하고 모두 숫자형으로 변환
    base_cols = ['프로젝트', '프로젝트 내역', '기준월']
    for col in df.columns:
        if col not in base_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    dff = df.melt(id_vars=base_cols, var_name='항목', value_name='값')
    # '기간기준' 조건 열 추가 (Power Query의 '조건 열이 추가됨' 단계) : np.select로 효율적으로 구현
    p_conds = [
        dff['항목'].str.contains('누계금'),
        dff['항목'].str.contains('당'),
        dff['항목'].str.contains('금'),
        dff['항목'].str.contains('누'),
        dff['항목'].str.contains('연')]
    p_choices = ['누계', '당월', '금년', '누계', '금년']
    dff['기간기준'] = np.select(p_conds, p_choices, default=None)

    # '항목기준' 조건 열 추가 (Power Query의 '조건 열이 추가됨1' 단계)
    i_conds = [
        dff['항목'].str.contains('매출'),
        dff['항목'].str.contains('매원'),
        dff['항목'].str.contains('경비'),
        dff['항목'].str.contains('판관비'),
        dff['항목'].str.contains('금융비'),
        dff['항목'].str.contains('현장원가'),
        dff['항목'].str.contains('하자보수비'),
        dff['항목'].str.contains('공손충'),
        dff['항목'].str.contains('영업수익'),
        dff['항목'].str.contains('영업비용'),
        dff['항목'].str.contains('이자수익'),
        dff['항목'].str.contains('이자비용'),
        dff['항목'].str.contains('용지비|금누계비') # '용지비' 또는 '금누계비'
    ]
    i_choices = [
        '매출', '매원', '경비', '판관비', '금융비', '공사비', 
        '하자보수비', '공손충', '기타영업수익', '기타영업비용', 
        '이자수익', '이자비용', '토지비']
    dff['항목기준'] = np.select(i_conds, i_choices, default=None)

    # 6. 열 재정렬 (Power Query의 '다시 정렬한 열 수' 단계)
    final_cols = ["프로젝트", "프로젝트 내역", "기준월", "기간기준", "항목기준", "항목", "값"]
    dff = dff[final_cols]
    return dff
  

# --- 사이드바 ---
with st.sidebar:
    menu = option_menu("Manage", ["옵션선택","사업개요","분양","실적조회","PF현황","동호약정", "자금수지","채권", "중도금결산", "중도금","실거래조회", "입주예정","인구","미분양", "pjcode"],  #청약홈조회 제외
                       #icons=["dash","info-circle", "bank", "bank", "bank", "bank","bank","house","house","house","house"],
                       icons=["dash"] + ["info-circle"]*15,
                       menu_icon="cast", default_index=0)
# --- 메뉴별 로직 ---
if menu == "pjcode":
    st.subheader('📊 pjcode 조회/입력')            
    # 1. 인증 및 시트 연결
    client = get_gspread_client()
    spreadsheet_id = '1uoL2CDVEi_KPV74eT5VEOjVB7ucCrEYnl9TkNQvstsM'
    spreadsheet = client.open_by_key(spreadsheet_id)  #구글시트에 쓰기.
        
    col1, col2, col3, col4 = st.columns([2,2,4,2])
    with col1:
        st.write('🔎 pjcode 목록')
        sheet = spreadsheet.worksheet("pjcode")
        data = sheet.get_all_records()
        df = pd.DataFrame(data)        
        st.dataframe(df, use_container_width=True, hide_index=True, height=600)   
        
    with col2:
        st.write('🔎 alv 목록')
        sheet = spreadsheet.worksheet("alv")
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        df_col = df['프로젝트 내역'].drop_duplicates()
        st.dataframe(df_col, use_container_width=True, hide_index=True, height=600)   
        
    with col3:
        st.write('🔎 등록된 pj_pair 목록')
        try:            
            sheet = spreadsheet.worksheet("pj_pair")
            data = sheet.get_all_records()
            if data:
                df_view = pd.DataFrame(data)                
                # [선택] 최신 입력값이 위로 오게 하려면 (역순 정렬)
                df_view = df_view.iloc[::-1]                
                # 데이터프레임 출력
                st.dataframe(df_view, use_container_width=True, hide_index=True, height=600)
            else:
                st.info("현재 등록된 데이터가 없습니다.")
        except Exception as e:
            st.error(f"데이터 로드 중 오류 발생: {e}")
        
    with col4:  
        st.write('➕ pj_pair 신규입력')
        with st.container():
            pj = st.text_input('본공사 입력:')
            pjo = st.text_input('옵션공사 입력:')                    
            if st.button('저장'):
                if pj and pjo:
                    try:                        
                        # 데이터 쓰기 (append_row 사용)                        
                        new_row = [pj, pjo]
                        sheet.append_row(new_row)                                            
                        st.success(f"✅ 저장 성공: {pj} / {pjo}")
                        st.balloons()                        
                        # [중요] 저장 후 화면 갱신을 위해 재실행
                        st.rerun()                         
                    except Exception as e:
                        st.error(f"❌ gspread 쓰기 오류 발생: {e}")
                else:
                    st.warning("⚠️ 본공사와 옵션공사 코드를 모두 입력해주세요.")


elif menu == "사업개요":
    st.subheader('📊 사업개요')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=0#gid=0"    
    
    data = conn.read(spreadsheet=url, usecols=list(range(15))).fillna("")        
    pj_list = ["전체 조회"] + data['사업명'].drop_duplicates().tolist()        
    sel_pj = st.selectbox('조회할 사업명을 선택하세요', pj_list)        
    
    if st.button('조회'):
        data2 = conn.read(spreadsheet=url, usecols=[1] + list(range(17, 41))).fillna("")        
        # 필터링 조건 설정
        is_all = sel_pj == "전체 조회"  #sel_pj이 "전체 조회"인지 확인합니다.결과는 True 또는 False (Boolean) 값으로 is_all 변수에 저장
        dff = data if is_all else data[data['사업명'] == sel_pj] #파이썬 삼항연산자        
        if not dff.empty:
            if not is_all:                
                col1, col2, col3 = st.columns([3, 3, 2])                
                with col1:
                    st.write(f"### 조감도")                    
                    img_path = f"image/{sel_pj}.jpg"
                    try:
                        #st.image(os.path.join(os.getcwd(), "image", sel_pj+".jpg"), width=50)
                        st.image(img_path, use_container_width=True)
                    except:
                        st.warning("등록된 조감도가 없습니다.")
                
                with col2:
                    st.subheader('개요')
                    dfft = dff.iloc[[0]].T.reset_index()
                    dfft.columns = ['구분', '내용']
                    st.dataframe(dfft, use_container_width=True, hide_index=True, height=500)
                
                with col3:
                    st.subheader('추진일정')
                    row2 = data2[data2['사업명'] == sel_pj]
                    
                    if not row2.empty:
                        row = row2.iloc[0]
                        schedules = []
                        for i in range(1, 13):
                            d_col, n_col = f'일정{i}', f'일정명{i}'
                            if row.get(d_col) and row.get(n_col):
                                date_val = str(row[d_col])[:10]
                                if "1900-01" not in date_val:
                                    schedules.append({"날짜": date_val, "일정명": row[n_col]})
                        
                        df_schedule = pd.DataFrame(schedules)
                        if not df_schedule.empty:
                            st.dataframe(df_schedule, use_container_width=True, hide_index=True)
                        else:
                            st.info("등록된 추진 일정이 없습니다.")
            else:
                # 전체 목록 표시
                st.info("전체 목록을 표시합니다.")
                st.dataframe(dff, use_container_width=True, hide_index=True)
        else:
            st.error("결과를 찾을 수 없습니다.")


elif menu == "PF현황":
    st.subheader('📊 PF현황 조회')
    url = "https://docs.google.com/spreadsheets/d/1G4GJIXw36pKUoPgAR2I8yQ0zcTKoscwAoNW5nu7oNPI/edit?gid=0#gid=0"
    ddf = conn.read(spreadsheet=url, usecols=[0,1,2,3,4,5,6,7,11,12,14])
    ncols = ['약정','기표','상환','잔액']  #숫자칼럼 명시
    for col in ncols:
        if col in ddf.columns:
            ddf[col] = pd.to_numeric(ddf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    
    
    col1, col2 = st.columns(2)
    pj_list = ddf['PJ명'].drop_duplicates().tolist()        
    with col1: pj = st.selectbox('조회할 사업명을 선택하세요', pj_list)            
    with col2: dday = st.selectbox('기준월 선택', sorted(ddf['기준월'].unique(), reverse=True))       
        
    if st.button('조회'):
        cond = (ddf['기준월'] == dday)
        if pj:
            cond &= ddf['PJ명'].str.contains(pj, na=False, case=False)
            dff = ddf[cond].copy()
            if not dff.empty:
                ncols = dff.select_dtypes(include=['number']).columns
                config = {col: st.column_config.NumberColumn(format="%d") for col in ncols}
                st.dataframe(dff, use_container_width=True, hide_index=True, column_config=config)
                target_col = '잔액'
                if target_col and target_col in dff.columns:
                    total_val = dff[target_col].sum()            
                    st.metric(label=f"💰 {target_col} 합계", value=f"{total_val:,.0f} 원")
        else:
            st.warning("조회된 결과가 없습니다.")            


elif menu == "동호약정":
    st.subheader('📊 동호약정 납입현황')
    
    sid = {
        '벤처밸리': 'https://docs.google.com/spreadsheets/d/1N1qhgvhoVBWtuF6LfBPjaRGX6kawiUWpc0bJb8vBDgM/edit?gid=0#gid=0',
        '시민공원': 'https://docs.google.com/spreadsheets/d/1N1qhgvhoVBWtuF6LfBPjaRGX6kawiUWpc0bJb8vBDgM/edit?gid=767298303#gid=767298303',
        '시화디오션': 'https://docs.google.com/spreadsheets/d/1N1qhgvhoVBWtuF6LfBPjaRGX6kawiUWpc0bJb8vBDgM/edit?gid=1762659893#gid=1762659893',
        }
    opt = sid.keys()
    sel_pj = st.selectbox('사업명 선택', opt)

    if st.button('조회'):
        url = sid[sel_pj]      
        df = conn.read(spreadsheet=url)    

        # [수정 포인트 1] 그룹화하기 전, 가장 먼저 숫자형으로 변환합니다.
        # 금액 컬럼들에 콤마(,)가 있을 수 있으므로 제거 후 변환합니다.
        numeric_cols = ["약정금액", "납부원금"] 
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

        df["약정일자"] = pd.to_datetime(df["약정일자"])    
        df["동호수"] = df["동"].astype(str) + "-" + df["호수"].astype(str)
        df["상품"] = df["세대속성"].str.split("/").str[0]    
        df = df[df["주택형"] != "소계"].copy()    
        df["약정월"] = df["약정일자"] + pd.offsets.MonthEnd(0) 

        # [포인트 2] 이제 그룹화를 하면 숫자로 합산됩니다.
        dfg = df.groupby(["상품", "약정월", "차수", "동호수"], as_index=False).agg({
            "약정금액": "sum",
            "납부원금": "sum"})
        dfg = dfg[dfg["약정월"].notnull()]
        dfg = dfg.sort_values("약정월")    
        dfg["차수구분"] = dfg["차수"].str[:2]
        # [포인트 3] 재그룹화
        dfg2 = dfg.groupby(["상품", "약정월", "차수", "차수구분"], as_index=False).agg({
            "납부원금": "sum",
            "약정금액": "sum"}).rename(columns={"약정금액": "약정원금"})    
        dfg2 = dfg2.sort_values(["상품", "약정월"])        
        df_melted = dfg2.melt(id_vars=['상품', '약정월'], value_vars=['약정원금', '납부원금',], var_name='구분')        
        # 날짜를 문자열로 변환 (YYYY-MM)
        df_melted['약정월'] = df_melted['약정월'].dt.strftime('%Y-%m')        
        # 피벗 테이블 생성
        dfp = df_melted.pivot_table(
            index=['상품', '구분'], 
            columns='약정월', 
            values='value', 
            aggfunc='sum', 
            fill_value=0
        )
        # 14. 백만원 단위 변환 및 출력
        dfp = dfp / 1000000          
        dfp = dfp.reset_index()

        custom_product_order = ['아파트', '오피스텔', '생활숙박시설', '지식산업센터', '판매시설', '상가']
        custom_type_order = ['약정원금', '납부원금'] # 약정원금이 앞으로 오도록 설정
        # 3. 카테고리 설정 (상품 & 구분 둘 다 적용)
        dfp['상품'] = pd.Categorical(dfp['상품'], categories=custom_product_order, ordered=True)
        dfp['구분'] = pd.Categorical(dfp['구분'], categories=custom_type_order, ordered=True)
        # 4. [핵심] 상품 순서대로 먼저, 그 안에서 구분 순서대로 정렬
        dfp = dfp.sort_values(by=['상품', '구분']).reset_index(drop=True)
        # 1. 오늘 기준 전월 말일 계산
        today = datetime.date.today()
        first_day_of_this_month = today.replace(day=1)
        last_day_of_last_month = first_day_of_this_month - datetime.timedelta(days=1)
        # 비교를 위해 Timestamp 형식으로 변환
        threshold_date = pd.Timestamp(last_day_of_last_month)        
        
        
        # 3. 스타일 적용 및 출력        
        styled_dfp = dfp.style.apply(style_by_date, axis=0).format(thousands=",", precision=0)
        
        st.dataframe(styled_dfp, use_container_width=True, hide_index=True)                
        
        
elif menu == "자금수지":
    st.subheader('📊 자금수지 조회')
    url = "https://docs.google.com/spreadsheets/d/18AhC-xVCGMpapdZwpptxnkED3_sO18B7qDeKz-4oa60/edit?gid=0#gid=0"
    data = conn.read(spreadsheet=url)
    ncols = []
    for col in ncols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    col1, col2 = st.columns(2)
    with col1: pj = st.text_input('사업명 입력')
    with col2: dday = st.selectbox('기준월 선택', sorted(data['기준월'].unique(), reverse=True))       
    if st.button('조회'):
        cond = (data['기준월'] == dday)
        if pj:
            cond &= data['pjcode'].str.contains(pj, na=False, case=False)
    
        dff = data[cond].copy()
        if not dff.empty:
            #st.dataframe(dff, use_container_width=True, hide_index=True)            
            dff['집행월'] = pd.to_datetime(dff['집행월'])
            dff['금액'] = pd.to_numeric(dff['금액'])
            dfp = dff.pivot_table(index=['구분'], columns='집행월', values='금액', aggfunc='sum', fill_value=0)            
            # 2. 과부족 계산 (수입 - 지출) : '수입'과 '지출' 행이 데이터에 있는지 확인 후 계산
            #대괄호 두 개([[...]])를 쓴 이유: 결과값을 단일 값이 아닌 데이터프레임 형태로 유지
            #인덱스가 '수입'인 모든 칼럼의 합을 구해서, 집행월이 index인 series를 반환            
            income = dfp.loc[['수입']].sum() if '수입' in dfp.index else 0
            expense = dfp.loc[['지출']].sum() if '지출' in dfp.index else 0
            
            shortage = income - expense
            shortage.name = '과부족'            
            # 3. 누계 과부족 계산 (열 방향으로 누적 합계)
            cum_shortage = shortage.cumsum()
            cum_shortage.name = '누계과부족'
            
            # 4. 기존 피벗 테이블에 과부족, 누계과부족 행 추가
            # shortage와 cum_shortage (series)를 데이터프레임 형태로 변환하여 결합
            dfp = pd.concat([dfp, shortage.to_frame().T, cum_shortage.to_frame().T])
            def style_past_dates(col):
                # 기준 날짜 (2025-12-31) : dday로 변경할 것.
                threshold = pd.Timestamp('2025-12-31')
                # 컬럼 이름(col.name)이 Timestamp 객체인지 확인 후 비교
                color = 'color: gray;' if col.name <= threshold else 'color: black;'
                return [color] * len(col)

            # 6. 스타일 적용 (아직 컬럼이 날짜 객체일 때 적용해야 함)
            styled_dfp = dfp.style.apply(style_past_dates)
            
            # 7. [출력 필터링] 2025-01-31 이후 컬럼만 선택
            display_cols = [c for c in dfp.columns if c > pd.Timestamp('2025-01-31')]
            dfp_to_show = dfp[display_cols].copy()

            # 8. 스타일 함수 보강 (문자열 날짜가 들어와도 에러 안 나게 처리)
            def style_past_dates(col):
                threshold = pd.Timestamp('2025-12-31')
                # 컬럼 이름이 문자열일 경우를 대비해 변환 후 비교
                current_date = pd.to_datetime(col.name)
                color = 'color: #9E9E9E;' if current_date <= threshold else 'color: white;'
                return [color] * len(col)

            # 9. 스타일 및 포맷 적용 (thousands로 수정)
            # 만약 0을 숨기고 싶다면 .format(lambda v: "" if v == 0 else f"{v:,.0f}") 사용
            styled_dfp = dfp_to_show.style.apply(style_past_dates).format(thousands=",")

            # 10. 컬럼명을 깔끔하게 문자열로 변환 (출력용)
            dfp_to_show.columns = [c.strftime('%Y-%m-%d') for c in dfp_to_show.columns]
            
            # 최종 출력
            st.dataframe(styled_dfp, use_container_width=True)
                     
# =============================================================================
#             target_date = pd.to_datetime('2024-12-31')
#             dfp = dfp.loc[:, dfp.columns > target_date]            
#             # 6. 보기 좋게 컬럼명을 문자열(YYYY-MM)로 변환
#             dfp.columns = dfp.columns.strftime('%y/%m')            
# =============================================================================
        else:
            st.warning("조회된 결과가 없습니다.")       
    
        
elif menu == "채권":
    st.subheader('📊 채권현황 조회')
    url = "https://docs.google.com/spreadsheets/d/1RlNYrWWezvHQfceEgmHIkC-c7dnIxRIWZTM3fWdqDWQ/edit?gid=0#gid=0"
    ddf = conn.read(spreadsheet=url)
    ncols = ['채권', '불량', '잔액', '총분양금', '대출잔액']
    for col in ncols:
        if col in ddf.columns:
            ddf[col] = pd.to_numeric(ddf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    
    
    col1, col2 = st.columns(2)
    pj_list = ddf['손익센터명'].drop_duplicates().tolist()        
    with col1: pj = st.selectbox('조회할 사업명을 선택하세요', pj_list)            
    with col2: dday = st.selectbox('기준월 선택 ', sorted(ddf['기준월'].unique(), reverse=True))

    if st.button('조회'):
        cond = (ddf['기준월'] == dday)
        if pj: cond &= ddf['손익센터명'].str.contains(pj, na=False, case=False)
        dff = ddf[cond]        
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
    ddf = conn.read(spreadsheet=urls[mid_tab])      
    ncols =['대출잔액']
    for col in ncols:
        if col in ddf.columns:
            ddf[col] = pd.to_numeric(ddf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    if not ddf.empty:
        # 숫자 컬럼에 대해 콤마 포맷 적용 (%d는 정수형)
        col_config = {col: st.column_config.NumberColumn(format="%d") for col in ddf.select_dtypes(include=['number']).columns}
        
        st.dataframe(ddf, use_container_width=True, hide_index=True, column_config=col_config)
        
        # 4. 하단 합계 표시 (Metric)
        if '대출잔액' in ddf.columns:
            total_loan = ddf['대출잔액'].sum()
            st.metric(label=f"💰 {mid_tab} 대출잔액 합계", value=f"{total_loan:,.0f} 원")
    else:
        st.warning("조회된 데이터가 없습니다.")
    

elif menu == "중도금결산":
    st.subheader('🏠 중도금결산자료')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=67742981"
    ddf = conn.read(spreadsheet=url)    
    ncols =['잔액']
    for col in ncols:
        if col in ddf.columns:
            ddf[col] = pd.to_numeric(ddf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

    pj_list = ddf['사업명'].drop_duplicates().tolist()            
    pj = st.selectbox('조회할 사업명을 선택하세요', pj_list)                   
    if st.button('조회'):
        cond = ddf['사업명'].str.contains(pj, na=False, case=False) if pj else [True] * len(ddf)
        dff = ddf[cond]        
        dff = ddf[cond][['사업명','상품유형','대출기관','잔액','대출만기일']]
        col1, col2 = st.columns([6,4])
        with col1:
            if not dff.empty:                                                  
                dfp = dff.pivot_table(index=['사업명','상품유형', '대출기관'], columns='대출만기일', values='잔액', 
                                      aggfunc='sum', fill_value=0, margins=True, margins_name='합계')
                st.dataframe(dfp.style.format("{:,.0f}"))
            else:
                st.warning("조회된 데이터가 없습니다.")
  

elif menu == "분양":
    st.subheader('📊 분양현황')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=391839077#gid=391839077"
    data = conn.read(spreadsheet=url)   
           
    ncols = ['입주증번호','총분양금']
    for col in ncols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    

    pj_list = data['사업명'].drop_duplicates().tolist()    
    pj = st.selectbox('사업명 선택', pj_list)    
    search_btn = st.button('조회')
    
    if search_btn:        
        if pj:            
            cond = data['사업명'].str.contains(pj, na=False, case=False)                    
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
                    styled_df = dfp.style.apply(style_fill_col, axis=0)
                    styled_df = styled_df.format({'계약(%)': '{:.0f}', '미계약(%)': '{:.0f}'})
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)                    
                    #st.dataframe(dfp, use_container_width=True, hide_index=True)
                    
                    
                with c2:
                    st.write('금액기준')                    
                    st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)                    
                    styled_df = dfp2.style.apply(style_fill_col, axis=0)
                    styled_df = styled_df.format({'공급': '{:,.0f}', '계약': '{:,.0f}', '미계약': '{:,.0f}',
                                                  '계약(%)': '{:.0f}', '미계약(%)': '{:.0f}',
                                                  })
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)                    
                    #st.dataframe(dfp2.style.format(thousands=",", precision=0), use_container_width=True, hide_index=True)                
                
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
                st.markdown("#### 📈 상품별 월별 누적계약률")
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
                        styled_df = dfp3.style.apply(style_fill_col, axis=0)
                        styled_df = styled_df.format({'완납(%)': '{:.0f}', '미납(%)': '{:.0f}'})
                        st.dataframe(styled_df, use_container_width=True, hide_index=True)                    
                        #st.dataframe(dfp3, use_container_width=True, hide_index=True) #hide_index를 하면 인덱스 숨김                    
                    with c4:                        
                        st.write('금액기준')                
                        st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)
                        styled_df = dfp4.style.apply(style_fill_col, axis=0)
                        styled_df = styled_df.format({'공급': '{:,.0f}', '완납': '{:,.0f}', '미납': '{:,.0f}',
                                                      '완납(%)': '{:.0f}', '미납(%)': '{:.0f}',
                                                      })
                        st.dataframe(styled_df, use_container_width=True, hide_index=True)                    
                        #st.dataframe(dfp4.style.format(thousands=",", precision=0), use_container_width=True, hide_index=True)                        
                    
                    st.markdown("#### ✅ 상품별 월별 누적완납률")
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
                        st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 세대(실), %)</div>', unsafe_allow_html=True)
                        styled_df = dfp5.style.apply(style_fill_col, axis=0)
                        styled_df = styled_df.format({'소송(%)': '{:.0f}', '미소송(%)': '{:.0f}'})
                        st.dataframe(styled_df, use_container_width=True, hide_index=True)                    
                        #st.dataframe(dfp5, use_container_width=True, hide_index=True) #hide_index를 하면 인덱스 숨김                
                    with c6:                        
                        st.write('금액기준')                
                        st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)
                        styled_df = dfp6.style.apply(style_fill_col, axis=0)
                        styled_df = styled_df.format({'세대': '{:,.0f}', '소송': '{:,.0f}', '미소송': '{:,.0f}',
                                                      '소송(%)': '{:.0f}', '미소송(%)': '{:.0f}',
                                                      })
                        st.dataframe(styled_df, use_container_width=True, hide_index=True)                    
                        #st.dataframe(dfp6.style.format(thousands=",", precision=0), use_container_width=True, hide_index=True)
                    
                    
                    st.divider()
                    st.subheader('📊 전체현황')                                        
                    dff_final = dff.groupby(['상품', '소송','완납여부', '계약여부2']).agg({
                        '동호수': 'count',
                        '총분양금': 'sum'
                        }).reset_index()
                    
                    dff_final['총분양금'] = (dff_final['총분양금'] / 1_000_000).round(0) 
                    # 해당 컬럼을 Categorical 타입으로 변환 (ordered=True가 핵심)
                    dff_final['상품'] = pd.Categorical(dff_final['상품'], categories=custom_order, ordered=True)                    
                    # '상품'은 오름차순(True), '완납여부'와 '소송'은 내림차순(False)
                    dff_final = dff_final.sort_values(
                        by=['상품', '완납여부', '소송'], 
                        ascending=[True, False, False])
                    st.dataframe(dff_final.style.format(thousands=",", precision=0), use_container_width=True, hide_index=True)
                    
                    
            else:
                st.warning("조회된 결과가 없습니다.")            


elif menu == '실적조회':
    st.write("SAP실적조회")
# =============================================================================
#     url = "https://docs.google.com/spreadsheets/d/1uoL2CDVEi_KPV74eT5VEOjVB7ucCrEYnl9TkNQvstsM/edit?gid=391726001#gid=391726001"    
#     data = conn.read(spreadsheet=url)      
#     df_raw = pd.DataFrame(data)
# =============================================================================
    df_raw = alv_data()
    
    # 숫자형 변환 (중요: 문자열로 섞여 있으면 연산 안됨)
    df_raw['값'] = pd.to_numeric(df_raw['값'], errors='coerce').fillna(0)

    col1, col2, col3 = st.columns([3,3,3])
    #with col1: pj = st.text_input('사업명 입력')
    with col1:
        pj = st.selectbox('(본공사)사업명 선택', sorted(df_raw['프로젝트 내역'].unique()))
        url_pair = 'https://docs.google.com/spreadsheets/d/1uoL2CDVEi_KPV74eT5VEOjVB7ucCrEYnl9TkNQvstsM/edit?gid=1549480112#gid=1549480112'
        data1 = conn.read(spreadsheet=url_pair)      
        df1 = pd.DataFrame(data1)
        pj2 = next(iter(df1.loc[df1['pj'] == pj, 'pjo']), None)
        
    
    with col2: dday = st.selectbox('기준월 선택', sorted(df_raw['기준월'].unique(), reverse=True))               
    with col3:     
        st.markdown('<p style="margin-bottom: 28px;"></p>', unsafe_allow_html=True)
        sch_button = st.button('조회')
        
    if sch_button:
        all_dfs = []
        with col1:
            st.write("본공사")
            cond = (df_raw['기준월'] == dday)
            if pj:
                cond &= df_raw['프로젝트 내역'].str.contains(pj, na=False, case=False)        
        
            dff = df_raw[cond].copy()
    
            if dff.empty:
                st.warning("조회된 데이터가 없습니다.")
            else:
                idx_cols = ['프로젝트', '프로젝트 내역', '기준월', '기간기준']
                
                # 1. 헬퍼 함수들
                def get_series(df, category):
                    return df[df['항목기준'] == category].groupby(idx_cols)['값'].sum()
    
                def create_row(series, name):
                    # 연산 결과(Series)를 DataFrame으로 변환하고 컬럼명을 '값'으로 지정
                    res = series.to_frame(name='값').reset_index()
                    res['항목기준'] = name
                    return res
    
                # 2. 항목별 Series 추출
                s_sales = get_series(dff, '매출')
                s_cost = get_series(dff, '매원')
                s_sg_a = get_series(dff, '판관비')
                s_oth_i = get_series(dff, '기타영업수익')
                s_oth_e = get_series(dff, '기타영업비용')
                s_int_i = get_series(dff, '이자수익')
                s_int_e = get_series(dff, '이자비용')
                s_fin = get_series(dff, '금융비')
    
                # 3. 산식 계산 (fill_value=0 필수)
                gp_val = s_sales.sub(s_cost, fill_value=0) # 매출이익
                op_val = gp_val.sub(s_sg_a, fill_value=0)  # 영업이익
                ord_val = (op_val.add(s_oth_i, fill_value=0)
                                 .sub(s_oth_e, fill_value=0)
                                 .add(s_int_i, fill_value=0)
                                 .sub(s_int_e, fill_value=0)
                                 .sub(s_fin, fill_value=0)) # 경상이익
                
                cost_ratio_val = (s_cost.div(s_sales.replace(0, np.nan), fill_value=0).fillna(0) * 100)  
                cost_ratio_df = create_row(cost_ratio_val, '원가율')
                gp_df = create_row(gp_val, '매출이익')
                op_df = create_row(op_val, '영업이익')
                ord_df = create_row(ord_val, '경상이익')
    
                # 기존 dff와 계산된 지표들 병합                
                dffc = pd.concat([dff, gp_df, op_df, ord_df, cost_ratio_df], ignore_index=True)    
                # 6. 정렬 및 피벗
                corder = ['매출', '토지비', '공사비', '경비', '공손충', '하자보수비', '매원','원가율', '매출이익',
                          '판관비', '영업이익', '기타영업수익', '기타영업비용', '금융비', '이자수익', '이자비용', '경상이익']                
                dffc['항목기준'] = pd.Categorical(dffc['항목기준'], categories=corder, ordered=True)
                
                # 기간기준 정렬 순서 정의
                period_order = ['당월', '금년', '누계']                
                # 기간기준 컬럼을 카테고리형으로 변환 (순서 고정)
                dffc['기간기준'] = pd.Categorical(dffc['기간기준'], categories=period_order, ordered=True)                
                               
                
                dffc_main = dffc.copy() # 본공사 결과 저장
                all_dfs.append(dffc_main)
                
                # 피벗 테이블 생성 (이제 행과 열 모두 지정한 순서대로 정렬됩니다)                
                dffp_main = dffc_main.pivot_table(index=['프로젝트', '항목기준'], columns='기간기준', values='값', aggfunc='sum')
                styled_main = dffp_main.style.apply(style_fill_row, axis=1).format("{:,.0f}")
                st.dataframe(styled_main, use_container_width=True, height=630)
                             
        with col2:
            if pj2:
                st.write("옵션공사")
                cond = (df_raw['기준월'] == dday)            
                cond &= df_raw['프로젝트 내역'].str.contains(pj2, na=False, case=False)        
        
                dff = df_raw[cond].copy()
    
                if dff.empty:
                    st.warning("조회된 데이터가 없습니다.")
                else:
                    idx_cols = ['프로젝트', '프로젝트 내역', '기준월', '기간기준']
                    
                    # 1. 헬퍼 함수들
                    def get_series(df, category):
                        return df[df['항목기준'] == category].groupby(idx_cols)['값'].sum()
        
                    def create_row(series, name):
                        # 연산 결과(Series)를 DataFrame으로 변환하고 컬럼명을 '값'으로 지정
                        res = series.to_frame(name='값').reset_index()
                        res['항목기준'] = name
                        return res
        
                    # 2. 항목별 Series 추출
                    s_sales = get_series(dff, '매출')
                    s_cost = get_series(dff, '매원')
                    s_sg_a = get_series(dff, '판관비')
                    s_oth_i = get_series(dff, '기타영업수익')
                    s_oth_e = get_series(dff, '기타영업비용')
                    s_int_i = get_series(dff, '이자수익')
                    s_int_e = get_series(dff, '이자비용')
                    s_fin = get_series(dff, '금융비')
        
                    # 3. 산식 계산 (fill_value=0 필수)
                    gp_val = s_sales.sub(s_cost, fill_value=0) # 매출이익
                    op_val = gp_val.sub(s_sg_a, fill_value=0)  # 영업이익
                    ord_val = (op_val.add(s_oth_i, fill_value=0)
                                     .sub(s_oth_e, fill_value=0)
                                     .add(s_int_i, fill_value=0)
                                     .sub(s_int_e, fill_value=0)
                                     .sub(s_fin, fill_value=0)) # 경상이익
        
                    # 4. 새로운 행 데이터 생성
                    cost_ratio_val = (s_cost.div(s_sales.replace(0, np.nan), fill_value=0).fillna(0) * 100)  
                    cost_ratio_df = create_row(cost_ratio_val, '원가율')
                    gp_df = create_row(gp_val, '매출이익')
                    op_df = create_row(op_val, '영업이익')
                    ord_df = create_row(ord_val, '경상이익')
        
                    # 5. 기존 dff와 계산된 지표들 병합                    
                    dffc = pd.concat([dff, gp_df, op_df, ord_df, cost_ratio_df], ignore_index=True)
        
                    # 6. 정렬 및 피벗
                    corder = ['매출', '토지비', '공사비', '경비', '공손충', '하자보수비', '매원', '원가율','매출이익',
                              '판관비', '영업이익', '기타영업수익', '기타영업비용', '금융비', '이자수익', '이자비용', '경상이익']                    
                    dffc['항목기준'] = pd.Categorical(dffc['항목기준'], categories=corder, ordered=True)
                    
                    # 1. 기간기준 정렬 순서 정의
                    period_order = ['당월', '금년', '누계']                    
                    # 2. 기간기준 컬럼을 카테고리형으로 변환 (순서 고정)
                    dffc['기간기준'] = pd.Categorical(dffc['기간기준'], categories=period_order, ordered=True)
                                        
                    
                    dffc_opt = dffc.copy() # 옵션공사 결과 저장
                    all_dfs.append(dffc_opt)
                    
                    # 옵션공사 테이블 출력 (기존 코드 유지)
                    dffp_opt = dffc_opt.pivot_table(index=['프로젝트', '항목기준'], columns='기간기준', values='값', aggfunc='sum')
                    styled_opt = dffp_opt.style.apply(style_fill_row, axis=1).format("{:,.0f}")
                    st.dataframe(styled_opt, use_container_width=True, height=630)
                    
# =============================================================================
#                     # 4. 피벗 테이블 생성 (이제 행과 열 모두 지정한 순서대로 정렬됩니다)
#                     dffp = dffc.pivot_table(index=['프로젝트', '항목기준'], columns='기간기준', values='값', aggfunc='sum')                    
#                     # .format("{:,.0f}")을 함께 써주면 천 단위 콤마와 배경색을 동시에 적용할 수 있습니다.
#                     styled_dffp = dffp.style.apply(style_fill_row, axis=1).format("{:,.0f}")                
#                     # 3. Streamlit에 출력
#                     st.dataframe(styled_dffp, use_container_width=True, height=600)
# =============================================================================
            
        with col3:
            if pj2:
                st.write("본공사+옵션공사 합계")                
                # 1. 모든 데이터를 하나로 합침
                total_df = pd.concat(all_dfs, ignore_index=True)                
                # 2. '프로젝트'명을 '합계'로 통일 (피벗 시 행을 하나로 합치기 위함)
                total_df['프로젝트'] = '합계'
                
                # 3. 정렬 순서 재지정 (concat 후 카테고리 속성이 풀릴 수 있으므로 다시 설정)
                total_df['항목기준'] = pd.Categorical(total_df['항목기준'], categories=corder, ordered=True)
                total_df['기간기준'] = pd.Categorical(total_df['기간기준'], categories=period_order, ordered=True)              
                                
                # 4. 피벗 테이블 생성
                dffp_total = total_df.pivot_table(index=['프로젝트', '항목기준'], columns='기간기준', values='값', aggfunc='sum')                
                
                try:
                    # 피벗 테이블에서 매출과 매원 행 추출 (인덱스 구조에 주의)
                    # dffp_total.loc[('합계', '항목명')] 형태로 접근
                    total_sales = dffp_total.loc[('합계', '매출')]
                    total_cost = dffp_total.loc[('합계', '매원')]            
                    # 총 매원 / 총 매출 (매출이 0인 경우 대비)
                    total_ratio = (total_cost / total_sales.replace(0, np.nan)).fillna(0) * 100                    
                    # 피벗 테이블의 '원가율' 행 업데이트
                    dffp_total.loc[('합계', '원가율'), :] = total_ratio
                except KeyError:
                    # 데이터에 매출이나 매원이 없는 경우 대비
                    pass
                
                # 5. 스타일 적용 및 출력
                styled_total = dffp_total.style.apply(style_fill_row, axis=1).format("{:,.0f}")
                st.dataframe(styled_total, use_container_width=True, height=630)        


                
elif menu == "실거래조회":        
    if "result_df" not in st.session_state:
        st.session_state.result_df = None  # 또는 pd.DataFrame()
    st.subheader('📊 실거래DB 조회')
    sido_data = load_sigungu()

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
            load_dotenv()            
            engine = get_engine()
            sma = ['서울특별시', '인천광역시', '경기도']
            big6 = ['부산광역시', '대구광역시', '대전광역시', '광주광역시', '울산광역시', '세종특별자치시']
            dodo = ['강원특별자치도', '충청북도', '충청남도', '전라특별자치도', '전라남도', '경상북도', '경상남도', '제주특별자치도']
            
            table_map = {
                "분양권": "bunyang", "아파트 매매": "sale_sma", "아파트 전월세": "rent_sma",
                "오피스텔 매매": "ot_sale", "오피스텔 전월세": "ot_rent",
                "연립/다세대 매매": "villa_sale", "연립/다세대 전월세": "villa_rent"}
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
                "ex_min": ex_min, "ex_max": ex_max}
            
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
                    use_container_width=True)
        else:
            st.warning("조회된 데이터가 없습니다. 기준월을 과거 날짜로 변경해 보세요.")


elif menu == "입주예정":
    st.subheader('🏠 아파트 입주예정(부동산지인)')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=1029648553#gid=1029648553"
    ddf = conn.read(spreadsheet=url)        
    num_col = ['세대수','기준년']
    for col in num_col:
        if col in ddf.columns:
            ddf[col] = pd.to_numeric(ddf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    
    #df = df[df['기준년'] > 2025]
    ddf.loc[ddf['시도'] == '강원특별자치도', '시도'] = '강원도'        
    dff = ddf[ddf['구분']=='아파트']    
    #chart는 인덱스를 그대로 쓰고, px는 reset_index()를 해서 x값으로 쓴다.
    dfp = dff.pivot_table(index='기준년', values='세대수', aggfunc='sum', fill_value=0).reset_index()    
    fig1 = px.bar(dfp, x='기준년', y='세대수', 
                  template="plotly_white",
                  text='세대수') # 막대 위에 값 표시
    # 4. 차트 디테일 설정 (막대 전용 설정)
    fig1.update_traces(
        texttemplate='%{text:,.0f}', 
        textposition='outside',  # 막대 바깥쪽 상단에 수치 표시
        marker_color='#1f77b4'   # 막대 색상 지정 (선택사항)
    )    
    fig1.update_layout(
        xaxis=dict(tickmode='linear'), # 모든 년도가 나오도록 설정
        yaxis=dict(
            showgrid=True, 
            gridcolor='LightGray',            
            range=[0, dfp['세대수'].max() * 1.2], # 수치 라벨 공간 확보
            dtick=100000,            
            tickformat=',d'
        ),
        bargap=0.5 # 막대 사이의 간격 조절 (0.1 ~ 0.5 사이 추천)
    )    
    st.plotly_chart(fig1, use_container_width=True)
    #st.line_chart(dfp)
    #st.dataframe(dfp.style.format('{:,.0f}'), use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1: region = st.selectbox('시도 선택', sorted(ddf['시도'].unique()), index=1)       
    with col2: dday = st.selectbox('기준월 선택', sorted(ddf[ddf['기준년']>2025]['기준월'].unique()))     
    if st.button('조회'):
        scol1, scol2, scol3 = st.columns([3.5,0.5,6])
        cond = (ddf['기준월'] >= dday) & (ddf['시도']== region)
        dff = ddf[cond]
        dff = dff[['구분','단지명','소재지','세대수','기준월','기준년']]
        with scol1:
            st.write(f'{region} 연도별 입주예정')
            dfp = dff.pivot_table(index='기준년', values='세대수', aggfunc='sum')
            st.line_chart(dfp, height=500)            
    
        with scol3:                    
            st.write(f'{region} 입주예정 자료')
            st.dataframe(dff, use_container_width=True, hide_index=True, height=500)


elif menu == "인구":
        st.subheader('🏠 주민등록인구')
        url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=1726816395#gid=1726816395"    
        ddf = conn.read(spreadsheet=url)
        ddf = ddf.drop("행정기관코드", axis=1)
        ncols = ['총인구수', '세대수', '남자 인구수', '여자 인구수']                    
        for col in ncols:
            if col in ddf.columns:
                ddf[col] = pd.to_numeric(ddf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)            
                # 비율 등이 아닌 일반 수치라면 정수(int)로 강제 변환
                ddf[col] = ddf[col].astype(int)
                
        col1, col2 = st.columns(2)
        with col1: region = st.text_input('지역입력')
        with col2: dday = st.selectbox('기준월 선택', sorted(ddf['기준월'].unique(), reverse=True))               
        if st.button('조회'):
            cond = (ddf['행정기관'].str.contains(region)) & (ddf['기준월'] == dday)
            dff = ddf[cond]
            format_dict = {}
            for col in dff.columns:
                if dff[col].dtype == 'int64':
                    format_dict[col] = '{:,.0f}'  # 정수는 천단위 콤마만
                elif dff[col].dtype == 'float64':
                    format_dict[col] = '{:,.1f}'  # 실수는 천단위 콤마 + 소수점 1자리
            
            st.dataframe(dff.style.format(format_dict), use_container_width=True, hide_index=True, height=500)
        
        
elif menu == "미분양":
    st.subheader('🏠 전국 미분양 추이')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=667956721#gid=667956721"    
    data = conn.read(spreadsheet=url)
    data = data.drop(["항목","단위"], axis=1)
    
    data_sido = data[data['시군구']=='계']    
    # axis=0은 세로 방향(컬럼별) 합계를 의미합니다.
    monthly_total = data_sido.drop(columns=['구분','시군구']).sum(axis=0)
    # 결과 확인을 위해 데이터프레임으로 변환
    df_total = monthly_total.reset_index()
    df_total.columns = ['월', '합계']    
    # 차트를 위해 '월'을 인덱스로 설정
    chart_data = df_total.set_index('월')
    # 선 차트로 표현할 경우
    st.line_chart(chart_data)    
    st.divider()       
    
    st.subheader('🏠 시군구별 미분양')
    col1, col2 = st.columns(2)
    with col1: region = st.selectbox('시도 선택', sorted(data['구분'].unique()), index=1)       
    with col2: dday = st.selectbox('기준월 선택', sorted(data.columns[3:], reverse=True))           
    if st.button('조회'):
        scol1, scol2 = st.columns(2)
        with scol1:
            row_cond = (data['시군구'] == '계')
            sel_cols = ['구분', dday]            
            dff = data.loc[row_cond, sel_cols].copy()            
            if not dff.empty:
                st.write(f"📊시도별 미분양 현황 [{dday} 기준]")                        
                # '구분' 컬럼을 인덱스로 설정해야 막대 아래에 이름이 나옵니다.
                chart_data = dff.set_index('구분')                                
                st.bar_chart(chart_data, height=500)                                
                # st.dataframe(dff, use_container_width=True, hide_index=True)
            else:
                st.warning("차트를 표시할 데이터가 없습니다.")    
                
        with scol2:
            # 1. 행 조건 설정 (구분에 region이 포함된 행)            
            row_cond = data['구분'].str.contains(region, na=False)    
            # 2. 행 조건 필터링 + dday 열(컬럼) 선택
            # 필수로 보여야 할 정보(예: '구분')와 선택한 'dday' 열만 추출
            sel_cols = ['구분','시군구', dday] # 보여주고 싶은 컬럼 리스트
            dff = data.loc[row_cond, sel_cols]                     
            if not dff.empty:
                st.write(f"📊 {region} 지역 미분양 현황 [{dday}기준]")                                
                dff[dday] = pd.to_numeric(dff[dday], errors='coerce').fillna(0)                        
                dff = dff.sort_values(by=dday, ascending=False)                
                # [핵심] subset을 사용하여 dday 컬럼에만 포맷 적용
                styled_dff = dff.style.format("{:,.0f}", subset=[dday])                
                st.dataframe(styled_dff, use_container_width=True, hide_index=True, height=500)
            else:
                st.warning("조회된 결과가 없습니다.")

        
elif menu == "청약홈조회":
    st.subheader('🏠 청약홈 APT 분양 정보 조회')            
    
    col1, col2 = st.columns(2)    
    with col1:
        short_sido = ["서울", "경기", "인천", "부산", "대전", "대구", "광주", "울산", "세종", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]
        area = st.selectbox("공급지역 선택", short_sido)
        
    with col2:
        #pd.Timestamp.now() + MonthEnd(-6)
        sdate = datetime.datetime.now() + MonthEnd(-6)
        edate = datetime.datetime.now() + MonthEnd(0)
        dates = pd.date_range(sdate, edate, freq="ME")
        date_list = [i.isoformat()[0:10] for i in dates]
        selected_date = st.selectbox("모집공고월 기준(이후)", date_list)

    if st.button("🔍 검색 실행"):
        df_list = get_applyhome_list(area, selected_date)
        if not df_list.empty:
            st.session_state['apply_df'] = df_list
        else:
            st.warning("조회된 결과가 없습니다.")

    # 결과 표시 구역
    if 'apply_df' in st.session_state:
        st.subheader("📋 분양 정보 리스트")
        st.info("행을 클릭하여 상세 정보를 확인하세요.")        
        # 데이터프레임 선택 기능 (st.dataframe의 on_select 활용)
        event = st.dataframe(
            st.session_state['apply_df'], 
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row")
        # 상세 정보 표시 (선택 시)
        if event and len(event.selection.rows) > 0:
            selected_row_idx = event.selection.rows[0]
            manage_no = st.session_state['apply_df'].iloc[selected_row_idx]["주택관리번호"]
            house_name = st.session_state['apply_df'].iloc[selected_row_idx]["주택명"]
            
            st.divider()
            st.subheader(f"🔍 {house_name} 상세 타입 정보")
            
            with st.spinner("상세 정보를 불러오는 중..."):
                df_detail = get_applyhome_detail(manage_no)
                if not df_detail.empty:
                    # 금액 콤마 포맷팅 적용하여 출력
                    st.table(df_detail.style.format({"타입최고가": "{:,}", "공급면적": "{:.2f}"}, na_rep="-"))
                else:
                    st.error("상세 정보를 찾을 수 없습니다.")



# --- 하단 안내 ---
if menu == "옵션선택":
    st.info("왼쪽 사이드바에서 메뉴를 선택해 주세요.")


