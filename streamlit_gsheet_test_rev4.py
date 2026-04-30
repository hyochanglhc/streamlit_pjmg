# -*- coding: utf-8 -*-
import streamlit as st
from streamlit_option_menu import option_menu 
from streamlit_gsheets import GSheetsConnection 
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np

import datetime
import requests as rq
from pandas.tseries.offsets import MonthEnd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os, io
import plotly.express as px
import pymysql

cur_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(cur_dir)  # 현재 디렉토리로 이동
load_dotenv()

# --- 설정 및 스타일 ---
st.set_page_config(page_title="MANAGE", layout="wide")
# ==================== 로그인 로직 추가 ====================

def get_engine():
    # 로컬 .env 또는 서버 환경 변수에서 가져옴
    db_user = os.getenv("DB_USER")
    db_pw = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    
    # SQLAlchemy 엔진 생성
    db_url = f"mysql+pymysql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url)


def get_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        # 포트 번호는 정수(int)형이어야 하므로 형변환이 필요합니다.
        port=int(os.getenv("DB_PORT", 3309)), 
        charset='utf8',
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )
# 2. 로그인 처리 로직
def login_handler(id_input, pass_input):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        # ID와 PW를 동시에 체크
        sql = "SELECT user FROM rp_user WHERE user = %s AND password = %s;"
        cur.execute(sql, (id_input, pass_input))
        row = cur.fetchone()
        return True if row else False
    except pymysql.Error as e:
        st.error(f"DB 오류: {e}")
        return False
    finally:
        if conn: conn.close()

def signup_handler(new_id, new_pass):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        # 아이디 중복 체크
        cur.execute("SELECT * FROM rp_user WHERE user = %s;", (new_id,))
        if cur.fetchone():
            return False, "이미 존재하는 아이디입니다."
        
        # 정보 저장
        cur.execute("INSERT INTO rp_user (user, password) VALUES (%s, %s);", (new_id, new_pass))
        return True, "회원가입이 완료되었습니다!"
    except pymysql.Error as e:
        return False, f"DB 오류: {e}"
    finally:
        if conn: conn.close()

def delete_user_handler(user_id):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM rp_user WHERE user = %s;", (user_id,))
        return True
    except pymysql.Error as e:
        st.error(f"탈퇴 처리 중 오류 발생: {e}")
        return False
    finally:
        if conn: conn.close()

def get_total_user_count():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        # rp_user 테이블의 전체 행 개수 조회
        cur.execute("SELECT COUNT(*) as count FROM rp_user;")
        row = cur.fetchone()
        return row['count'] if row else 0
    except pymysql.Error as e:
        st.error(f"회원 수 조회 중 오류: {e}")
        return 0
    finally:
        if conn: conn.close()


def check_login():
    """DB 연동 사용자 인증 상태 확인 및 로그인 화면 출력"""
    # 1. 세션 상태 초기화
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "user_id" not in st.session_state:
        st.session_state.user_id = None

    # 2. 로그인 되어있지 않은 경우 양식 출력
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
            st.write("## 🔒 로그인")
            input_id = st.text_input("아이디[사번]", placeholder="아이디 입력")
            input_pw = st.text_input("비밀번호[사번]", type="password", placeholder="비밀번호 입력")
            
            if st.button("로그인", use_container_width=True):
                # ---------------------------------------------------------
                # [핵심 수정 부분] 하드코딩 대신 DB 핸들러 호출
                # ---------------------------------------------------------
                if login_handler(input_id, input_pw):
                    st.session_state.logged_in = True
                    st.session_state.user_id = input_id  # 로그인한 ID 저장
                    st.success(f"{input_id}님, 환영합니다! 대시보드로 이동합니다.")
                    st.rerun()
                else:
                    st.error("아이디 또는 비밀번호가 틀렸습니다.")
                # ---------------------------------------------------------
        
        return False # 로그인 실패 상태
    
    return True # 로그인 완료 상태

# 3. 로그인 체크 실행 (로그인 적용여부)
if not check_login():
    st.stop()


# ==========================================================
st.markdown("""
    <style>    
    .stDataFrame div[data-testid="stTableHD"] {font-size: 16px !important;}    
    .stDataFrame div[data-testid="stTableCD"] {font-size: 16px !important;}
    .stTable td, .stTable th {font-size: 16px !important;}
    [data-testid="stMetricLabel"] {font-size: 18px !important;}
    [data-testid="stMetricValue"] {font-size: 20px !important;}
    </style>
    """, unsafe_allow_html=True) 
    

def get_gspread_client():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    # 기존에 사용하시던 JSON 키 경로를 그대로 입력하세요.
    SERVICE_ACCOUNT_FILE = r'K:/pyenv/py311/py_gsheet/python-gsheet-484713-be4d9602c973.json'

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    return gspread.authorize(creds)


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
    #load_dotenv()
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



def style_fill_col(col):    
    style = ['' for _ in col]    
    if col.name in ['계약(%)','완납(%)','소송(%)']:
        #style = ['background-color: yellow' for _ in col]
        style = ['background-color: yellow' for _ in col] # 컬럼명이 '계약(%)'인 경우에만 노란색 적용        
    return style

def style_fill_row(row):
    name = row.name #해당 row(행)의 인덱스(Index) 이름    
    #isinstance(name, tuple): 만약 인덱스가 멀티 인덱스라면 ('대분류', '항목명')처럼 튜플(Tuple) 형태가 됩니다.
    #이 경우 name[1]을 선택해 실제 항목명인 '항목명'만 가져옵니다.
    item_name = name[1] if isinstance(name, tuple) else name        
    if item_name in ['영업이익','원가율','경상이익','소계','과부족']:
        return ['background-color: black'] * len(row)    
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


# --- 공통 연결 객체 및 함수 ---
gsconn = st.connection("gsheets", type=GSheetsConnection)  #gsconn : google sheet connection

@st.cache_data
def alv_data(target_month=None):
    # 1. 데이터 로드
    url = "https://docs.google.com/spreadsheets/d/1uoL2CDVEi_KPV74eT5VEOjVB7ucCrEYnl9TkNQvstsM/edit?gid=1989275734#gid=1989275734"
    rdf = gsconn.read(spreadsheet=url)
    
    if target_month is None:
        valid_months = rdf[~rdf['기준월'].isin(['nan', '', 'None'])]['기준월']
        if valid_months.empty: return pd.DataFrame()
        filter_month = valid_months.max()
    else:
        filter_month = target_month    
    
    df_month = rdf[rdf['기준월'] == filter_month].copy()    
    
    str_txt = '프로젝트,프로젝트 내역,당월매출,금년매출,누계매출,당월매원,금년매원,누계매원,당사업경비,금사업경비,누사업경비,당용지비,금누계비,누용지비,당월판관비(수주후),금년판관비(수주후),누계판관비(수주후),당월판관비(수주전),금년판관비(수주전),누계판관비(수주전),당월금융비,금년금융비,누계금융비,당현장원가,금현장원가,누현장원가,당공손충,연공손충,누공손충,당월(실)하자보수비,금년(실)하자보수비,누계(실)하자보수비,당기타영업수익,금기타영업수익,누기타영업수익,당기타영업비용,금기타영업비용,누기타영업비용,당이자수익,금이자수익,누이자수익,당이자비용,금이자비용,누이자비용,기준월'
    col_list = list(str_txt.split(","))
    
    df = df_month[col_list]
    
    base_cols = ['프로젝트', '프로젝트 내역', '기준월']
    num_cols = [c for c in df.columns if c not in base_cols]
    
    #먼저 데이터를 문자열로 바꾸고 콤마(,)를 제거하고, 숫자변환하고, 오류는 Nan처리후 결측치를 0으로 채우고 정수형으로 변환    
    for col in num_cols:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)

    # 데이터 구조 변경 (가로 -> 세로)
    # 각 항목(당월매출, 누계매출 등)을 '항목'이라는 이름의 행으로 보냅니다.
    dff = df.melt(id_vars=['프로젝트', '프로젝트 내역', '기준월'], var_name='항목', value_name='값')

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

    

# --- 사이드바(로그인후) ---
with st.sidebar:
    with st.sidebar:            
# =============================================================================
#         total_users = get_total_user_count()
#         st.markdown(f"""
#     <div style="margin-bottom: 10px;">
#         <p style="font-size: 16px">전체: {total_users}명</p>            
#     </div>""", unsafe_allow_html=True)
#         #st.metric(label="전체 회원 수", value=f"{total_users}명")
#         
#         st.info(f"👤 {st.session_state.user_id}님 접속 중")
#         if st.button("로그아웃"):
#             st.session_state.update({"logged_in": False, "result_df": None, "user_id": None})
#             st.rerun()
# =============================================================================
        
        #st.divider()
# =============================================================================
#         with st.expander("회원탈퇴"):
#             st.warning("탈퇴 시 데이터가 삭제됩니다.")
#             confirm_delete = st.checkbox("정말 탈퇴하시겠습니까?")
#             if st.button("회원탈퇴 실행"):
#                 if confirm_delete and delete_user_handler(st.session_state.user_id):
#                     st.session_state.update({"logged_in": False, "result_df": None, "user_id": None})
#                     st.rerun()
# =============================================================================
        items = ["옵션선택","사업개요","실적조회","분양","동호약정납부","PF현황","중도금대출","자금수지","채권","소송","PJ도급","pjcode","실거래조회", "입주예정","인구","미분양",] #청약홈조회
        menu = option_menu("Manage", items,
                       #icons=["dash","info-circle", "bank", "bank", "bank", "bank","bank","house","house","house","house"],
                       icons=["dash"] + ["info-circle"]*len(items),
                       menu_icon="cast", default_index=0)

# --- 메뉴별 로직 ---

if menu == '실적조회':
    st.write("### SAP 실적조회")    
    if 'sel_pj' not in st.session_state:
        st.session_state.sel_pj = None
    
    url = "https://docs.google.com/spreadsheets/d/1uoL2CDVEi_KPV74eT5VEOjVB7ucCrEYnl9TkNQvstsM/edit?gid=1989275734#gid=1989275734"
    rdf = gsconn.read(spreadsheet=url)
    rdf['기준월'] = rdf['기준월'].astype(str).str.strip()
    month_list = sorted(rdf.loc[~rdf['기준월'].isin(['nan', '', 'None']), '기준월'].unique(), reverse=True)
        
    col1, col2, col3 = st.columns([3, 3, 3])
    
    with col2:        
        dday = st.selectbox('기준월 선택', month_list)        
        
    # [핵심] 선택된 dday를 인자로 전달하여 해당 월의 가공 데이터(df) 생성    
    df = alv_data(target_month=dday)    
    if df.empty:
        st.warning(f"⚠️ {dday} 기준 조회된 데이터가 없습니다.")
        st.stop()

    with col1:        
        pjs = sorted(df['프로젝트 내역'].unique())
        # [핵심] index 설정: 세션에 저장된 값이 옵션 목록에 있으면 해당 위치를 기본값으로 설정
        default_index = 0
        if st.session_state.sel_pj in pjs:
            default_index = pjs.index(st.session_state.sel_pj)
        
        # 프로젝트 선택 시 세션 상태 업데이트
        pj = st.selectbox('(본공사)사업명 선택', pjs, index=default_index, key='pj_selector')
        st.session_state.sel_pj = pj # 선택한 값을 세션에 저장        
        
        # 옵션공사 맵핑용 시트 조회
        url_pair = 'https://docs.google.com/spreadsheets/d/1uoL2CDVEi_KPV74eT5VEOjVB7ucCrEYnl9TkNQvstsM/edit?gid=1549480112#gid=1549480112'
        df_pair = gsconn.read(spreadsheet=url_pair)
        pj2 = next(iter(df_pair.loc[df_pair['pj'] == pj, 'pjo']), None)        
    
    with col3:     
        st.markdown('<p style="margin-bottom: 28px;"></p>', unsafe_allow_html=True)
        sch_button = st.button('조회', use_container_width=True)        
        
    
    if sch_button:
        df_all = [] 
        col_order = ['매출', '토지비', '공사비', '경비', '공손충', '하자보수비', '매원', '원가율', '매출이익',
                  '판관비', '영업이익', '기타영업수익', '기타영업비용', '금융비', '이자수익', '이자비용', '경상이익']
        period_order = ['당월', '금년', '누계']
    
        # 스타일 및 원가율 % 포맷팅 함수
        def apply_custom_styling(df):
            styler = df.style.apply(style_fill_row, axis=1).set_properties(**{
                'padding-top': '0px', 'padding-bottom': '0px', 'line-height': '1', 'font-size': '10px'
            })
            styler = styler.format("{:,.0f}")
            for idx in df.index:
                if idx[1] == '원가율':
                    styler = styler.format(subset=(idx, slice(None)), formatter="{:.1f}%")
            return styler
    
        # 공통 지표 계산 및 데이터프레임 생성 함수
        def common_process(df, pj_name):
            idx_cols = ['프로젝트', '프로젝트 내역', '기준월', '기간기준']
            
            def get_series(tdf, category):
                return tdf[tdf['항목기준'] == category].groupby(idx_cols)['값'].sum()
    
            def create_row(series, name):
                res = series.to_frame(name='값').reset_index()
                res['항목기준'] = name
                return res
    
            # 1. 항목별 Series 추출
            s_sales = get_series(df, '매출')
            s_cost = get_series(df, '매원')
            s_sg_a = get_series(df, '판관비')
            s_oth_i = get_series(df, '기타영업수익')
            s_oth_e = get_series(df, '기타영업비용')
            s_int_i = get_series(df, '이자수익')
            s_int_e = get_series(df, '이자비용')
            s_fin = get_series(df, '금융비')
    
            # 2. 산식 계산
            gp_val = s_sales.sub(s_cost, fill_value=0) # 매출이익
            op_val = gp_val.sub(s_sg_a, fill_value=0)  # 영업이익
            ord_val = (op_val.add(s_oth_i, fill_value=0)
                             .sub(s_oth_e, fill_value=0)
                             .add(s_int_i, fill_value=0)
                             .sub(s_int_e, fill_value=0)
                             .sub(s_fin, fill_value=0)) # 경상이익
            
            # 3. 데이터프레임 행 생성
            gp_df = create_row(gp_val, '매출이익')
            op_df = create_row(op_val, '영업이익')
            ord_df = create_row(ord_val, '경상이익')
            
            # 4. 결합 및 카테고리 정렬
            # (원가율은 피벗 후 정확한 재계산을 위해 여기서 합치지 않고 피벗 테이블에서 직접 처리 권장)
            dfc = pd.concat([df, gp_df, op_df, ord_df], ignore_index=True)
            dfc['항목기준'] = pd.Categorical(dfc['항목기준'], categories=col_order, ordered=True)
            dfc['기간기준'] = pd.Categorical(dfc['기간기준'], categories=period_order, ordered=True)
            
            # 5. 피벗 테이블 생성
            dfp = dfc.pivot_table(index=['프로젝트', '항목기준'], columns='기간기준', values='값', aggfunc='sum', observed=False)
            
            # 6. [중요] 원가율 재계산 (단순 합산 방지)
            #행 인덱스에서 첫 번째 레벨(프로젝트)은 전체(slice(None))를 선택하고, 두 번째 레벨(항목기준)이 '원가율'인 행들만 가져온 뒤, 모든 컬럼(:)을 표시해라.
            try:
                dfp.loc[(slice(None), '원가율'), :] = (
                    dfp.loc[(slice(None), '매원'), :].values / 
                    dfp.loc[(slice(None), '매출'), :].replace(0, np.nan).values
                ) * 100
            except: pass
                
            return dfp
    
        # --- A. 본공사 데이터 처리 ---
        with col1:
            st.markdown("본공사")
            dff_main = df[df['프로젝트 내역'] == pj].copy()
            if not dff_main.empty:
                p_main = common_process(dff_main, pj)
                st.dataframe(apply_custom_styling(p_main), use_container_width=True, height=630)
                df_all.append(dff_main)
    
        # --- B. 옵션공사 데이터 처리 ---
        with col2:
            if pj2:
                st.markdown("옵션공사")
                dff_opt = df[df['프로젝트 내역'] == pj2].copy()
                if not dff_opt.empty:
                    p_opt = common_process(dff_opt, pj2)
                    st.dataframe(apply_custom_styling(p_opt), use_container_width=True, height=630)
                    df_all.append(dff_opt)
    
        # --- C. 합계 데이터 처리 ---
        with col3:
            if len(df_all) > 1:
                st.markdown("합계")
                df_total = pd.concat(df_all, ignore_index=True)
                df_total['프로젝트'] = '합계'
                p_total = common_process(df_total, '합계')
                st.dataframe(apply_custom_styling(p_total), use_container_width=True, height=630)


elif menu == "pjcode":
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

elif menu == 'PJ도급':
    st.subheader('📊 PJ도급 수입전망')
    st.write('단위:백만원')
    url ='https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=764755693#gid=764755693'    
    rdf = gsconn.read(spreadsheet=url).fillna("")
    num_cols = rdf.columns[4:] 

    format_dict = {}
    
    for col in num_cols:
        if col == '면세율':
            # 면세율: 숫자라면 소수점 1자리 퍼센트, 0이면 빈칸, 문자는 그대로
            format_dict[col] = lambda x: f"{x:.1%}" if (isinstance(x, (int, float)) and x != 0) else ("" if x == 0 else x)
        else:
            # 나머지 숫자 컬럼: 숫자라면 천 단위 콤마, 0이면 빈칸, 문자는 그대로 (에러 방지)
            format_dict[col] = lambda x: f"{x:,.0f}" if (isinstance(x, (int, float)) and x != 0) else ("" if x == 0 else x)
    
    # 2. 스타일 적용 및 출력
    styled_rdf = rdf.style.format(format_dict)
    
    st.dataframe(styled_rdf, use_container_width=True, hide_index=True)


elif menu == "사업개요":
    st.subheader('📊 사업개요')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=0#gid=0"        
    data = gsconn.read(spreadsheet=url, usecols=list(range(15))).fillna("")        
    pj_list = ["전체 조회"] + data['사업명'].drop_duplicates().tolist()        
    sel_pj = st.selectbox('조회할 사업명을 선택하세요', pj_list)        
    
    if st.button('조회'):
        data2 = gsconn.read(spreadsheet=url, usecols=[1] + list(range(17, 41))).fillna("")        
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
    rdf = gsconn.read(spreadsheet=url, usecols=[0,1,2,3,4,5,6,7,11,12,14])
    ncols = ['약정','기표','상환','잔액']  #숫자칼럼 명시
    for col in ncols:
        if col in rdf.columns:
            rdf[col] = pd.to_numeric(rdf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    
    
    col1, col2 = st.columns(2)
    pj_list = rdf['PJ명'].drop_duplicates().tolist()        
    with col1: pj = st.selectbox('조회할 사업명을 선택하세요', pj_list)            
    with col2: dday = st.selectbox('기준월 선택', sorted(rdf['기준월'].unique(), reverse=True))       
        
    if st.button('조회'):
        cond = (rdf['기준월'] == dday)
        if pj:
            cond &= rdf['PJ명'].str.contains(pj, na=False, case=False)
            dff = rdf[cond].copy()
            if not dff.empty:
                dff['약정'] = dff['약정'] / 100000000
                dff['기표'] = dff['기표'] / 100000000
                dff['상환'] = dff['상환'] / 100000000
                dff['잔액'] = dff['잔액'] / 100000000
                dff_sty = dff.style.format(thousands=",", precision=0)
# =============================================================================
#                 ncols = dff.select_dtypes(include=['number']).columns
#                 config = {col: st.column_config.NumberColumn(format="%d") for col in ncols}
#                 st.dataframe(dff, use_container_width=True, hide_index=True, column_config=config)
# =============================================================================
                st.dataframe(dff_sty, use_container_width=True, hide_index=True)
                target_col = '잔액'
                if target_col and target_col in dff.columns:
                    total_val = dff[target_col].sum()            
                    st.metric(label=f"💰 {target_col} 합계", value=f"{total_val:,.0f} 억원")
        else:
            st.warning("조회된 결과가 없습니다.")            


elif menu == "동호약정납부":
    st.subheader('📊 동호약정 납입현황')
    
    sht_id = {
        '벤처밸리': 'https://docs.google.com/spreadsheets/d/1N1qhgvhoVBWtuF6LfBPjaRGX6kawiUWpc0bJb8vBDgM/edit?gid=0#gid=0',
        '시민공원': 'https://docs.google.com/spreadsheets/d/1N1qhgvhoVBWtuF6LfBPjaRGX6kawiUWpc0bJb8vBDgM/edit?gid=767298303#gid=767298303',
        '시화디오션': 'https://docs.google.com/spreadsheets/d/1N1qhgvhoVBWtuF6LfBPjaRGX6kawiUWpc0bJb8vBDgM/edit?gid=1762659893#gid=1762659893',
        }
    opt = sht_id.keys()
    sel_pj = st.selectbox('사업명 선택', opt)

    if st.button('조회'):
        url = sht_id[sel_pj]      
        rdf = gsconn.read(spreadsheet=url)    
        df = rdf[rdf['동']!='합계']

        # [수정 포인트 1] 그룹화하기 전, 가장 먼저 숫자형으로 변환합니다.
        # 금액 컬럼들에 콤마(,)가 있을 수 있으므로 제거 후 변환합니다.
        numeric_cols = ["약정금액", "납부원금"] 
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
        # fillna(0)는 결측치가 있을 경우 에러를 방지하기 위함입니다.
# =============================================================================
#          1. 동호수 생성 시 정수형 변환 후 문자열 결합 (소수점 제거 핵심)
#         df["동호수"] = (
#             df["동"].fillna(0).astype(int).astype(str) + 
#             "-" + 
#             df["호수"].fillna(0).astype(int).astype(str).str.zfill(4))
# =============================================================================
        def format_dong(x):
            if pd.isna(x): return "0"
            try:
                # 숫자로 변환 가능하면 소수점 제거 후 문자열화 (101.0 -> 101)
                return str(int(float(x)))
            except (ValueError, TypeError):
                # 'A'와 같은 문자면 그대로 반환
                return str(x)
        
        # 2. '호수' 컬럼 처리: 4자리 zfill 적용
        def format_hosu(x):
            if pd.isna(x): return "0000"
            try:
                return str(int(float(x))).zfill(4)
            except (ValueError, TypeError):
                return str(x).zfill(4)
        
        # 데이터프레임 적용
        df["동호수"] = df["동"].apply(format_dong) + "-" + df["호수"].apply(format_hosu)                 
        
        df["상품"] = df["세대속성"].str.split("/").str[0]    
        df = df[df["주택형"] != "소계"].copy()    
        df["약정일자"] = pd.to_datetime(df["약정일자"])    
        df["약정월"] = df["약정일자"] + pd.offsets.MonthEnd(0) 

        # [포인트 2] 이제 그룹화를 하면 숫자로 합산됩니다.
        dfg = df.groupby(["상품", "약정월", "차수", "동호수"], as_index=False).agg({
            "약정금액": "sum",
            "납부원금": "sum"})
        dfg = dfg[dfg["약정월"].notnull()]
        dfg = dfg.sort_values("약정월")    
        dfg["차수구분"] = dfg["차수"].str[:2]  #계약, 1차, 2차, 잔금
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
        
        def highlight_row(row):
            # '구분' 컬럼이 '납부원금'이면 lightyellow, 아니면 빈 문자열(기본값)
            color = 'background-color: lightyellow; color: black; font-weight: bold' if row['구분'] == '납부원금' else ''
            return [color] * len(row)
        
        

        # 3. 스타일 적용 및 출력
        # 기존 style_by_date(열 기준 스타일)와 highlight_row(행 기준 스타일)를 체이닝하여 적용
        styled_dfp = (dfp.style
                      .apply(style_by_date, axis=0)        # 날짜 기준 열 스타일 (기존)
                      .apply(highlight_row, axis=1)        # '납부원금' 기준 행 스타일 (추가)
                      .format(thousands=",", precision=0))
        
        st.dataframe(styled_dfp, use_container_width=True, hide_index=True)        
        
                
        c1, c2 = st.columns([3,7])
        with c1:
            st.subheader('상품별 중도금납부현황')
            df_mid = dfg[dfg['차수'].str.contains('차', na=False)].copy() #중도금만 : 1차, 2차
            df_mid = df_mid[['동호수','상품','차수','차수구분','약정금액','납부원금']]        
            #df_mid.rename(columns={'납부원금': '납부중도금'}, inplace=True)
            dfp2 = df_mid.pivot_table(index=['상품'], values=['약정금액','납부원금'], aggfunc='sum',fill_value=0, margins=True, margins_name='합계').reset_index()
            dfp2_disp = dfp2.copy()
            dfp2_disp['약정금액'] = dfp2_disp['약정금액'] / 1000000            
            dfp2_disp['납부원금'] = dfp2_disp['납부원금'] / 1000000            

            st.dataframe(dfp2_disp.style.format(precision=0, thousands=","), use_container_width=True, hide_index=True)                    
        
        with c2:
            st.subheader('동호별 납부현황')                                     
            
            # 1. 단위 변환 (중복 계산 방지 위해 별도 변수 권장하나 기존 흐름 유지)
            df_c2 = dfg.copy()
            df_c2['납부원금'] = df_c2['납부원금'] / 1000000 
            
            # 2. 피벗 테이블 생성
            dfp3 = df_c2.pivot_table(
                index=['동호수', '상품'], 
                columns='차수구분', 
                values='납부원금', 
                aggfunc='sum',
                fill_value=0, 
                margins=True, 
                margins_name='합계'
            ).reset_index()            
            
            # 3. 컬럼 순서 재배치 (계약 컬럼 위치 조정)
            cols = dfp3.columns.tolist()
            if "계약" in cols:
                cols.insert(2, cols.pop(cols.index("계약")))            
            dfp3 = dfp3[cols]          
        
            # 4. 스타일 적용 (0은 빈칸으로, 숫자는 콤마 적용)
            # subset을 사용하여 '동호수', '상품' 컬럼은 포맷팅 대상에서 제외합니다.
            numeric_cols = [c for c in dfp3.columns if c not in ['동호수', '상품']]
            
            styled_dfp3 = dfp3.style.format(
                lambda x: f"{x:,.0f}" if x != 0 else "", 
                subset=numeric_cols
            )
            
            st.dataframe(styled_dfp3, use_container_width=True, hide_index=True)
            #st.dataframe(dfp3.style.format(precision=0, thousands=","), use_container_width=True, height=500)                       
                
        
elif menu == "자금수지":
    st.subheader('📊 자금수지 조회')
    url = "https://docs.google.com/spreadsheets/d/18AhC-xVCGMpapdZwpptxnkED3_sO18B7qDeKz-4oa60/edit?gid=0#gid=0"
    rdf = gsconn.read(spreadsheet=url)    
    rdf = rdf[rdf['수지구분']=='영업수지']  #영업수지만..
        
    ncols = ['금액']
    for col in ncols:
        if col in rdf.columns:
            rdf[col] = pd.to_numeric(rdf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

    # 1. 입력 UI (사업명 및 기준월 선택)
    col1, col2, col3 = st.columns(3)
    pj_list = rdf['사업명'].drop_duplicates().tolist()
    unique_months = sorted(rdf['기준월'].unique(), reverse=True)
    
    with col1: 
        pj = st.selectbox('조회할 사업명을 선택하세요', pj_list)
    with col2:
        dday = st.selectbox('기준월 선택', unique_months)
    with col3:
        # 화면에 표시를 시작할 시점 (출력월) 설정
        # 기본값을 '2025-09' 등으로 설정하거나 리스트에서 선택하게 할 수 있습니다.
        print_month = st.date_input('출력 시작월 선택', value=pd.to_datetime('2025-12-31'))
    
    # 지난달(last_month) 자동 계산 로직
    if len(unique_months) >= 2:
        idx = unique_months.index(dday)
        last_month = unique_months[idx + 1] if idx + 1 < len(unique_months) else unique_months[-1]
    else:
        last_month = unique_months[0] if unique_months else None
    
    ## --- 2. 가공 및 스타일링 함수 (print_month 변수 사용) ---
    def make_styled_df(input_df, title_text, start_date):
        if input_df.empty:
            st.warning(f"[{title_text}] 조회된 결과가 없습니다.")
            return
    
        # (1) 데이터 타입 변환 및 피벗
        temp_df = input_df.copy()
        temp_df['집행월'] = pd.to_datetime(temp_df['집행월'])
        dfp = temp_df.pivot_table(index='구분', columns='집행월', values='금액', aggfunc='sum', fill_value=0)
        
        # 가로 총합계 계산
        dfp['총합계'] = dfp.sum(axis=1)
    
        # (2) 누계 및 과부족 계산
        original_indices = dfp.index.tolist()
        calc_rows = []
        
        income_rows = [i for i in original_indices if '수입' in i]
        expense_rows = [i for i in original_indices if '지출' in i]
        
        income_total = dfp.loc[income_rows].sum() if income_rows else pd.Series(0, index=dfp.columns)
        expense_total = dfp.loc[expense_rows].sum() if expense_rows else pd.Series(0, index=dfp.columns)
    
        # [수정] 수입소계 제외, 수입누계만 추가
        if not income_total.sum() == 0:
            s2 = income_total.cumsum().to_frame().T
            s2.index = ['수입누계']
            calc_rows.append(s2)
    
        # [수정] 지출소계 제외, 지출누계만 추가
        if not expense_total.sum() == 0:
            e2 = expense_total.cumsum().to_frame().T
            e2.index = ['지출누계']
            calc_rows.append(e2)
    
        # 과부족 및 누계 과부족
        shortage = (income_total - expense_total).to_frame().T
        shortage.index = ['과부족']
        cum_shortage = (income_total - expense_total).cumsum().to_frame().T
        cum_shortage.index = ['누계과부족']
        calc_rows.extend([shortage, cum_shortage])
    
        # (3) 결합 및 정렬
        dfp = pd.concat([dfp] + calc_rows)
        
        # [수정] 정렬 순서 정의에서 소계 명칭 제거
        all_indices = dfp.index.tolist()
        final_order = (
            [i for i in all_indices if '수입' in i and i != '수입누계'] + ['수입누계'] +
            [i for i in all_indices if '지출' in i and i != '지출누계'] + ['지출누계'] +
            ['과부족', '누계과부족']
        )
        seen = set()
        final_order = [x for x in final_order if x in dfp.index and not (x in seen or seen.add(x))]
        dfp = dfp.reindex(final_order)
    
        # (4) 출력 필터링 및 컬럼명 변환
        sel_cols = [c for c in dfp.columns if (isinstance(c, pd.Timestamp) and c >= pd.to_datetime(start_date))]
        dis_cols = sel_cols + ['총합계']
        dfp_to_show = dfp[dis_cols].copy()
        
        new_columns = []
        for c in dfp_to_show.columns:
            if isinstance(c, pd.Timestamp):
                new_columns.append(c.strftime('%Y-%m'))
            else:
                new_columns.append(str(c))
        dfp_to_show.columns = new_columns
    
        # (5) 스타일링 함수
        def apply_styles(styler):
# =============================================================================
#             def style_rows(row):                
#                 target_names = ['수입', '지출', '과부족']  #음영적용                
#                 is_highlight = row.name in target_names
#                 return ['background-color: lightyellow; color: black; font-weight: bold' if is_highlight else '' for _ in row]
# =============================================================================            
            def style_rows(row):
            # 1. 색상 및 스타일 정의
                income_style = 'background-color: lightblue; color: black; font-weight: bold'
                expense_style = 'background-color: lightyellow; color: black; font-weight: bold'
                shortage_style = 'font-weight: bold' # 과부족용 (선택사항)
        
                # 2. 행 이름(row.name)에 따른 조건부 스타일 선택
                if '수입' == str(row.name):
                    style = income_style
                elif '지출' == str(row.name):
                    style = expense_style
                elif '과부족' == str(row.name):
                    style = shortage_style
                else:
                    style = ''
                # 3. 해당 행의 모든 셀에 스타일 적용
                return [style for _ in row]                    
            
            return styler.apply(style_rows, axis=1)
            
    
        # (6) 최종 렌더링
        st.write(f"### {title_text}")
        if not dfp_to_show.empty:    
            styled_df = apply_styles(dfp_to_show.style).format(lambda x: f"{x:,.0f}" if x != 0 else "")    
            st.dataframe(styled_df, use_container_width=True)
        else:
            st.info("선택한 출력월 이후의 데이터가 없습니다.")

    # 3. 조회 실행
    if st.button('조회'):
        # 공통 사업명 필터
        pj_cond = rdf['사업명'].str.contains(pj, na=False, case=False) if pj else True
        
        # (A) 이번 달 출력
        cond_now = (rdf['기준월'] == dday) & pj_cond
        # 마지막 인자로 print_month를 반드시 넣어주어야 합니다.
        make_styled_df(rdf[cond_now], f"📊 당월전망: {dday}", print_month) 
        
        # (B) 지난 달 출력
        if last_month:
            cond_last = (rdf['기준월'] == last_month) & pj_cond
            # 여기도 마찬가지로 print_month를 추가합니다.
            make_styled_df(rdf[cond_last], f"📊 전월전망 ({last_month})", print_month)
            
            
        
    
        
elif menu == "채권":
    st.subheader('📊 채권현황 조회')
    url = "https://docs.google.com/spreadsheets/d/1RlNYrWWezvHQfceEgmHIkC-c7dnIxRIWZTM3fWdqDWQ/edit?gid=0#gid=0"           
    rdf = gsconn.read(spreadsheet=url)
    ncols = ['채권', '불량', '잔액', '총분양금', '대출잔액']
    for col in ncols:
        if col in rdf.columns:
            rdf[col] = pd.to_numeric(rdf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    
    
    col1, col2 = st.columns(2)
    pj_list = rdf['그룹코드'].drop_duplicates().tolist()        
    with col1: pj = st.selectbox('조회할 그룹코드를 선택하세요', pj_list)            
    with col2: dday = st.selectbox('기준월 선택 ', sorted(rdf['기준월'].unique(), reverse=True))

    if st.button('조회'):
        cond = (rdf['기준월'] == dday)
        if pj: cond &= rdf['그룹코드'].str.contains(pj, na=False, case=False)
        dff = rdf[cond]        
        if not dff.empty:
            # 1. 스타일 및 포맷 설정
            styled_df = dff.style.format({
                "채권": lambda x: f"{x / 100_000_000:,.1f}억",
                "불량": lambda x: f"{x / 100_000_000:,.1f}억"  # 백만 단위로 나누고 'M' 접미사 추가
            })
        
            # 2. 메인 데이터프레임 출력
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            st.divider()
            
            # 3. 그룹화 데이터 계산 및 스타일 적용
            dfg = dff.groupby(['계정대분류', '계정소분류'], as_index=False)['채권'].sum()
            
            # 그룹화 데이터에도 동일하게 백만 단위 포맷 적용
            styled_dfg = dfg.style.format({
                "채권": lambda x: f"{x / 100_000_000:,.1f}억"
            })
            
            c1, c2 = st.columns([2, 1])
            with c1: 
                st.dataframe(styled_dfg, use_container_width=True, hide_index=True)
            
            with c2: 
                total_m = dff['채권'].sum() / 100_000_000
                st.metric(label="💰 총 채권 합계", value=f"{total_m:,.1f} 억")
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
    rdf = gsconn.read(spreadsheet=urls[mid_tab])      
    ncols =['대출잔액']
    for col in ncols:
        if col in rdf.columns:
            rdf[col] = pd.to_numeric(rdf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    if not rdf.empty:
        # 숫자 컬럼에 대해 콤마 포맷 적용 (%d는 정수형)
        col_config = {col: st.column_config.NumberColumn(format="%d") for col in rdf.select_dtypes(include=['number']).columns}
        
        st.dataframe(rdf, use_container_width=True, hide_index=True, column_config=col_config)
        
        # 4. 하단 합계 표시 (Metric)
        if '대출잔액' in rdf.columns:
            total_loan = rdf['대출잔액'].sum()
            st.metric(label=f"💰 {mid_tab} 대출잔액 합계", value=f"{total_loan:,.0f} 원")
    else:
        st.warning("조회된 데이터가 없습니다.")
    

elif menu == "중도금대출":
    st.subheader('🏠 중도금대출 결산자료')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=67742981"
    rdf = gsconn.read(spreadsheet=url)    
    ncols =['잔액']
    for col in ncols:
        if col in rdf.columns:
            rdf[col] = pd.to_numeric(rdf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

    col1, col2 = st.columns(2)
    pj_list = rdf['PJ명'].drop_duplicates().tolist()            
    pj_list = rdf['PJ명'].drop_duplicates().tolist()                
    with col1: pj = st.selectbox('조회할 사업명을 선택하세요', pj_list)            
    with col2: dday = st.selectbox('기준월 선택 ', sorted(rdf['기준월'].unique(), reverse=True))
    if st.button('조회'):
        cond = (rdf['PJ명'].str.contains(pj, na=False, case=False) if pj else [True] * len(rdf)) & (rdf['기준월']==dday)
        dff = rdf[cond]        
        dff = rdf[cond][['PJ명','상품유형','대출기관','잔액','대출만기일']]
        col1, col2 = st.columns([5,5])
        with col1:
            if not dff.empty:                                                  
                dfp = dff.pivot_table(index=['PJ명','상품유형', '대출기관'], columns='대출만기일', values='잔액', 
                                      aggfunc='sum', fill_value=0, margins=True, margins_name='합계')
                st.dataframe(dfp.style.format("{:,.0f}"))
            else:
                st.warning("조회된 데이터가 없습니다.")      

elif menu == "분양":
    st.subheader('📊 분양현황')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=391839077#gid=391839077"
    rdf = gsconn.read(spreadsheet=url)   
           
    ncols = ['입주증번호','총분양금']
    for col in ncols:
        if col in rdf.columns:
            rdf[col] = pd.to_numeric(rdf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    

    pj_list = rdf['사업명'].drop_duplicates().tolist()    
    pj = st.selectbox('사업명 선택', pj_list)    
    search_btn = st.button('조회')
    
    if search_btn:        
        if pj:            
            cond = rdf['사업명'].str.contains(pj, na=False, case=False)                    
            df = rdf[cond]    
            ibju = df['입주증번호'].sum()
            lawsuit = (df['소송']=='소송').sum() #소송 개수
            if not df.empty:                
                dff_total = df.groupby('상품')['동호수'].count().reset_index(name='총공급')
                # 1. 피벗 테이블 생성 (계약여부별 동호수 개수)
                dfp = df.pivot_table(
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
            
                cols = [c for c in dfp.columns if c != '공급'] # 합계 제외한 원래 칼럼들
                for col in cols:
                    dfp[f'{col}(%)'] = (dfp[col] / dfp['공급'] * 100).round(0).fillna(0)
                # 3. 데이터프레임 정리 (인덱스 초기화)                
                dfp = dfp[['공급','계약','미계약','계약(%)','미계약(%)']]
                dfp = dfp.reset_index()
                
                
                dfp2 = df.pivot_table(
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

                for col in cols:
                    dfp2[f'{col}(%)'] = (dfp2[col] / dfp2['공급'] * 100).round(0).fillna(0)
                # 3. 데이터프레임 정리 (인덱스 초기화)                
                dfp2 = dfp2[['공급','계약','미계약','계약(%)','미계약(%)']]
                dfp2 = dfp2.reset_index()               
        
                #c1, c2 = st.columns([3, 1]) # %가 추가되었으므로 비율을 조금 조정
                c1, c2 = st.columns(2) # %가 추가되었으므로 비율을 조금 조정
                with c1:
                    st.write('동호기준')                                    
                    st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 세대(실), %)</div>', unsafe_allow_html=True)
                    sdf = dfp.style.apply(style_fill_col, axis=0).format({'계약(%)': '{:.0f}%', '미계약(%)': '{:.0f}%'})
                    st.dataframe(sdf, use_container_width=True, hide_index=True)                                        
                    
                    
                with c2:
                    st.write('금액기준')                    
                    st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)                    
                    sdf2 = dfp2.style.apply(style_fill_col, axis=0).format({'공급': '{:,.0f}', '계약': '{:,.0f}', '미계약': '{:,.0f}',
                                                  '계약(%)': '{:.0f}%', '미계약(%)': '{:.0f}%',
                                                  })
                    st.dataframe(sdf2, use_container_width=True, hide_index=True)                    
                    #st.dataframe(dfp2.style.format(thousands=",", precision=0), use_container_width=True, hide_index=True)                
                
                
                dfg = df.groupby(['상품', '계약월'])['동호수'].count().reset_index(name='계약건수')
                dfg['날짜'] = pd.to_datetime(dfg['계약월'], errors='coerce')
                dfg = dfg.dropna(subset=['날짜']).sort_values(['상품', '날짜'])
                dfg['누적계약'] = dfg.groupby('상품')['계약건수'].cumsum()
                
                # 3. 완납 데이터 정리 (ibju > 0 조건은 dff 필터링 단계에서 이미 ibju 합계로 확인 가능)
                dff_paid = df[df['완납여부'] == '완납'].copy()
                if not dff_paid.empty:
                    dfg_paid = dff_paid.groupby(['상품', '완납월'])['동호수'].count().reset_index(name='완납건수')
                    dfg_paid['날짜'] = pd.to_datetime(dfg_paid['완납월'], errors='coerce')
                    dfg_paid = dfg_paid.dropna(subset=['날짜']).sort_values(['상품', '날짜'])
                    dfg_paid['누적완납'] = dfg_paid.groupby('상품')['완납건수'].cumsum()
                else:
                    dfg_paid = pd.DataFrame(columns=['상품', '날짜', '누적완납'])
            
                # 4. 데이터 통합 및 정렬 (날짜순 정렬 필수)
                combined = pd.merge(
                    dfg[['상품', '날짜', '누적계약']], 
                    dfg_paid[['상품', '날짜', '누적완납']], 
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
                    # 🌟 [해결 1] Plotly 오류 방지를 위해 데이터프레임을 복사하여 일반 Pandas DF로 확정
                    chart_data = combined.copy()
                    
                    # 🌟 [해결 2] '날짜표시' 컬럼이 문자열인지 확인 (카테고리형 오류 방지)
                    chart_data['날짜표시'] = chart_data['날짜표시'].astype(str)
                
                    fig1 = px.line(
                        chart_data,               # 수정된 데이터 사용
                        x='날짜표시', 
                        y='계약률', 
                        color='상품', 
                        markers=True, 
                        template="plotly_white"
                    )                   
                    
                    # X축 정렬 설정
                    fig1.update_xaxes(
                        type='category', 
                        categoryorder='array', 
                        categoryarray=sorted_date_strings,
                        title="계약월",
                        tickangle=45  # <--- 이 부분을 추가하세요. -90은 세로 방향입니다.
                    )
                    
                    # Y축 포맷 설정
                    fig1.update_yaxes(
                        tickformat=".0%", 
                        range=[0, 1.1],
                        minor=dict(showgrid=True, nticks=10), 
                        gridcolor='lightgray'
                    )
                    
                    # 레이아웃 업데이트
                    fig1.update_layout(
                        margin=dict(l=20, r=20, t=20, b=20), # 여백 조정 (선택사항)
                        hovermode="x unified"                # 툴팁 가독성 향상
                    )
                    
                    st.plotly_chart(fig1, use_container_width=True)
                                
                #입주현황표시
                if ibju > 0:
                    st.divider()               
                    st.subheader('📊 입주현황')                                        
                    dfp3 = df.pivot_table(
                        index='상품',
                        columns='완납여부', 
                        values='동호수', 
                        aggfunc='count',
                        fill_value=0)        
                
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
                    
                                        
                    dfp4 = df.pivot_table(
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
                        styled_df = styled_df.format({'완납(%)': '{:.0f}%', '미납(%)': '{:.0f}%'})
                        st.dataframe(styled_df, use_container_width=True, hide_index=True)                    
                        #st.dataframe(dfp3, use_container_width=True, hide_index=True) #hide_index를 하면 인덱스 숨김                    
                    with c4:                        
                        st.write('금액기준')                
                        st.markdown('<div style="text-align: right; font-size: 12px;">(단위 : 백만원, %)</div>', unsafe_allow_html=True)
                        styled_df = dfp4.style.apply(style_fill_col, axis=0)
                        styled_df = styled_df.format({'공급': '{:,.0f}', '완납': '{:,.0f}', '미납': '{:,.0f}',
                                                      '완납(%)': '{:.0f}%', '미납(%)': '{:.0f}%',
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
                    dfp5 = df.pivot_table(
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
                    
                    dfp6 = df.pivot_table(
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
                    dfg = df.groupby(['상품', '소송','완납여부', '계약여부2']).agg({
                        '동호수': 'count',
                        '총분양금': 'sum'
                        }).reset_index()
                    
                    dfg['총분양금'] = (dfg['총분양금'] / 1_000_000).round(0) 
                    # 해당 컬럼을 Categorical 타입으로 변환 (ordered=True가 핵심)
                    dfg['상품'] = pd.Categorical(dfg['상품'], categories=custom_order, ordered=True)                    
                    # '상품'은 오름차순(True), '완납여부'와 '소송'은 내림차순(False)
                    dfg = dfg.sort_values(
                        by=['상품', '완납여부', '소송'], 
                        ascending=[True, False, False])
                    st.dataframe(dfg.style.format(thousands=",", precision=0), use_container_width=True, hide_index=True)
                    
                    
            else:
                st.warning("조회된 결과가 없습니다.")            


elif menu == "입주예정":
    st.subheader('🏠 아파트 입주예정(부동산지인)')
    url = "https://docs.google.com/spreadsheets/d/1j4lp5-8MJWr0ZgFevDs3Bv3O_rv9dJvQQUt7ew6Yt3A/edit?gid=1029648553#gid=1029648553"
    rdf = gsconn.read(spreadsheet=url)        
    num_col = ['세대수','기준년']
    for col in num_col:
        if col in rdf.columns:
            rdf[col] = pd.to_numeric(rdf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    
    #df = df[df['기준년'] > 2025]
    rdf.loc[rdf['시도'] == '강원특별자치도', '시도'] = '강원도'        
    dff = rdf[rdf['구분']=='아파트']    
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
    with col1: region = st.selectbox('시도 선택', sorted(rdf['시도'].unique()), index=1)       
    with col2: dday = st.selectbox('기준월 선택', sorted(rdf[rdf['기준년']>2025]['기준월'].unique()))     
    if st.button('조회'):
        scol1, scol2, scol3 = st.columns([3.5,0.5,6])
        cond = (rdf['기준월'] >= dday) & (rdf['시도']== region)
        dff = rdf[cond]
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
        rdf = gsconn.read(spreadsheet=url)
        rdf = rdf.drop(rdf.columns[0], axis=1) #행정기관코드 삭제
        ncols = ['총인구수', '세대수', '남자 인구수', '여자 인구수']                    
        for col in ncols:
            if col in rdf.columns:
                rdf[col] = pd.to_numeric(rdf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)            
                # 비율 등이 아닌 일반 수치라면 정수(int)로 강제 변환
                rdf[col] = rdf[col].astype(int)                
                
        region = st.text_input('지역입력')
        dday = st.selectbox('기준월 선택', sorted(rdf['기준월'].unique(), reverse=True), key='select_1')            
        
        if st.button('조회'):
            cond = (rdf['행정기관'].str.contains(region)) & (rdf['기준월'] == dday)
            dff = rdf[cond]
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
    data = gsconn.read(spreadsheet=url)
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
    #with col1: region = st.selectbox('시도 선택', sorted(data['구분'].unique()), index=1)       
    with col1: region = st.selectbox('시도 선택', ['전체'] + sorted(data['구분'].unique()))
    with col2: dday = st.selectbox('기준월 선택', sorted(data.columns[3:], reverse=True))           
    if st.button('조회'):
        scol1, scol2 = st.columns(2)
        with scol1:
            row_cond = (data['시군구'] == '계')
            sel_cols = ['구분', dday]            
            dff = data.loc[row_cond, sel_cols].copy()            
            if not dff.empty:
                st.write(f"📊시도별 미분양 현황 [{dday} 기준]")                        
                dff[dday] = pd.to_numeric(dff[dday].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                # '구분' 컬럼을 인덱스로 설정해야 막대 아래에 이름이 나옵니다.
                chart_data = dff.set_index('구분')                                
                st.bar_chart(chart_data, height=500)                                
                # st.dataframe(dff, use_container_width=True, hide_index=True)
            else:
                st.warning("차트를 표시할 데이터가 없습니다.")    
                
        with scol2:
            # 1. 행 조건 설정 (구분에 region이 포함된 행)            
            sel_cols = ['구분','시군구', dday] # 보여주고 싶은 컬럼 리스트
            if region == '전체':
                dff = data.loc[:, sel_cols]                     
            else:
                row_cond = data['구분'].str.contains(region, na=False)    
                dff = data.loc[row_cond, sel_cols]                     
            # 2. 행 조건 필터링 + dday 열(컬럼) 선택
            # 필수로 보여야 할 정보(예: '구분')와 선택한 'dday' 열만 추출
            
            
            if not dff.empty:
                st.write(f"📊 {region} 지역 미분양 현황 [{dday}기준]")
                
                # 2. 데이터를 숫자로 강제 변환 (콤마 제거 및 에러 처리)
                # 이 과정이 있어야 숫자 크기대로 정확히 정렬됩니다.
                dff[dday] = pd.to_numeric(dff[dday].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                
                # 3. 내림차순 정렬
                dff = dff.sort_values(by=dday, ascending=False)
                
                # 4. 스타일 적용 및 출력
                styled_dff = dff.style.format("{:,.0f}", subset=[dday])
                st.dataframe(styled_dff, use_container_width=True, hide_index=True, height=500)
            else:
                st.warning("조회된 결과가 없습니다.")


elif menu == "소송":
    st.subheader('📊 소송현황 조회')
    url = "https://docs.google.com/spreadsheets/d/1diNe5cD5pFtz9ca7c4z5leUsbUTPgAPMM7fFmjFOczQ/edit?gid=1053819147#gid=1053819147"
    rdf = gsconn.read(spreadsheet=url)
# =============================================================================
#     ncols = ['약정','기표','상환','잔액']  #숫자칼럼 명시
#     for col in ncols:
#         if col in rdf.columns:
#             rdf[col] = pd.to_numeric(rdf[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)    
# =============================================================================
    
    col1, col2 = st.columns(2)
    pj_list = rdf['사업명'].drop_duplicates().tolist()        
    with col1: pj = st.selectbox('조회할 사업명을 선택하세요', pj_list)                
        
    if st.button('조회'):        
        if pj:
            cond = (rdf['사업명'].str.contains(pj, na=False, case=False)) & (rdf['판결여부'])
            dff = rdf[cond].copy()
            if not dff.empty:
                dfp = dff.pivot_table(index=['판결여부','소송규모','사건명','접수일','원고','기일차수','최종일자'], values='원고수', aggfunc='sum').reset_index()
                dfp = dfp.sort_values(['소송규모','접수일'], ascending=[False, True]) 
# =============================================================================
#                 ncols = dff.select_dtypes(include=['number']).columns
#                 config = {col: st.column_config.NumberColumn(format="%d") for col in ncols}
# =============================================================================
                st.dataframe(dfp, use_container_width=True, hide_index=True)
                st.metric(label="소송건수", value=f"{len(dfp)}건")
                
                
        else:
            st.warning("조회된 결과가 없습니다.")            


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



