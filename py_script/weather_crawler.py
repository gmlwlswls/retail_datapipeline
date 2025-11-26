import requests
import pandas as pd 
from datetime import date, timedelta
from sqlalchemy import create_engine, text
import time

USER = "chjin"
PW = "gmlwlswls"
HOST = 'localhost'
PORT = 3306
DB = "RETAIL_DB"

REGION_COORDS = {
    "서울": (37.5665, 126.9780),   # 서울시청
    "경기": (37.2636, 127.0286),   # 수원시청
    "인천": (37.4563, 126.7052),   # 인천시청
    "강원": (37.8813, 127.7298),   # 춘천시청
    "충청": (36.3504, 127.3845),   # 대전시청 (충청권 대표)
    "경상": (35.8722, 128.6014),   # 대구시청 (경상권 대표)
    "전라": (35.1595, 126.8526),   # 광주시청 (전라권 대표)
    "제주": (33.4996, 126.5312),   # 제주시청
}

OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"

def get_engine() :
  url = f"mysql+pymysql://{USER}:{PW}@{HOST}:{PORT}/{DB}"
  return create_engine(url, future= True)

def get_lat_date(engine, region: str) :
  """WEATHER 테이블에서 해당 REGION의 가장 최근 날짜 조회"""
  with engine.connect() as conn :
    result = conn.execute(
      text("SELECT MAX(DATE) FROM WEATHER WHERE REGION = :REGION"),
      {"REGION" : region}
    )
    row = result.fetchone()
    return row[0] if row and row[0] is not None else None 

def fetch_weather(region: str, lat:float, lon: float, start_date : date, end_date : date) -> pd.DataFrame :
  """특정 권역에 대해 start_date~end_date 구간 날씨 데이터 가져오기"""
  params = {
    "latitude" : lat,
    "longitude" : lon,
    "start_date" : start_date.isoformat(),
    "end_date" : end_date.isoformat(),
    "daily" : "temperature_2m_mean,precipitation_sum",
    "timezone" : "Asia/Seoul",
  }
  resp = requests.get(OPENMETEO_URL, params= params, timeout= 90)
  resp.raise_for_status()
  data= resp.json()
  
  if "daily" not in data :
    raise ValueError(f"[{region}] Unexpected response: {data}")
  
  daily = data["daily"]
  df = pd.DataFrame({
    "DATE" : daily['time'],
    'TEMP' : daily.get('temperature_2m_mean', [None] * len(daily['time'])),
    'PRECIPITATION' : daily.get('precipitation_sum', [None] * len(daily['time'])),
  })
  df['DATE'] = pd.to_datetime(df['DATE']).dt.date
  df['REGION'] = region
  return df[['REGION', 'DATE', 'TEMP', 'PRECIPITATION']]

def upsert_weather(engine, df: pd.DataFrame) :
  """weather(region, date) 기준 upsert"""
  if df.empty :
    print("새로 가져올 날씨 데이터가 없습니다.")
    return 
  
  with engine.begin() as conn :
    for _, row in df.iterrows() :
      conn.execute(
        text("""
             INSERT INTO WEATHER(REGION, DATE, TEMP, PRECIPITATION)
             VALUES (:REGION, :DATE, :TEMP, :PRECIPITATION)
             """),
        {
          "REGION" : row['REGION'],
          "DATE" : row['DATE'],
          'TEMP' : float(row['TEMP']) if pd.notna(row['TEMP']) else None,
          'PRECIPITATION' : float(row['PRECIPITATION']) if pd.notna(row['PRECIPITATION']) else None,
        }
      )

def fetch_weather_retry(region, lat, lon, start_date, end_date, max_retries= 3, retry_sleep = 5) :
  '''타임아웃/일시 오류를 고려한 재시도'''
  for attempt in range(1, max_retries + 1 ) :
    try :
      df = fetch_weather(region, lat, lon, start_date, end_date)
      return df 
    except requests.exceptions.Timeout :
      print(f" [WARN] {region} {start_date}~{end_date} timeout (retry {attempt} / {max_retries})")
    except requests.exceptions.RequestException as e :
      print(f" [WARN] {region} {start_date}~{end_date} error : {e} (retry {attempt} / {max_retries})")
      
    if attempt < max_retries :
      time.sleep(retry_sleep * attempt) # 점점 길게 쉬도록
  
  # 재시도 다 실패
  raise RuntimeError(F"[ERROR] {region} {start_date}~{end_date} 구간 {max_retries}번 재시도 후 실패")

def main() :
  engine = get_engine()
  today = date.today()
  api_end = today - timedelta(days= 1)
  
  for region, (lat, lon) in REGION_COORDS.items() :
    print(f"\n=== {region} ({lat}, {lon}) ===")
    
    last_date = get_lat_date(engine, region)
    
    if last_date is None :
      start_date = api_end - timedelta(days= 5 * 365) # 5년전 데이터
      print(f"[INIT] {region} : {start_date} ~ {api_end} 수집")
    else :
      start_date = last_date + timedelta(days= 1)
      print(f"[INCREMENTAL] {region}: {start_date} ~ {api_end} 추가 수집")
      
    if start_date > api_end :
      print(f"{region} : 이미 최신 상태")
      continue 
    
    chunk_start = start_date
    while chunk_start <= api_end :
      chunk_end = min(chunk_start + timedelta(days= 30), api_end) # 한 달 후 일자랑 어제 중 작은 일자가 마지막 수집일
      print(f" - {region}: {chunk_start} ~ {chunk_end}")
      
      # 이 청크에서 실패하면 runtimeerror가 터지고 그 시점 이후는 아예 안 감
      df_chunk = fetch_weather_retry(region, lat, lon, chunk_start, chunk_end)
      upsert_weather(engine, df_chunk) # 이 청크를 DB에 바로 반영
      print(f"{region}: {len(df_chunk)}일치 upsert 완료")

      chunk_start = chunk_end + timedelta(days= 1)
    
    # ii) 모아서 upsert
    # upsert_weather(engine, df_all) # chuck모아서 전체 기간에 대해 upsert
    # print(f"{region}: {len(df_all)}일치 upsert 완료")
  
if __name__ == "__main__" :
  main()
