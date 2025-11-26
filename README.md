# DataPipeline
물류센터 매입 데이터와 날씨 데이터를 자동으로 수집/전처리/예측하는 End-to-End 데이터 파이프라인입니다.

### 파이프라인 구조
1) 수집
- 날씨 데이터
: weather_crawler.py로 최근 5년 크롤링 + 매일 스케쥴링
: MariaDB WEATHER 테이블에 저장
- 매입 데이터(엑셀)
: /home/chjin/retail_datapipe/purchases_xlsx폴더 NiFi로 감시
: NiFi GetFile > Excel > JSON 변환
: MariaDB PURCHASES_RAW 테이블에 저장

2) 전처리
- python 스크립트 preprocess_purchases.py
- MariaDB PURCHASES_FEATURE 저장

3) 전환
- python Consumer(purchases_expectation.py)
- Kafka 메시지 > ML모델 예측
- 예측 결과를 MongoDB PURCHASES_FORECAST 컬렉션에 저장

5) 서빙
- FastAPI 웹 서버에서 center, year_month 입력 > MongoDB에서 해당 월의 매입 예측치 조회

### 사용 기술
- NiFi : 파일 자동 수집, 변환, DB 적재, Kafka 발행
- Kafka : Feature 전달 및 모델과 파이프라인 분리 
- MariaDB : Raw / Feature 저장
- MongoDB : 예측 결과 저장 및 조회
- Python : 크롤링, 전처리, 모델 예측
- FastAPI : 예측 결과 API 제공

### 폴더 구조
retail_datapipeline                                           
|___ purchases_xlsx(매입 데이터)          
|___ py_script           
.gitignore          
README.md              

### 핵심 흐름
날씨/엑셀 > NiFi > MariaDB(raw) > Python전처리 > MariaDB(feature) > NiFi > Kafka > Python 모델(Expectation Node) > MongoDB > FastAPI

### 데이터베이스
MariaDB     
[데이터베이스] RETAIL_DB
[테이블]
WEATHER : (REGION,DATE) : PRIMARY KEY, DATE : DATE, REGION: VARCHAR(20), TEMP : FLOAT, PRECIPITATION : FLOAT
PURCHASES_RAW
PURCHASES_FEATURE

