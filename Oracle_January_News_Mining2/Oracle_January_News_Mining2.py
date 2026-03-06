import sys
import boto3
import io
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup

# 1. 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client('s3')

# --- 원지 님의 버킷 정보 ---
MY_BUCKET = "oracle-sentiment-wonji-project" 
DEST_PATH = f"s3://{MY_BUCKET}/silver/2026/01/"
# --------------------------

def run_silver_mining():
    input_bucket = "commoncrawl"
    prefix = "crawl-data/CC-NEWS/2026/01/"
    
    # 1월 파일 목록 중 상위 10개 처리 (테스트 및 비용 최적화)
    list_objects = s3.list_objects_v2(Bucket=input_bucket, Prefix=prefix, MaxKeys=10)
    
    silver_results = []
    
    # [기술적 필터 1] 제외할 국가 도메인 및 언어 경로 (중국어 추가)
    exclude_domains = [
        ".de/", ".ro/", ".fr/", ".jp/", ".cn/", ".hk/", ".tw/",  # 도메인 확장자 차단
        "/de-de/", "/ro-ro/", "/fr-fr/", "/zh-cn/", "/zh-tw/", "/zh-hk/" # 언어 경로 차단
    ]

    for obj in list_objects.get('Contents', []):
        key = obj['Key']
        print(f"Reading from Common Crawl: {key}")
        
        try:
            resp = s3.get_object(Bucket=input_bucket, Key=key)
            raw_data = resp['Body'].read()
            
            with io.BytesIO(raw_data) as stream:
                for record in ArchiveIterator(stream):
                    if record.rec_type == 'response':
                        # [기술적 필터 2] HTTP 상태 코드가 200(정상)인 것만
                        if record.http_headers.get_statuscode() != "200":
                            continue
                        
                        url = record.rec_headers.get_header('WARC-Target-URI').lower()
                        
                        # [기술적 필터 3] 비영어권 도메인 차단
                        if any(dom in url for dom in exclude_domains):
                            continue
                        
                        # [기술적 필터 4] HTML 내용 읽기 및 'oracle' 키워드 1차 확인
                        html = record.content_stream().read().decode('utf-8', 'ignore')
                        if "oracle" not in html.lower():
                            continue
                        
                        # [기술적 필터 5] BeautifulSoup을 통한 순수 텍스트 추출
                        soup = BeautifulSoup(html, 'html.parser')
                        title = soup.title.string if (soup.title and soup.title.string) else "No Title"
                        clean_text = soup.get_text(separator=' ', strip=True)
                        
                        # 데이터가 유의미한 길이인 경우만 Silver로 채택
                        if len(clean_text) > 800:
                            silver_results.append({
                                "url": url,
                                "title": str(title).strip(),
                                "content": clean_text[:3000],  # Silver 단계는 분석을 위해 넉넉히 저장
                                "extracted_at": "2026-03-06"
                            })
                            
        except Exception as e:
            print(f"Error processing {key}: {e}")

    # 2. Silver 계층 저장 (JSON 형식)
    if silver_results:
        df = spark.createDataFrame(silver_results)
        # overwrite 모드로 저장하여 이전 노이즈 데이터를 깔끔하게 대체합니다.
        df.write.mode("overwrite").json(DEST_PATH)
        print(f"✅ Silver Mining Done! Saved {len(silver_results)} candidate articles to {DEST_PATH}")
    else:
        print("❌ No matching articles found in these WARC files.")

# 실행
run_silver_mining()