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

DEST_BUCKET = "oracle-project-wonji"
TARGET_MONTH = "2026/01"
# ---------------------------------------

def process_common_crawl():
    input_bucket = "commoncrawl"
    prefix = f"crawl-data/CC-NEWS/{TARGET_MONTH}/"
    
    # 1월 파일 목록 중 상위 20개만 우선 처리 (테스트 겸 비용 절약)
    objects = s3.list_objects_v2(Bucket=input_bucket, Prefix=prefix, MaxKeys=20)
    
    results = []
    exclude_list = ["hindustantimes", "livemint", "economictimes", "tickerreport", "astrology"]

    for obj in objects.get('Contents', []):
        file_key = obj['Key']
        print(f"Processing: {file_key}")
        
        resp = s3.get_object(Bucket=input_bucket, Key=file_key)
        
        # 스트리밍 읽기
        with io.BytesIO(resp['Body'].read()) as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    if record.http_headers.get_statuscode() != "200": continue
                    
                    url = record.rec_headers.get_header('WARC-Target-URI').lower()
                    if any(ex in url for ex in exclude_list): continue

                    html_content = record.content_stream().read().decode('utf-8', 'ignore')
                    
                    # 가벼운 1차 필터
                    if "oracle" not in html_content.lower(): continue

                    soup = BeautifulSoup(html_content, 'html.parser')
                    raw_title = soup.title.string if (soup.title and soup.title.string) else ""
                    title = str(raw_title)
                    
                    if any(err in title.lower() for err in ["404", "not found", "error"]): continue

                    clean_text = soup.get_text(separator=' ', strip=True)
                    
                    # 2차 정밀 필터
                    if ("Oracle" in title or clean_text.count("Oracle") >= 3) and len(clean_text) > 1000:
                        results.append({
                            "url": url,
                            "title": title.strip(),
                            "content": clean_text[:2000] # 분석에 필요한 앞부분만 저장
                        })
    
    # 결과 저장
    if results:
        df = spark.createDataFrame(results)
        # S3에 Parquet 형식으로 저장 (분석하기 가장 좋은 포맷)
        output_path = f"s3://{DEST_BUCKET}/refined-news/{TARGET_MONTH}/"
        df.write.mode("overwrite").parquet(output_path)
        print(f"Successfully saved {len(results)} articles to {output_path}")

# 실행
process_warcio_news()