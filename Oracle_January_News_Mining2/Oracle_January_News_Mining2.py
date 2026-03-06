import sys
import boto3
import io
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
from datetime import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client('s3')

MY_BUCKET = "oracle-sentiment-wonji-project" 
DEST_PATH = f"s3://{MY_BUCKET}/silver/2026/01/"

current_date = datetime.utcnow().strftime('%Y-%m-%d')

def run_silver_mining():
    input_bucket = "commoncrawl"
    prefix = "crawl-data/CC-NEWS/2026/01/"
    
    # 1월 파일 목록 중 상위 10개 처리 
    list_objects = s3.list_objects_v2(Bucket=input_bucket, Prefix=prefix, MaxKeys=10)
    
    silver_results = []
    
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
                        if record.http_headers.get_statuscode() != "200":
                            continue
                        
                        url = record.rec_headers.get_header('WARC-Target-URI').lower()
                        
                        if any(dom in url for dom in exclude_domains):
                            continue
                        
                        html = record.content_stream().read().decode('utf-8', 'ignore')
                        if "oracle" not in html.lower():
                            continue
                        
                        soup = BeautifulSoup(html, 'html.parser')
                        raw_title = soup.title.string if (soup.title and soup.title.string) else "No Title"
                        title = str(raw_title).strip()
                        clean_text = soup.get_text(separator=' ', strip=True)
                        
                        # footer 및 광고 마커 기준 절단
                        footer_markers = ["brand studio", "cxo leaders dialogue", "subscribe now", "advertisement", "follow us"]
                        
                        processed_text = clean_text.lower()
                        for marker in footer_markers:
                            if marker in processed_text:
                                # 해당 마커가 나타나는 첫 지점까지만 본문으로 취급
                                clean_text = clean_text[:processed_text.find(marker)]
                                break 
                        
                        if len(clean_text) > 800:
                            silver_results.append({
                                "url": url,
                                "title": str(title).strip(),
                                "content": clean_text[:3000],  
                                "extracted_at": current_date
                            })
                            
        except Exception as e:
            print(f"Error processing {key}: {e}")

    if silver_results:
        df = spark.createDataFrame(silver_results)
        df.write.mode("overwrite").json(DEST_PATH)
        print(f"Silver Mining Done! Saved {len(silver_results)} candidate articles to {DEST_PATH}")
    else:
        print("No matching articles found in these WARC files.")

run_silver_mining()