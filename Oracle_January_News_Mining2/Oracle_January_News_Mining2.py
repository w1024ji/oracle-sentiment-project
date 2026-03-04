import sys
import boto3
import io
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup

# 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client('s3')

MY_BUCKET = "oracle-project-wonji" 
# --------------------------

def run_mining():
    input_bucket = "commoncrawl"
    prefix = "crawl-data/CC-NEWS/2026/01/"
    
    # 안전하게 5개 파일만 먼저 테스트
    list_objects = s3.list_objects_v2(Bucket=input_bucket, Prefix=prefix, MaxKeys=5)
    
    final_results = []
    exclude_list = ["hindustantimes", "livemint", "economictimes", "tickerreport", "astrology"]

    for obj in list_objects.get('Contents', []):
        key = obj['Key']
        print(f"Working on: {key}")
        
        try:
            resp = s3.get_object(Bucket=input_bucket, Key=key)
            # 데이터를 한 번에 메모리에 올리지 않고 스트림으로 처리
            raw_data = resp['Body'].read()
            with io.BytesIO(raw_data) as stream:
                for record in ArchiveIterator(stream):
                    if record.rec_type == 'response' and record.http_headers.get_statuscode() == "200":
                        url = record.rec_headers.get_header('WARC-Target-URI').lower()
                        if any(ex in url for ex in exclude_list): continue
                        
                        html = record.content_stream().read().decode('utf-8', 'ignore')
                        if "oracle" not in html.lower(): continue
                        
                        soup = BeautifulSoup(html, 'html.parser')
                        title = soup.title.string if soup.title else "No Title"
                        text = soup.get_text(separator=' ', strip=True)
                        
                        if ("Oracle" in str(title) or text.count("Oracle") >= 3) and len(text) > 1000:
                            final_results.append({
                                "url": url,
                                "title": str(title),
                                "content": text[:1000]
                            })
        except Exception as e:
            print(f"Error processing {key}: {e}")

    if final_results:
        # 결과를 Spark 데이터프레임으로 변환
        df = spark.createDataFrame(final_results)
        # S3에 저장
        output_path = f"s3://{MY_BUCKET}/project/oracle_news_jan/"
        df.write.mode("overwrite").json(output_path)
        print(f"Done! Saved {len(final_results)} articles.")

run_mining()