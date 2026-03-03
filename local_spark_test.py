from pyspark.sql import SparkSession
from warcio.archiveiterator import ArchiveIterator
import io

# 1. 로컬 Spark 세션 생성 (내 컴퓨터 자원 사용)
spark = SparkSession.builder \
    .appName("OracleNewsLocalTest") \
    .master("local[*]") \
    .getOrCreate()

def process_warc_local(file_path):
    print(f"--- {file_path} 분석 시작 ---")
    with open(file_path, 'rb') as f:
        count = 0
        for record in ArchiveIterator(f):
            if record.rec_type == 'response':
                content = record.content_stream().read().decode('utf-8', 'ignore')
                if "ORCL" in content or ("Oracle" in content and ("stock" in content.lower() or "shares" in content.lower() or "NYSE" in content)):
                    # 여기에 '1969' 같은 옛날 기사를 제외하는 조건도 넣을 수 있습니다.
                    if "2026" in content: 
                        print(f"Found Real News: {record.rec_headers.get_header('WARC-Target-URI')[:80]}...")
                        count += 1
            if count >= 5: break
    print(f"분석 완료! 총 {count}개 발견")

# 로컬에 다운로드한 샘플 파일로 테스트
process_warc_local("sample.warc.gz")

spark.stop()