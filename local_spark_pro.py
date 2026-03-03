from pyspark.sql import SparkSession
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import io

spark = SparkSession.builder.appName("OracleNewsPro").master("local[*]").getOrCreate()

def process_warc_pro(file_path):
    print(f"--- {file_path} 초정밀 분석 시작 ---")
    
    # 제외 리스트 강화 (노이즈 사이트 추가)
    exclude_list = ["hindustantimes", "livemint", "economictimes", "tickerreport", "astrology"]
    
    with open(file_path, 'rb') as f:
        count = 0
        for record in ArchiveIterator(f):
            if record.rec_type == 'response':
                # [필터 1] 수집 당시 상태 코드가 200인 것만!
                if record.http_headers.get_statuscode() != "200":
                    continue
                
                url = record.rec_headers.get_header('WARC-Target-URI').lower()
                if any(ex in url for ex in exclude_list): continue

                # [속도 개선 핵심] HTML 전체를 읽되, BeautifulSoup에 넣기 전에 검사!
                html_content = record.content_stream().read().decode('utf-8', 'ignore')

                # 텍스트에 'Oracle'이 없으면 BeautifulSoup을 아예 실행하지 않음
                if "oracle" not in html_content.lower(): continue

                soup = BeautifulSoup(html_content, 'html.parser')
                
                # [필터 2] 제목에 에러 메시지가 있는지 확인
                raw_title = soup.title.string if (soup.title and soup.title.string) else ""
                title = str(raw_title) # 확실하게 문자열로 변환
                if any(err in title.lower() for err in ["404", "not found", "error"]): 
                    continue

                clean_text = soup.get_text(separator=' ', strip=True)
                
                # [필터 3] 핵심 키워드 전략 수정
                # 'Oracle'이 제목에 있거나, 본문에 아주 많이 등장해야 함
                is_oracle_in_title = "Oracle" in title
                is_oracle_main = clean_text.count("Oracle") >= 2 # 본문에 3번 이상 언급될 것
                
                # is_january = ("January 2026" in clean_text or "2026-01" in url or "2026/01" in url)
                is_january = True

                if (is_oracle_in_title or is_oracle_main) and is_january:
                    # 너무 짧은 페이지(리스트형 페이지) 제외
                    if len(clean_text) > 1000:
                        print(f"[진짜 뉴스 발견!]")
                        print(f"URL: {url}")
                        print(f"Title: {title.strip()}")
                        print("-" * 50)
                        count += 1
            
            if count >= 5: break
    print(f"분석 완료! 총 {count}개의 고품질 뉴스를 찾았습니다.")

process_warc_pro("sample.warc.gz")