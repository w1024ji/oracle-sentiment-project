import boto3
import json
import pandas as pd

def refine_silver_to_gold():
    s3 = boto3.resource('s3')
    bucket_name = "oracle-sentiment-wonji-project" 
    silver_prefix = "silver/2026/01/"
    
    bucket = s3.Bucket(bucket_name)
    gold_data = []

    exclude_keywords = [
        "warren buffett", "omaha", "berkshire", "buffett",  # 워렌 버핏 관련 (Oracle of Omaha)
        "orclon", "orclx", "mexc", "token", "crypto",      # 크립토/파생 토큰 관련
        "jakarta", "indonesia", "turkey", "turkish",       # 특정 국가 뉴스
        "phk", "fiyat", "tahmini", "karyawan"              # 외국어 전용 단어 (해고, 가격, 예측 등)
    ]
    
    target_keywords = ["nyse", "nasdaq", "cloud", "database", "earnings", "quarterly", "openai", "stock", "share"]

    print("🚀 Silver 데이터를 분석하여 최상위 Gold 데이터로 정제 중...")

    for obj in bucket.objects.filter(Prefix=silver_prefix):
        if obj.key.endswith('.json'):
            # Spark JSON (JSON Lines) 읽기
            content = obj.get()['Body'].read().decode('utf-8')
            for line in content.strip().split('\n'):
                if not line: continue
                
                item = json.loads(line)
                title = item.get('title', '').lower()
                text = item.get('content', '').lower()
                url = item.get('url', '').lower()

                if "oracle" not in title or "omaha" in title or "buffett" in title:
                    continue
                
                if any(n in text or n in title or n in url for n in exclude_keywords):
                    continue
                
                is_stock_news = ("orcl" in text) or any(k in text for k in target_keywords)

                if is_stock_news:
                    # 데이터 용량 최적화를 위해 본문은 2000자까지만 Gold로 승격
                    item['content'] = item['content'][:2000]
                    gold_data.append(item)

    # 4. 최종 결과 저장 및 보고
    if gold_data:
        df = pd.DataFrame(gold_data)
        # 엑셀에서 바로 열 수 있게 utf-8-sig로 저장
        output_name = "oracle_gold_january_final.csv"
        df.to_csv(output_name, index=False, encoding='utf-8-sig')
        
        print("-" * 50)
        print(f"✨ 정제 완료! 총 {len(gold_data)}개의 진짜 Oracle 뉴스를 확보했습니다.")
        print(f"📁 저장 파일: {output_name}")
        print("-" * 50)
        
        # 샘플 확인용 출력
        print("\n📈 Gold 데이터 샘플 (상위 3개):")
        for i, row in df.head(3).iterrows():
            print(f"[{i+1}] {row['title']}")
    else:
        print("❌ 필터링 결과 남은 데이터가 없습니다. 기준을 조금 완화해보세요.")

if __name__ == "__main__":
    refine_silver_to_gold()
