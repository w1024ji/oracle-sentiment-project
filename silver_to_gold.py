import boto3
import json
import pandas as pd

def refine_silver_to_gold():
    s3 = boto3.resource('s3')
    bucket_name = "oracle-sentiment-wonji-project" 
    silver_prefix = "silver/2026/01/"
    
    bucket = s3.Bucket(bucket_name)
    gold_data = []
    
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

                oracle_count = text.count("oracle")
                is_about_oracle = ("oracle" in title) or (oracle_count >= 6)
                
                is_buffett = "omaha" in title or "buffett" in title
                
                is_foreign = any(w in text for w in [" en ", " por ", " las ", " los "])

                if is_about_oracle and not is_buffett and not is_foreign:
                    # 주식 뉴스 증거가 하나라도 있으면 합격
                    if any(k in text for k in target_keywords) or "orcl" in text:
                        gold_data.append(item)

    # 최종 결과 저장 및 보고
    if gold_data:
        df = pd.DataFrame(gold_data)
        
        before_count = len(df)
        df = df.drop_duplicates(subset=['title'], keep='first')
        after_count = len(df)
        
        print(f"중복된 기사 {before_count - after_count}개를 제거했습니다.")

        output_name = "oracle_gold_january.csv"
        df.to_csv(output_name, index=False, encoding='utf-8-sig')
        
        print("-" * 50)
        print(f"정제 완료! 총 {len(gold_data)}개의 진짜 Oracle 뉴스를 확보했습니다.")
        print(f"저장 파일: {output_name}")
        
    else:
        print("필터링 결과 남은 데이터가 없습니다. 기준을 조금 완화해보세요.")

if __name__ == "__main__":
    refine_silver_to_gold()
