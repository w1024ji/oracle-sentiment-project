import boto3
import json
import pandas as pd
from textblob import TextBlob

def refine_and_analyze():
    s3 = boto3.resource('s3')
    bucket_name = "oracle-sentiment-wonji-project"
    # silver_prefix = "silver/2026/01/"
    silver_prefix = "silver/2026/02/" # 2월 폴더로 변경
    output_name = "oracle_gold_analysis_february.csv" # 2월 결과 파일명

    bucket = s3.Bucket(bucket_name)
    gold_list = []

    # exclude_keywords = ["warren buffett", "omaha", "berkshire", "buffett", "phk", "fiyat", "tahmini"]
    target_keywords = ["nyse", "nasdaq", "cloud", "database", "earnings", "openai", "stock", "share"]

    print("🚀 Silver 데이터를 읽어 Gold로 정제 및 감성 분석 중...")

    for obj in bucket.objects.filter(Prefix=silver_prefix):
        if obj.key.endswith('.json'):
            content = obj.get()['Body'].read().decode('utf-8')

        for line in content.strip().split('\n'):
            if not line: continue
            item = json.loads(line)
            title = item.get('title', '').lower()
            text = item.get('content', '').lower()

            oracle_count = text.count("oracle")
            is_about_oracle = ("oracle" in title) or (oracle_count >= 3)
            
            is_buffett = any(b in title for b in ["omaha", "buffett"])
            
            is_foreign_title = any(w in title for w in [" en ", " por ", " las ", " los "])

            if is_about_oracle and not is_buffett and not is_foreign_title:
                if any(k in text for k in target_keywords) or "orcl" in text:
                    # if not any(e in title for e in exclude_keywords):
                        
                    analysis = TextBlob(item['content'])
                    item['sentiment_score'] = round(analysis.sentiment.polarity, 4)
                    gold_list.append(item)

    if gold_list:
        df = pd.DataFrame(gold_list)

        before_count = len(df)
        df = df.drop_duplicates(subset=['title'], keep='first')
        after_count = len(df)
        print(f"중복된 기사 {before_count - after_count}개를 제거했습니다.")

        # output_name = "oracle_gold_analysis_january.csv"
        df.to_csv(output_name, index=False, encoding='utf-8-sig')
        
        print("-" * 50)
        print(f"분석 완료! {len(df)}개의 기사가 Gold 계층으로 확정되었습니다.")
        print(f"평균 감성 점수: {df['sentiment_score'].mean():.4f}")
        print("-" * 50)
    else:
        print("정제 결과 데이터가 없습니다.")

if __name__ == "__main__":
    refine_and_analyze()