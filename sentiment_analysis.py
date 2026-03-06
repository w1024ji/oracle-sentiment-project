import boto3
import json
import pandas as pd
from textblob import TextBlob

def analyze_oracle_sentiment():
    s3 = boto3.resource('s3')
    bucket_name = "oracle-project-wonji" 
    prefix = "project/oracle_news_jan/"
    
    bucket = s3.Bucket(bucket_name)
    all_news = []

    print("S3에서 결과 파일 읽어오는 중...")
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith('.json'):
            content = obj.get()['Body'].read().decode('utf-8')
            for line in content.strip().split('\n'):
                if line:
                    all_news.append(json.loads(line))

    df = pd.DataFrame(all_news)
    
    # 감성 점수 계산 함수 (Polarity: -1.0은 매우 부정, 1.0은 매우 긍정)
    def get_sentiment(text):
        analysis = TextBlob(text)
        return analysis.sentiment.polarity

    print("감성 분석 진행 중...")
    df['sentiment_score'] = df['content'].apply(get_sentiment)
    
    avg_sentiment = df['sentiment_score'].mean()
    print(f"\n1월 오라클 뉴스 평균 감성 점수: {avg_sentiment:.4f}")
    
    print("\n가장 부정적인 뉴스 TOP 3:")
    worst_news = df.sort_values(by='sentiment_score').head(3)
    for i, row in worst_news.iterrows():
        print(f"[{row['sentiment_score']:.2f}] {row['title']}")
        print(f"URL: {row['url']}\n")

    # 결과 저장 (나중에 주가 차트와 합치기 위함)
    df.to_csv("oracle_jan_sentiment.csv", index=False)
    print("분석 결과가 oracle_jan_sentiment.csv로 저장되었습니다.")

analyze_oracle_sentiment()