import yfinance as yf
import feedparser
import pandas as pd
from datetime import datetime

def get_oracle_news():
    news_data = []
    
    # 1. Yahoo Finance 뉴스 수집 (에러 방지 로직 추가)
    # 수정할 Yahoo Finance 수집 부분
    print("Fetching news from Yahoo Finance (RSS Mode)...")
    try:
        # 야후 파이낸스 오라클 뉴스 RSS URL
        yf_rss_url = "https://finance.yahoo.com/rss/headline?s=ORCL"
        yf_feed = feedparser.parse(yf_rss_url)
        
        for entry in yf_feed.entries:
            news_data.append({
                'source': 'Yahoo Finance',
                'title': entry.get('title', 'No Title'),
                'link': entry.get('link', ''),
                'published': entry.get('published', '') # RSS는 문자열 날짜를 줌
            })
    except Exception as e:
        print(f"Yahoo Finance RSS 수집 중 오류 발생: {e}")

    # 2. Google News RSS 수집
    print("Fetching news from Google News...")
    try:
        gn_url = "https://news.google.com/rss/search?q=Oracle+stock&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(gn_url)
        for entry in feed.entries[:20]:
            news_data.append({
                'source': 'Google News',
                'title': entry.get('title', 'No Title'),
                'link': entry.get('link', ''),
                'published': entry.get('published', '')
            })
    except Exception as e:
        print(f"Google News 수집 중 오류 발생: {e}")
    
    return pd.DataFrame(news_data)

# 실행 및 저장
df = get_oracle_news()
if not df.empty:
    df.to_csv("oracle_news_raw.csv", index=False, encoding='utf-8-sig')
    print(f"총 {len(df)}개의 뉴스를 수집했습니다!")
    print(df.head()) # 상위 5개 데이터 미리보기
else:
    print("수집된 데이터가 없습니다.")

