import yfinance as yf
import pandas as pd

def get_oracle_stock():
    print("Fetching Oracle ($ORCL) stock price...")
    # 프로젝트 분석 기간에 맞춰 설정 (뉴스 데이터 기간과 맞춤)
    # 최근 1개월간의 데이터를 일일 단위로 가져옵니다.
    oracle_stock = yf.download("ORCL", start="2026-01-01", end="2026-03-01")
    
    if not oracle_stock.empty:
        # 데이터 구조를 평탄화 (Date를 컬럼으로 뺌)
        oracle_stock.reset_index(inplace=True)
        # 필요한 컬럼만 선택 (날짜, 종가)
        stock_df = oracle_stock[['Date', 'Close']]
        
        stock_df.to_csv("oracle_stock_price.csv", index=False)
        print("주가 데이터 수집 완료! 'oracle_stock_price.csv'를 확인하세요.")
        print(stock_df.head())
    else:
        print("주가 데이터를 가져오지 못했습니다.")

get_oracle_stock()

