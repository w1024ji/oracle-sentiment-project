import yfinance as yf
import json
import pandas as pd
from datetime import datetime

# 1. 설정값
TICKER_SYMBOL = "ORCL"
# yfinance의 end date는 'exclusive'(포함하지 않음)이므로,
# 24일까지 가져오려면 25일로 설정해야 해.
START_DATE = "2026-03-08"
END_DATE = "2026-03-25" 
OUTPUT_FILENAME = "oracle_stock_march.json"

def get_stock_data_to_json():
    print(f"{TICKER_SYMBOL} 주가 데이터 수집 시작 ({START_DATE} ~ 2026-03-24)...")
    
    try:
        # 2. yfinance를 이용해 데이터 다운로드
        df = yf.download(TICKER_SYMBOL, start=START_DATE, end=END_DATE)
        
        if df.empty:
            print("❌ 해당 기간의 데이터가 없습니다.")
            return

        # 3. 데이터 정제 (날짜와 종가(Close)만 추출)
        # yfinance는 Date가 Index로 되어 있어서 인덱스를 리셋해줌
        df = df.reset_index()
        
        # 필요한 필드만 선택하고 이름 변경
        df_cleaned = df[['Date', 'Close']].copy()
        df_cleaned.columns = ['date', 'close_price']
        
        # 날짜 형식을 'YYYY-MM-DD' 문자열로 변환
        df_cleaned['date'] = df_cleaned['date'].dt.strftime('%Y-%m-%d')
        
        # 종가를 소수점 2자리까지 반올림
        df_cleaned['close_price'] = df_cleaned['close_price'].round(2)

        # 4. JSON으로 변환 (리스트 오브 딕셔너리 형식)
        # [{date: '...', close_price: ...}, ...]
        stock_list = df_cleaned.to_dict(orient='records')

        # 5. 로컬 파일로 저장
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(stock_list, f, ensure_ascii=False, indent=4)
            
        print(f"✅ 성공: {OUTPUT_FILENAME} 파일이 생성되었습니다. (총 {len(stock_list)}영업일)")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")

if __name__ == "__main__":
    get_stock_data_to_json()