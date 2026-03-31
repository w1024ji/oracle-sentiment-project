import streamlit as st
import pandas as pd
import json
import glob
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="Oracle Sentiment & Stock Dashboard", layout="wide")

st.title("📈 Oracle 주가 및 뉴스 감성 분석 대시보드")

@st.cache_data
def load_data():
    sentiment_files = glob.glob('oracle_meta_*.json')
    sentiment_list = []
    
    for file in sentiment_files:
        filename = os.path.basename(file)
        date_str = filename.split('_')[2].replace('.json', '')
        
        with open(file, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                continue
            
            scores = []
            
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and 'sentiment_score' in item:
                        scores.append(item.get('sentiment_score', 0))
            elif isinstance(data, dict):
                if 'sentiment_score' in data:
                    scores.append(data.get('sentiment_score', 0))
                    
            if scores:
                avg_score = sum(scores) / len(scores)
            else:
                avg_score = 0
                
            sentiment_list.append({
                'Date': date_str,
                'Sentiment': avg_score
            })
            
    df_sentiment = pd.DataFrame(sentiment_list)
    if not df_sentiment.empty:
        df_sentiment['Date'] = pd.to_datetime(df_sentiment['Date'])

    # 주가 데이터 불러오기
    with open('oracle_stock_march.json', 'r', encoding='utf-8') as f:
        stock_data = json.load(f)
        
    df_stock = pd.DataFrame(stock_data) 
    df_stock['Date'] = pd.to_datetime(df_stock['date']) 
    
    # Outer Join으로 병합 (주말 데이터 보존)
    df_merged = pd.merge(df_stock, df_sentiment, on='Date', how='outer')
    df_merged = df_merged.sort_values('Date').reset_index(drop=True)
    
    return df_merged

# 데이터 로드 및 시각화
try:
    df = load_data()
    
    st.write("### 📊 통합 데이터 미리보기")
    st.dataframe(df[['Date', 'close_price', 'Sentiment']].style.format({'close_price': '{:.2f}', 'Sentiment': '{:.4f}'}))
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 1. 주가 라인 차트 (기본 Y축 - 왼쪽) : 굵은 파란색 실선
    fig.add_trace(
        go.Scatter(x=df['Date'], y=df['close_price'], name="Oracle 주가 (USD)", 
                   mode='lines+markers',
                   line=dict(color='#1f77b4', width=3), 
                   connectgaps=True),
        secondary_y=False,
    )

    # 2. 감성 지수 라인 차트 (보조 Y축 - 오른쪽) : 얇은 주황색 점선 + 마커
    # go.Bar 에서 go.Scatter 로 변경되었습니다.
    fig.add_trace(
        go.Scatter(x=df['Date'], y=df['Sentiment'], name="일일 평균 감성 지수", 
                   mode='lines+markers',
                   line=dict(color='#ff7f0e', width=2, dash='dot'), 
                   connectgaps=True),
        secondary_y=True,
    )

    fig.update_layout(
        title_text="3월 Oracle 주가 흐름과 뉴스 감성 지수 비교",
        hovermode="x unified",
        plot_bgcolor='rgba(240, 240, 240, 0.3)'
    )
    fig.update_xaxes(title_text="날짜")
    fig.update_yaxes(title_text="<b>주가 (USD)</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>평균 감성 지수 (-1 ~ 1)</b>", secondary_y=True)

    st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"데이터 처리 중 에러가 발생했습니다: {e}")