# 📈 Oracle Stock Sentiment Data Pipeline




---




## 🇰🇷 프로젝트 개요 (Korean Summary)

이 프로젝트는 금융 뉴스 데이터를 수집하고 감성 분석을 통해 정량화한 뒤, 주가 데이터와 결합하여 시장 변화를 분석하는 데이터 파이프라인입니다.

Apache Airflow와 AWS(Lambda, S3, EC2)를 활용하여 자동화된 ETL 파이프라인을 구축했으며, 이를 통해 뉴스 감성과 주가 간의 관계를 분석할 수 있는 데이터 환경을 만들었습니다.

### 💡 주요 결과

* 부정적인 뉴스 감성 증가 이후 단기적인 주가 하락 패턴 확인
* 감성 데이터가 시장 변화를 설명하는 선행 지표로 활용될 가능성 발견

이 프로젝트는 데이터 파이프라인 구축뿐만 아니라, 데이터를 통해 실제 의미 있는 인사이트를 도출하는 과정을 포함합니다.



---





## ❓ Problem

Can news sentiment predict stock price movement?

Financial markets are influenced by external information such as news, but quantifying this relationship is not straightforward.
This project explores whether daily news sentiment can serve as a leading indicator for stock price changes.

---

## 🎯 Goal

* Build an automated data pipeline to collect and process financial news
* Quantify sentiment using NLP
* Analyze the relationship between sentiment and stock price
* Enable data-driven insights for market behavior

---

## 🧠 Key Insights

* Negative sentiment spikes often **precede short-term drops** in Oracle stock price
* Identified a **lag effect** between sentiment changes and market response
* Demonstrated that sentiment data can act as a **potential leading indicator**

---

## 🏗️ Architecture Overview

This project implements an end-to-end data pipeline on AWS:

* **Orchestration**: Apache Airflow (EC2)
* **Processing**: AWS Lambda (Serverless)
* **Storage**: Amazon S3 (Data Lake)
* **Analysis**: Python + NLP (TextBlob)

---

## ⚙️ Pipeline Workflow

1. **Orchestration**

   * Airflow triggers the pipeline daily at 08:00 AM KST

2. **Extraction**

   * AWS Lambda collects financial news from Google News RSS
   * Filters articles from the last 24 hours

3. **Transformation**

   * Parses XML data
   * Removes noise using keyword filtering
   * Applies sentiment analysis (TextBlob)

4. **Load**

   * Stores processed data in S3 Data Lake
   * Automatically partitioned by year/month

---

## 📊 Data Visualization

### Stock Price vs Sentiment Trend

This graph compares Oracle stock prices with daily sentiment scores.

👉 Observation:

* As sentiment increases, stock prices tend to stabilize or rise
* Sharp sentiment drops are followed by price declines

---

## 💰 Cost Optimization Insight

* Total AWS cost: **$9.54**
* Average daily cost: **$0.31**

👉 Demonstrates that scalable data pipelines can be built **cost-efficiently using serverless architecture**

---

## 💡 Why This Project Matters

This project is not just about building a data pipeline.

It shows how:

* Data infrastructure enables meaningful analysis
* Raw data can be transformed into actionable insights
* Engineering + analysis together create real value

---

## 🛠 Tech Stack

* **Language**: Python
* **Data Processing**: Pandas, TextBlob
* **Data Engineering**: AWS (S3, Lambda, EC2), Airflow
* **Parsing**: BeautifulSoup, XML

---

## 🚀 Future Improvements

* Replace TextBlob with advanced NLP models (e.g., FinBERT)
* Expand to multi-stock analysis
* Incorporate real-time streaming pipeline
* Apply predictive modeling (time series forecasting)

---

## 🔗 Related Resources

* GitHub Repository
* Blog Posts (detailed implementation & analysis)

---

## 🧩 Takeaway

👉 Data is only valuable when it leads to decisions.

This project demonstrates how building the right data pipeline enables extracting insights that can support real-world decision making.


<img width="1744" height="704" alt="image" src="https://github.com/user-attachments/assets/d89c1690-2eee-43fd-b75c-d9f0d49d0b68" />


<img width="1741" height="534" alt="image" src="https://github.com/user-attachments/assets/ec00c127-aa9b-4231-b23d-5ed7f9148584" />

<img width="813" height="500" alt="image" src="https://github.com/user-attachments/assets/e74694bf-55b6-46f2-b866-3740b9481b58" />


You can read posts about this project in my blog! It is in the repository of 'w1024ji.github.io'.
