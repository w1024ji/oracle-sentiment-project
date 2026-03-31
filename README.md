# 📈 Oracle Stock Sentiment Data Pipeline

An automated, end-to-end data engineering pipeline that extracts daily news articles about Oracle (ORCL), performs Natural Language Processing (NLP) sentiment analysis, and stores the enriched data in an AWS S3 Data Lake. 

This project aims to build the foundational data layer required to analyze the correlation between market news sentiment and Oracle's stock price movements.

## 🏗 Architecture & Tech Stack

* **Language:** Python 3.10
* **Data Extraction & NLP:** `requests`, `BeautifulSoup4` (XML Parsing), `TextBlob` (Sentiment Analysis)
* **Compute:** AWS Lambda (Serverless Data Processing)
* **Orchestration:** Apache Airflow (running on an Ubuntu EC2 `t3.small` instance)
* **Storage:** Amazon S3 (Data Lake)

## ⚙️ Pipeline Workflow

1. **Orchestration:** Apache Airflow triggers the workflow daily at 08:00 AM KST (23:00 UTC) using the `LambdaInvokeFunctionOperator`.
2. **Extraction:** An AWS Lambda function connects to the Google News RSS feed, querying articles strictly from the last 24 hours (`when:1d`) related to "Oracle Stock ORCL".
3. **Transformation & Filtering:** * Parses the XML response.
    * Excludes noise/irrelevant articles based on a predefined blacklist (e.g., "warren buffett", "omaha").
    * Validates relevance using a target keyword whitelist (e.g., "earnings", "ai", "openai").
    * Calculates a sentiment polarity score (ranging from -1.0 to 1.0) for the combined title and summary using `TextBlob`.
4. **Load:** The processed "Gold" data is formatted into JSON and loaded into an Amazon S3 bucket (`oracle-sentiment-wonji-project`), automatically partitioned by year and month (e.g., `gold/2026/03/oracle_meta_2026-03-15.json`).

## 🚀 Deployment & Setup

### 1. AWS Lambda Setup
* Create a Python-based Lambda function named `oracle-news-collector`.
* Add the required external libraries (`requests`, `bs4`, `textblob`, `lxml`) as a Lambda Layer.
* Attach an IAM execution role with `AmazonS3FullAccess` (or scoped S3 put access).

### 2. S3 Bucket
* Create an S3 bucket named `oracle-sentiment-wonji-project`.
* No public access is required; access is managed via IAM roles.

### 3. Airflow Orchestration (EC2)
The pipeline is orchestrated via a standalone Apache Airflow instance hosted on an AWS EC2 Ubuntu machine.

```bash
# Clone the repository and place the DAG file in the Airflow directory
cp dags/oracle_daily_crawler.py ~/airflow/dags/

```

<img width="1744" height="704" alt="image" src="https://github.com/user-attachments/assets/d89c1690-2eee-43fd-b75c-d9f0d49d0b68" />


<img width="1741" height="534" alt="image" src="https://github.com/user-attachments/assets/ec00c127-aa9b-4231-b23d-5ed7f9148584" />



You can read posts about this project in my blog! It is in the repository of 'w1024ji.github.io'.
