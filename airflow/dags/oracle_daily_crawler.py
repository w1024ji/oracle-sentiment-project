from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'wonji',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 14), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='oracle_sentiment_daily_crawler',
    default_args=default_args,
    description='trigger oracle news every morning',
    schedule='0 23 * * *',  
    catchup=False,
    tags=['oracle', 'lambda', 'crawler'],
) as dag:

    invoke_crawler_lambda = LambdaInvokeFunctionOperator(
        task_id='trigger_oracle_crawler',
        function_name='',
        invocation_type='RequestResponse',
        aws_conn_id='aws_default',
    )

    invoke_crawler_lambda
