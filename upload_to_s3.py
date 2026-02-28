import boto3
import os

def upload_files_to_s3(bucket_name):
    # AWS 인증 정보가 PC에 설정되어 있어야 합니다 (aws configure)
    s3 = boto3.client('s3')
    
    files = ['oracle_news_raw.csv', 'oracle_stock_price.csv']
    
    for file_name in files:
        if os.path.exists(file_name):
            print(f"Uploading {file_name} to S3...")
            s3.upload_file(file_name, bucket_name, f"raw-data/{file_name}")
            print(f"{file_name} 업로드 완료!")
        else:
            print(f"{file_name} 파일이 없습니다.")

# 본인이 만든 버킷 이름을 넣으세요
upload_files_to_s3('oracle-project-wonji')

