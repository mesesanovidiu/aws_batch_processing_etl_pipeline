import pandas as pd
import psycopg2
import boto3
from dotenv import load_dotenv
import os
from datetime import date
from sql_queries import create_table_dim_products, create_sales_staging_table, create_table_dim_customers, create_table_dim_date, create_table_dim_status, create_table_fact_sales
from sql_queries import update_dim_products, update_dim_customers, update_dim_date, update_dim_status, add_foreign_key_constraints

load_dotenv()

# Set up AWS credentials and S3 client
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
s3_bucket_name = 'sales-data-pipeline'
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)

redshift_host = os.getenv('redshift_host')
redshift_port = os.getenv('redshift_port')
redshift_dbname = os.getenv('redshift_dbname')
redshift_user = os.getenv('redshift_user')
redshift_password = os.getenv('redshift_password')
redshift_conn = psycopg2.connect(
    host=redshift_host,
    port=redshift_port,
    dbname=redshift_dbname,
    user=redshift_user,
    password=redshift_password
)

# Define the S3 key (object name) for the file
s3_staging_key = f'staging/raw_sales_{date.today()}'
# Define the S3 key (object name) for the transformed file
transformed_s3_key = f'transformed/sales_{date.today()}'

# Define Redshif cursor
redshift_cursor = redshift_conn.cursor()

def extract_data_to_staging():
    file_url = 'https://github.com/mesesanovidiu/sales-data-pipeline/raw/main/raw_files/raw_sales2.csv'
    df = pd.read_csv(file_url)
    # Convert the DataFrame to a binary string and upload it to S3
    csv_file = df.to_csv(index=False)
    s3_client.put_object(Body=csv_file, Bucket=s3_bucket_name, Key=s3_staging_key)

    print(f"File uploaded to s3://{s3_bucket_name}/{s3_staging_key}")

extract_data_to_staging()

def transform_data():
    # Get the file from S3
    response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_staging_key)
    csv_file = response['Body']
    df = pd.read_csv(csv_file)

    # Convert orderdate and shipdate columns to date format
    df['ORDERDATE'] = pd.to_datetime(df['ORDERDATE'], format="%Y-%m-%d")
    df['SHIPDATE'] = pd.to_datetime(df['SHIPDATE'], format="%Y-%m-%d")

    df = df.rename(columns={'MSRP': 'suggested_retail_price'})

    # Convert the DataFrame to a binary string and upload it to S3
    csv_file = df.to_csv(index=False)
    s3_client.put_object(Body=csv_file, Bucket=s3_bucket_name, Key=transformed_s3_key)

    print(f"Transformed file uploaded to s3://{s3_bucket_name}/{transformed_s3_key}")

transform_data()

def create_tables_in_redshift():
    redshift_cursor.execute(create_sales_staging_table)
    redshift_cursor.execute(create_table_dim_products)
    redshift_cursor.execute(create_table_dim_customers)
    redshift_cursor.execute(create_table_dim_date)
    redshift_cursor.execute(create_table_dim_status)

    redshift_conn.commit()
    print(f"Table created in Redshift")

create_tables_in_redshift()

def transfer_data_to_redshift():
    s3_key = f'staging/raw_sales_{date.today()}'
    redshift_cursor = redshift_conn.cursor()
    copy_query = f"""
        COPY staging_sales
        FROM 's3://{s3_bucket_name}/{s3_key}'
        ACCESS_KEY_ID '{aws_access_key_id}'
        SECRET_ACCESS_KEY '{aws_secret_access_key}'
        CSV
        IGNOREHEADER 1
        """
    redshift_cursor.execute(copy_query)
    redshift_conn.commit()
    print(f"Data copied to Redshift table sales_staging")

transfer_data_to_redshift()

def update_dimension_tables():   
    redshift_cursor.execute(update_dim_products)
    redshift_cursor.execute(update_dim_customers)
    redshift_cursor.execute(update_dim_date)
    redshift_cursor.execute(update_dim_status)
    redshift_conn.commit()
    print(f"Dimension tables updated in Redshift")

update_dimension_tables()

def update_fact_tables():
    redshift_cursor.execute(create_table_fact_sales)
    redshift_cursor.execute(add_foreign_key_constraints)

    redshift_conn.commit()
    print(f"Fact tables updated in Redshift")

update_fact_tables()