from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import tarfile
import csv
import os

# Define the path for the input and output files
source_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
destination_path = '/home/project/airflow/dags/python_etl/staging'

# Ensure the destination directory exists
os.makedirs(destination_path, exist_ok=True)

# Function to download the dataset
def download_dataset():
    response = requests.get(source_url, stream=True)
    if response.status_code == 200:
        with open(f"{destination_path}/tolldata.tgz", 'wb') as f:
            f.write(response.raw.read())
    else:
        raise Exception(f"Failed to download the file. Status code: {response.status_code}")

# Function to untar the dataset
def untar_dataset():
    with tarfile.open(f"{destination_path}/tolldata.tgz", "r:gz") as tar:
        tar.extractall(path=destination_path)

# Function to extract data from CSV
def extract_data_from_csv():
    input_file = f"{destination_path}/vehicle-data.csv"
    output_file = f"{destination_path}/csv_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in infile:
            row = line.strip().split(',')
            # Rowid, Timestamp, Anonymized Vehicle number, Vehicle type
            writer.writerow([row[0], row[1], row[2], row[3]])

# Function to extract data from TSV
def extract_data_from_tsv():
    input_file = f"{destination_path}/tollplaza-data.tsv"
    output_file = f"{destination_path}/tsv_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in infile:
            row = line.strip().split('\t')
            # Number of axles, Tollplaza id, Tollplaza code
            writer.writerow([row[4], row[5], row[6]])

# Function to extract data from fixed width file
def extract_data_from_fixed_width():
    input_file = f"{destination_path}/payment-data.txt"
    output_file = f"{destination_path}/fixed_width_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in infile:
            # Slicing based on the typical payment-data.txt structure (last two fields)
            payment_code = line[58:62].strip()
            vehicle_code = line[62:].strip()
            writer.writerow([payment_code, vehicle_code])

# Function to consolidate data
def consolidate_data():
    csv_file = f"{destination_path}/csv_data.csv"
    tsv_file = f"{destination_path}/tsv_data.csv"
    fw_file = f"{destination_path}/fixed_width_data.csv"
    output_file = f"{destination_path}/extracted_data.csv"

    with open(csv_file, 'r') as f_csv, open(tsv_file, 'r') as f_tsv, open(fw_file, 'r') as f_fw, open(output_file, 'w', newline='') as f_out:
        r_csv = csv.reader(f_csv)
        r_tsv = csv.reader(f_tsv)
        r_fw = csv.reader(f_fw)
        writer = csv.writer(f_out)

        for row_csv, row_tsv, row_fw in zip(r_csv, r_tsv, r_fw):
            writer.writerow(row_csv + row_tsv + row_fw)

# Function to transform data
def transform_data():
    input_file = f"{destination_path}/extracted_data.csv"
    output_file = f"{destination_path}/transformed_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            # Vehicle type is at index 3
            row[3] = row[3].upper()
            writer.writerow(row)

# Default arguments
default_args = {
    'owner': 'Your Name',
    'start_date': days_ago(0),
    'email': ['your_email@mail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Tasks
download_task = PythonOperator(task_id='download_dataset', python_callable=download_dataset, dag=dag)
untar_task = PythonOperator(task_id='untar_dataset', python_callable=untar_dataset, dag=dag)
extract_csv_task = PythonOperator(task_id='extract_data_from_csv', python_callable=extract_data_from_csv, dag=dag)
extract_tsv_task = PythonOperator(task_id='extract_data_from_tsv', python_callable=extract_data_from_tsv, dag=dag)
extract_fw_task = PythonOperator(task_id='extract_data_from_fixed_width', python_callable=extract_data_from_fixed_width, dag=dag)
consolidate_task = PythonOperator(task_id='consolidate_data', python_callable=consolidate_data, dag=dag)
transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)

# Pipeline
download_task >> untar_task >> [extract_csv_task, extract_tsv_task, extract_fw_task] >> consolidate_task >> transform_task