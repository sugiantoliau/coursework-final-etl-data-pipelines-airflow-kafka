from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# --- Task 1.1  ---
#defining DAG arguments
default_args = {
    'owner': 'Your Name',
    'start_date': days_ago(0),
    'email': ['your_email@mail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Task 1.2 ---
# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# --- Task 2.1: Unzip data ---
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)

# --- Task 2.2: Extract from CSV ---
# Fields: Rowid (1), Timestamp (2), Anonymized Vehicle number (3), Vehicle type (4)
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)

# --- Task 2.3: Extract from TSV ---
# Fields: Number of axles (5), Tollplaza id (6), Tollplaza code (7)
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | tr "\\t" "," > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

# --- Task 2.4: Extract from Fixed Width ---
# Fields: Type of Payment code (last two fields in the payment-data.txt)
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c59-67 /home/project/airflow/dags/finalassignment/payment-data.txt | tr " " "," > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)

# --- Task 2.5: Consolidate data ---
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d"," /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag,
)

# --- Task 2.6: Transform data ---
# Convert vehicle_type (column 4) to uppercase
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk -F"," \'BEGIN {OFS=","} {$4=toupper($4); print}\' /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/transformed_data.csv',
    dag=dag,
)

# --- Task 2.7: Define the task pipeline ---
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data