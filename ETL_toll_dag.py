# import the libraries

from datetime import timedelta

from airflow import DAG # The DAG object

from airflow.operators.bash_operator import BashOperator # Operators;

from airflow.utils.dates import days_ago # This makes scheduling easy

#defining DAG arguments
default_args = {
    'owner': 'JC',
    'start_date': days_ago(0),
    'email': ['jc@email.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='ETL TOll Data Dag',
    schedule_interval=timedelta(days=1),
)


# define the unzip task
unzip_data = BashOperator(
    task_id='unzip',
    bash_command='tar -xzvf tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)


# define the extract task: extract data from csv file
extract_data_from_csv = BashOperator(
    task_id='extract_csv',
    bash_command='cut -d"," -f1-4 /vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)


# define the extract task: extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id='extract_tsv',
    bash_command="cut -d$'\t' -f5-7 /vehicle-data.csv > /home/project/airflow/dags/finalassignment/tsv_data.csv",
    dag=dag,
)

# define the third task: extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id='extract_fixed_width',
    bash_command="cut -d' ' -f6-7 /payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv",
    dag=dag,
)

# define the consolidate task: consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste csv_data.csv tsv_data.csv fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv",
    dag=dag,
)


# define the transofrm task
transform_data= BashOperator(
    task_id='tranform',
    bash_command="cut -d',' -f4 /home/project/airflow/dags/finalassignment/extracted_data.csv | tr 'a-z' 'A-Z' > /home/project/airflow/dags/finalassignment/staging",
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data


