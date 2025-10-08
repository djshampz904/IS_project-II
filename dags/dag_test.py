# creating a simple DAG that will extract data from a csv and save it to a dataframe
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import etl
import numpy as np

table_names = {
    "service_table": ["customerid", "phoneservice", "multiplelines", "internetservice", "tenure"],
    "security_table": ["customerid", "onlinesecurity", "onlinebackup", "deviceprotection", "techsupport"],
    "streaming_table": ["customerid", "streamingtv", "streamingmovies"],
    "billing_table": ["customerid", "contract", "paperlessbilling", "paymentmethod", "monthlycharges",
                      "totalcharges", "churn"],
    "customer_table": ["customerid", "gender", "seniorcitizen", "partner", "dependents"]
}


filepath = '/home/shvmpz/airflow/dags/data/churn.csv'

# define the default arguments for the DAG
default_args = {
    'owner': 'shvmpz',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'telco_churn_workflow',
    default_args=default_args,
    description='A simple ETL workflow',
    schedule_interval=timedelta(days=1),
)


# define the extract task
def extract_data_from_csv(file_path):
    """Extract data from csv file and return a dataframe"""
    # log message together with the timestamp
    etl.log_message(f"Extracting data at {datetime.now()}")
    data_frame = pd.read_csv(file_path, sep=',')
    return data_frame


extract = PythonOperator(
    task_id='extract',
    python_callable=extract_data_from_csv,
    op_kwargs={'file_path': filepath},
    dag=dag
)


# define the transform task
def transform_data(**context):
    # log message together with the timestamp
    etl.log_message(f"Transforming data at {datetime.now()}")
    df = context['task_instance'].xcom_pull(task_ids='extract')

    # convert column names to lower case
    df.columns = map(str.lower, df.columns)
    # remove duplicates
    df = df.drop_duplicates(subset='customerid')

    # Replace empty cells with NaN
    df["totalcharges"].replace("", np.nan, inplace=True)
    # Convert 'TotalCharges' to numeric, coercing errors
    df["totalCharges"] = pd.to_numeric(df["totalcharges"], errors="coerce")

    # round up the TotalCharges column to 2 decimal places
    df["totalcharges"] = df["totalcharges"].round(2)

    return df


transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)


# define the load task
def load_data(table_names, **context):
    # log message together with the timestamp
    etl.log_message(f"Loading data at {datetime.now()}")

    df = context['task_instance'].xcom_pull(task_ids='transform')
    # extract column names
    column_names = etl.extract_column_names(df)

    # create column types
    column_types = etl.create_column_types(df)

    connection = etl.create_db_connection(host_name="localhost", user_name="shvmpz", user_password="Pioneer.254",
                                          db_name="telco_churn")

    # create table
    for table, columns in table_names.items():
        # Check if table exists
        table_exists_query = f"SHOW TABLES LIKE '{table}';"
        cursor = connection.cursor()
        cursor.execute(table_exists_query)
        result = cursor.fetchone()

        # If table does not exist, create it
        if not result:
            query = f"CREATE TABLE {table} ("
            if table != "customer_table":
                query += f"id INT AUTO_INCREMENT PRIMARY KEY,"
            for i in range(len(column_names)):
                if column_names[i] in table_names[table]:
                    query += f" {column_names[i]} {column_types[i]},"
            query = query.rstrip(",")  # remove trailing comma
            query += " )"
            etl.create_table(connection, query)

        # insert data into the database
        etl.insert_data_into_table(connection, df, table, columns)

    # add primary keys to the tables
    for table, columns in table_names.items():
        # first check if primary key exists
        primary_key_exists_query = f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY';"
        cursor = connection.cursor()
        cursor.execute(primary_key_exists_query)
        result = cursor.fetchone()
        if result:
            continue
        else:
            if table == "customer_table":
                query = f"ALTER TABLE {table} ADD PRIMARY KEY (customerid);"
            else:
                query = f"ALTER TABLE {table} ADD PRIMARY KEY (id);"

            etl.add_primary_key(connection, query)

    # add foreign keys to the tables
    for table, columns in table_names.items():
        if table != "customer_table":
            query = f"ALTER TABLE {table} ADD FOREIGN KEY (customerid) REFERENCES customer_table(customerid);"
            etl.add_foreign_key(connection, query)

    for table in table_names.keys():
        # check if index exists
        index_exists_query = f"SHOW INDEX FROM {table} WHERE Key_name = 'idx_{table}_customerid';"
        cursor = connection.cursor()
        cursor.execute(index_exists_query)
        result = cursor.fetchone()
        if result:
            continue
        else:
            query = f"CREATE INDEX idx_{table}_customerid ON {table}(customerid);"
            etl.create_index(connection, query)


load = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    op_kwargs={'table_names': table_names},
    dag=dag
)

# set the task dependencies
extract >> transform >> load
