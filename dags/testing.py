import etl
import pandas as pd
import numpy as np


def extract_data_from_csv(file_path):
    """Extract data from csv file and return a dataframe"""
    data_frame = pd.read_csv(file_path, sep=',')
    return data_frame


def transform_data(df, table_names):
    # remove duplicates
    for table, columns in table_names.items():
        df = df.drop_duplicates(subset=columns)

    # Replace empty cells with NaN
    df["totalcharges"].replace("", np.nan, inplace=True)
    # Convert 'totalcharges' to numeric, coercing errors
    df["totalcharges"] = pd.to_numeric(df["totalcharges"], errors="coerce")

    # round up the totalcharges column to 2 decimal places
    df["totalcharges"] = df["totalcharges"].round(2)

    return df


def load_data(df, table_names, connection):
    # extract column names
    column_names = etl.extract_column_names(df)

    # create column types
    column_types = etl.create_column_types(df)

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
        #check if index exists
        index_exists_query = f"SHOW INDEX FROM {table} WHERE Key_name = 'idx_{table}_customerid';"
        cursor = connection.cursor()
        cursor.execute(index_exists_query)
        result = cursor.fetchone()
        if result:
            continue
        else:
            query = f"CREATE INDEX idx_{table}_customerid ON {table}(customerid);"
            etl.create_index(connection, query)


if __name__ == "__main__":
    table_names = {
        "service_table": ["customerid", "phoneservice", "multiplelines", "internetservice", "tenure"],
        "security_table": ["customerid", "onlinesecurity", "onlinebackup", "deviceprotection", "techsupport"],
        "streaming_table": ["customerid", "streamingtv", "streamingmovies"],
        "billing_table": ["customerid", "contract", "paperlessbilling", "paymentmethod", "monthlycharges",
                          "totalcharges", "churn"],
        "customer_table": ["customerid", "gender", "seniorcitizen", "partner", "dependents"]
    }

    file_path = "data/churn.csv"
    df = extract_data_from_csv(file_path)
    df.columns = map(str.lower, df.columns)

    df = transform_data(df, table_names)

    # create connection to database
    connection = etl.create_db_connection(host_name="localhost", user_name="shvmpz", user_password="Pioneer.254",
                                          db_name="telco_churn")

    load_data(df, table_names, connection)
