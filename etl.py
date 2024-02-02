import pandas as pd
import mysql.connector

# The code will extract columns form csv and put them in a sql database

def extract_data_from_csv(file_path):
    """Extract data from csv file and return a dataframe"""
    df = pd.read_csv(file_path, sep=',')
    return df

def create_db_connection(host_name, user_name, user_password, db_name):
    """Create a connection to a database"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")
    return connection

def extract_column_names(df):
    """Extract column names from a dataframe"""
    column_names = []
    for column in map(str.lower, df.columns):
        column_names.append(column)
    return column_names

def create_column_types(df):
    """Create column types for a database"""
    column_types = []
    for column, dtype in df.dtypes.items():
        if dtype == 'float64':
            column_types.append('FLOAT')
        elif dtype == 'object':
            try:
                # Try to convert the column to integers in the DataFrame
                df[column] = df[column].astype(int)
                # If successful, add 'INT' to the column types
                column_types.append('INT')
            except ValueError:
                # If it fails, add 'VARCHAR(255)' to the column types
                column_types.append('VARCHAR(255)')
        elif dtype == 'int64':
            column_types.append('INT')
        elif dtype == 'datetime64[ns]':
            column_types.append('DATETIME')
    return column_types

def create_table(connection, query):
    """Create a table in a database"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Table created successfully")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")

def drop_table(connection, query):
    """Drop a table"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Table dropped successfully")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")

def insert_data_into_table(connection, query):
    """Insert data into a table"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Data inserted successfully")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")

