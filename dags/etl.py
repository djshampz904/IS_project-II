import pandas as pd
import mysql.connector


# creat a function that will help log our message to our file
def log_message(message):
    with open('etl_log.txt', 'a') as file:
        file.write(f'{message}\n')


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
            column_types.append('DECIMAL(10,2)')
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


def check_if_customer_exists(connection, table, customer_id):
    """Check if a customer with a specific ID already exists in a table"""
    cursor = connection.cursor()
    query = f"SELECT * FROM {table} WHERE customerid = '{customer_id}'"  # Enclose customer_id in quotes
    cursor.execute(query)
    result = cursor.fetchone()
    return result is not None


def insert_data_into_table(connection, df, table, columns):
    """Insert data into a table"""
    cursor = connection.cursor()
    for i in range(len(df)):
        customer_id = df["customerid"][i]
        if not check_if_customer_exists(connection, table, customer_id):
            values = []
            for column in columns:
                value = df[column][i]
                if type(value) == str:
                    value = f'"{value}"'
                if pd.isna(value):
                    value = "NULL"
                values.append(value)
            values = ', '.join(map(str, values))
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values})"
            try:
                cursor.execute(query)
                connection.commit()
                print("Data inserted successfully")
            except mysql.connector.Error as err:
                print(f"Error: '{err}'")


# alter tables and add primary key
def add_primary_key(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Key added successfully")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")


def drop_foreign_key(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Key dropped successfully")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")


def check_duplicates_in_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Duplicates checked successfully")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")


def add_foreign_key(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Foreign key added successfully")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()


def create_index(connection, query):
    """Create an index on a column in a table"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Index created successfully")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")
