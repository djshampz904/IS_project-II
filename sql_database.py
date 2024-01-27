import etl
import pandas as pd
import mysql.connector

#create table
def create_table(connection, query):
    """Create a table in a database"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Table created successfully")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")