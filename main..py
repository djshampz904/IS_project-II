import etl
import pandas as pd

if __name__ == "__main__":
    file_path = "data/WA_Fn-UseC_-Telco-Customer-Churn.csv"
    df = pd.read_csv(file_path, sep=',', header=0)
    table_names = {
                "customer_table": ["customerid", "gender", "seniorcitizen", "partner", "dependents"],
                "service_table": ["customerid", "phonedervice", "multiplelines", "internetservice"],
                "security_table": ["customerid", "onlinesecurity", "onlinebackup", "deviceprotection", "techsupport"],
                "streaming_table": ["customerid", "streamingtv", "streamingmovies"],
                "billing_table": ["customerid", "contract", "paperlessbilling", "paymentmethod", "monthlycharges", "totalcharges", "churn"]
    }
   
    #create connection to database
    connection = etl.create_db_connection(host_name="localhost", user_name="root", user_password="toor", db_name="telco_churn")  
   
    #extract column names
    column_names = etl.extract_column_names(df)
    
    #create column types
    column_types = etl.create_column_types(df)
    
    #print column names that are in the dictionary with key value customer_table
    total_columns = len(column_names)
    
    for table, columns in table_names.items():
        drop_query = f"DROP TABLE IF EXISTS {table};"
        etl.drop_table(connection, drop_query)
        
        query = f"CREATE TABLE {table} ("
        for i in range(total_columns):
            if column_names[i] in table_names[table]:
                query += f" {column_names[i]} {column_types[i]},"
        query = query.rstrip(",")  # remove trailing comma
        query += " )"
        etl.create_table(connection, query)