import etl
import pandas as pd
import numpy as np

if __name__ == "__main__":
    table_names = {
                "customer_table": ["customerid", "gender", "seniorcitizen", "partner", "dependents"],
                "service_table": ["customerid", "phoneservice", "multiplelines", "internetservice"],
                "security_table": ["customerid", "onlinesecurity", "onlinebackup", "deviceprotection", "techsupport"],
                "streaming_table": ["customerid", "streamingtv", "streamingmovies"],
                "billing_table": ["customerid", "contract", "paperlessbilling", "paymentmethod", "monthlycharges", "totalcharges", "churn"]
    }
    
    file_path = "data/WA_Fn-UseC_-Telco-Customer-Churn.csv"
    df = pd.read_csv(file_path, sep=',', header=0)
    df.columns = map(str.lower, df.columns)
    
    #remove duplicates
    for table, columns in table_names.items():
        df = df.drop_duplicates(subset=columns)
        
    # Replace empty cells with NaN
    df["totalcharges"].replace("", np.nan, inplace=True)

    # Convert 'totalcharges' to numeric, coercing errors
    df["totalcharges"] = pd.to_numeric(df["totalcharges"], errors="coerce")
    
    #round up the totalcharges column to 2 decimal places
    df["totalcharges"] = df["totalcharges"].round(2)
    
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
        
        
    #insert data into the database
    for table, columns in table_names.items():
        query = f"INSERT INTO {table} ("
        for column in table_names[table]:
            query += f"{column}, "
        query = query.rstrip(", ")
        query += ") VALUES ("
        for row in df[columns].values:
            for value in row:
                # check if value is NaN and replace with NULL
                if pd.isnull(value):
                    query += "NULL, "
                elif not isinstance(value, int):
                    query += f"'{value}', "
                else:
                    query += f"{value}, "
            query = query.rstrip(", ")
            query += "), ("
        query = query.rstrip(", (")
        query += ";"
        print(query)
        etl.insert_data_into_table(connection, query)
    
    for value in df["totalcharges"]:
        if value == None:
            print(value)

