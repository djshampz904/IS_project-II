import etl
import pandas as pd
import numpy as np

if __name__ == "__main__":
    table_names = {
                "service_table": ["customerid", "phoneservice", "multiplelines", "internetservice", "tenure"],
                "security_table": ["customerid", "onlinesecurity", "onlinebackup", "deviceprotection", "techsupport"],
                "streaming_table": ["customerid", "streamingtv", "streamingmovies"],
                "billing_table": ["customerid", "contract", "paperlessbilling", "paymentmethod", "monthlycharges", "totalcharges", "churn"],
                "customer_table": ["customerid", "gender", "seniorcitizen", "partner", "dependents"]
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
    connection = etl.create_db_connection(host_name="localhost", user_name="shvmpz", user_password="Pioneer.254", db_name="telco_churn")
   
    #extract column names
    column_names = etl.extract_column_names(df)
    
    #create column types
    column_types = etl.create_column_types(df)
    
    #print column names that are in the dictionary with key value customer_table
    total_columns = len(column_names)
    
    #create table
    for table, columns in table_names.items():
        #drop foreign key
        drop_query = f"DROP TABLE IF EXISTS {table};"
        etl.drop_table(connection, drop_query)

        query = f"CREATE TABLE {table} ("
        if table != "customer_table":
            query += f"id INT AUTO_INCREMENT PRIMARY KEY,"
        for i in range(total_columns):
            if column_names[i] in table_names[table]:
                query += f" {column_names[i]} {column_types[i]},"
        query = query.rstrip(",")  # remove trailing comma
        query += " )"
        etl.create_table(connection, query)


    #insert data into the database
    for table, columns in table_names.items():
        etl.insert_data_into_table(connection, df, table, columns)

    for value in df["totalcharges"]:
        if value == None:
            print(value)
            
    #add primary keys to the tables
    for table, columns in table_names.items():
        if table == "customer_table":
            query = f"ALTER TABLE {table} ADD PRIMARY KEY (customerid);"
        else:
            query = f"ALTER TABLE {table} ADD PRIMARY KEY (id);"
        
        etl.add_primary_key(connection, query)

    #add foreign keys to the tables
    for table, columns in table_names.items():
        if table != "customer_table":
            query = f"ALTER TABLE {table} ADD FOREIGN KEY (customerid) REFERENCES customer_table(customerid);"
            etl.add_foreign_key(connection, query)
