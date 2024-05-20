import pandas as pd
import csv
import pyodbc
import logging
from dotenv import load_dotenv
import os


load_dotenv(dotenv_path='./.env')
database_conn_str = os.getenv('database_connection_string')


def clean_n_upload_transaction_data():
    conn = pyodbc.connect(database_conn_str)
    cursor = conn.cursor()

    product_df = pd.read_sql("select product_id from product", conn)
    customer_df = pd.read_sql("select customer_id from customer", conn)
    store_df = pd.read_sql("select store_id from store", conn)
    transaction_df = pd.read_csv("./data/transaction.csv")

    unique_product_id = set(product_df['product_id'])
    unique_customer_id = set(customer_df['customer_id'])
    unique_store_id = set(store_df['store_id'])

    index_list = []
    for ind in transaction_df.index:
        if (transaction_df['customer_id'][ind] not in unique_customer_id or 
            transaction_df['product_id'][ind] not in unique_product_id or
            transaction_df['store_id'][ind] not in unique_store_id):
            index_list.append(ind)
    
    transaction_df.drop(index=index_list, inplace=True)
    transaction_df.reset_index()
    transaction_df.index += 1


    # convert into csv
    transaction_df.to_csv("/tmp/transaction_clean.csv")

    file = open("/tmp/transaction_clean.csv",'r')
    data = file.readlines()
    file.close()

    data[0] = 'id'+data[0]
    file = open("/tmp/transaction_clean.csv",'w')
    file.writelines(data)
    file.close()

    logging.info("Transaction data cleaning completed ^_^")



    # delete previous records
    cursor.execute("DELETE FROM transaction_table;")
    conn.commit() 

    # open the csv file and read data
    csvreader = csv.reader(open("/tmp/transaction_clean.csv",'r'))

    # insert data from csv file
    header = ','.join(next(csvreader))
    for row in csvreader:
        value = row.copy()
        for i in range(len(value)):
            if value[i] == '':
                value[i] = "null"
            elif i in [1,2,3,7,8]:
                value[i] = "'" + value[i] + "'"
        value = ','.join(value)
        
        cursor.execute("INSERT INTO transaction_table (" + header + ") VALUES (" + value + ");")
    
    # Commit changes in the database and close connection 
    conn.commit() 
    cursor.close()
    conn.close()
    
    logging.info("data is inserted into transaction table successfully ^_^")
