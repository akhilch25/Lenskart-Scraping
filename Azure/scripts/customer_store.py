import logging
import csv
import pandas as pd
import pyodbc
from dotenv import load_dotenv
import os


load_dotenv(dotenv_path='./.env')
database_conn_str = os.getenv('database_connection_string')



# clean and upload customer data
def clean_n_upload_customer_data():
    # file path
    raw_file_path = "./data/customers.csv"
    clean_file_path = "/tmp/customer_clean.csv"

    # open files to read and write data
    raw_file = open(raw_file_path,'r')
    clean_file = open(clean_file_path,'w')
    temp = raw_file.readline().split(',')
    temp[1], temp[2] = 'first_name', 'last_name'
    clean_file.write(','.join(temp))
    temp = raw_file.readline().strip()
    for line in raw_file.readlines():
        if line[:4] == 'CUST':
            clean_file.write(temp+'\n')
            temp = line.strip()
        else:
            temp += line.strip()
    clean_file.write(temp+'\n')

    # close files
    raw_file.close()
    clean_file.close()

    logging.info("Customer data cleaning completed ^_^")



    # read clean file
    csvreader = csv.reader(open("/tmp/customer_clean.csv",'r'))

    # create connection with database
    conn = pyodbc.connect(database_conn_str)
    cursor = conn.cursor()

    # insert data from csv file
    header = next(csvreader)
    header = ','.join(header)
    for row in csvreader:
        for i in range(len(row)):
            if row[i] == '':
                row[i] = "null"
            else:
                row[i] = "'" + row[i] + "'"
        value = ','.join(row)
        cursor.execute("INSERT INTO customer (" + header + ") VALUES (" + value + ");")
    
    # Commit changes in the database and close connection 
    conn.commit() 
    cursor.close()
    conn.close()
    
    logging.info("data is inserted into customer table successfully ^_^")



# clean and upload store data
def clean_n_upload_store_data():
    # file path
    raw_file_path = "./data/store_final.csv"
    clean_file_path = "/tmp/store_clean.csv"

    # convert into data frame
    df = pd.read_csv(raw_file_path)
    df = df[['id', 'address_full', 'address_city', 'address_state', 'address_pin_code', 'store_email','store_phone']]
    df.columns = ['store_id','address','city','state','pincode','email','phone']

    # convert into csv
    df.to_csv(clean_file_path, index=False)
    logging.info("Store data cleaning completed ^_^")



    # open the csv file and read data
    csvreader = csv.reader(open("/tmp/store_clean.csv",'r'))
    
    # create connection with database
    conn = pyodbc.connect(database_conn_str)
    cursor = conn.cursor()

    # insert data from csv file
    header = next(csvreader)
    header = ','.join(header)
    for row in csvreader:
        row[1] = row[1].replace("'", "")
        for i in range(len(row)):
            if row[i] == '':
                row[i] = "null"
            elif i in [1,2,3,5,6]:
                row[i] = "'" + row[i] + "'"
        value = ','.join(row)
        cursor.execute("INSERT INTO store (" + header + ") VALUES (" + value + ");")
    
    # Commit changes in the database and close connection 
    conn.commit() 
    cursor.close()
    conn.close()
    
    logging.info("data is inserted into store table successfully ^_^")



def clean_n_upload_customer_n_store_data():
    clean_n_upload_customer_data()
    clean_n_upload_store_data()
