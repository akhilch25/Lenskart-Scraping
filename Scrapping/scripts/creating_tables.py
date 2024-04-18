import sqlite3
from sqlite3 import Error
import csv
import os



# create product data table
def create_product_table(conn, cursor):

    # open the csv file and read data
    csvreader = csv.reader(open("../data/clean/product_clean.csv",'r'))
    
    # drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS product;") 

    table = """CREATE TABLE product(
                product_id INTEGER PRIMARY KEY,
                color VARCHAR(255),  
                size VARCHAR(50),
                width VARCHAR(50),
                brand_name VARCHAR(255),
                model_name VARCHAR(255),
                market_price INTEGER,
                lenskart_price INTEGER,
                classification VARCHAR(50),
                wishlistCount INTEGER, 
                purchaseCount INTEGER, 
                avgRating REAL, 
                totalNoOfRatings INTEGER, 
                offerName VARCHAR(50),
                qty INTEGER
            );"""    
    cursor.execute(table) 

    # insert data from csv file
    header = next(csvreader)
    header = ','.join(header)

    for row in csvreader:
        for i in range(len(row)):
            if row[i] == '':
                row[i] = "null"
            elif i in [1,2,3,4,5,8,13]:
                row[i] = "'" + row[i] + "'"
        value = ','.join(row)
        
        cursor.execute("INSERT INTO product (" + header + ") VALUES (" + value + ");")
    
    # Commit changes in the database     
    conn.commit() 
    
    print("product table is created ^_^")



# create product data table
def create_customer_table(conn, cursor):

    # read clean file
    csvreader = csv.reader(open("../data/clean/customer_clean.csv",'r'))

    # drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS customer;") 

    # create table
    table = """CREATE TABLE customer(
                customer_id VARCHAR(50) PRIMARY KEY,
                first_name VARCHAR(255),  
                last_name VARCHAR(255),
                email VARCHAR(100),
                dob VARCHAR(10),
                address VARCHAR(500),
                city VARCHAR(50),
                region VARCHAR(50)
            );"""    
    cursor.execute(table) 

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
    
    # Commit changes in the database     
    conn.commit() 

    print("customer table is created ^_^")



# create store table
def create_store_table(conn, cursor):
    # open the csv file and read data
    csvreader = csv.reader(open("../data/clean/store_clean.csv",'r'))
    
    # drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS store;") 

    table = """CREATE TABLE store(
                store_id INTEGER PRIMARY KEY,
                address VARCHAR(1000),  
                city VARCHAR(50),
                state VARCHAR(50),
                pincode INTEGER,
                email VARCHAR(255),
                phone VARCHAR(15)
            );"""    
    cursor.execute(table) 

    # insert data from csv file
    header = next(csvreader)
    header = ','.join(header)

    for row in csvreader:
        for i in range(len(row)):
            if row[i] == '':
                row[i] = "null"
            elif i in [1,2,3,5,6]:
                row[i] = '"' + row[i] + '"'
        value = ','.join(row)
        
        cursor.execute("INSERT INTO store (" + header + ") VALUES (" + value + ");")
    
    # Commit changes in the database     
    conn.commit() 
    
    print("store table is created ^_^")



# create transaction table
def create_transaction_table(conn, cursor):
    # open the csv file and read data
    print(1)
    csvreader = csv.reader(open("../data/clean/transaction_clean.csv",'r'))

    # drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS transaction_data;") 
    print(2)
    table = """CREATE TABLE transaction_data(
                transaction_id VARCHAR(10) PRIMARY KEY,
                quantity INTEGER,
                order_date VARCHAR(10),
                payment_method VARCHAR(20)
            );"""    
    cursor.execute(table) 

    # insert data from csv file
    header = next(csvreader)
    header = "transaction_id,quantity,order_date,payment_method"

    for row in csvreader:
        value = [row[1],row[5],row[6],row[7]]
        for i in range(len(value)):
            if value[i] == '':
                value[i] = "null"
            elif i in [0,2,3]:
                value[i] = '"' + row[i] + '"'
        value = ','.join(value)
        
        cursor.execute("INSERT INTO transaction_data (" + header + ") VALUES (" + value + ");")
    
    # Commit changes in the database     
    conn.commit() 
    
    print("transaction table is created ^_^")





def create_tables():
    print("Creating is tables is started...")
    try:
        # connect database and Creating a cursor object 
        conn = sqlite3.connect('../database/data.sqlite3') 
        cursor = conn.cursor() 

        # create_product_table(conn, cursor)
        # create_customer_table(conn, cursor)
        # create_store_table(conn, cursor)
        create_transaction_table(conn, cursor)


        # Closing the connection 
        conn.close()


    except Error as e:
        print(e)


