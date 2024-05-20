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
                lenskart_price INTEGER,
                classification VARCHAR(50),
                wishlistCount INTEGER, 
                purchaseCount INTEGER, 
                avgRating REAL, 
                totalNoOfRatings INTEGER, 
                qty INTEGER
            );"""    
    cursor.execute(table) 
    print("product table is created ^_^")

    # insert data from csv file
    header = next(csvreader)
    header = ','.join(header)

    for row in csvreader:
        for i in range(len(row)):
            if row[i] == '':
                row[i] = "null"
            elif i in [1,2,3,4,5,7]:
                row[i] = "'" + row[i] + "'"
        value = ','.join(row)
        
        cursor.execute("INSERT INTO product (" + header + ") VALUES (" + value + ");")
    
    # Commit changes in the database     
    conn.commit() 
    
    print("data is inserted into product table successfully ^_^")



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
    print("customer table is created ^_^")

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

    print("data is inserted into customer table successfully ^_^")



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
    print("store table is created ^_^")

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
    
    print("data is inserted into store table successfully ^_^")



# create transaction table
def create_transaction_table(conn, cursor):
    # open the csv file and read data
    csvreader = csv.reader(open("../data/clean/transaction_clean.csv",'r'))

    # drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS transaction_table;") 


    table = """CREATE TABLE transaction_table(
                id INTEGER PRIMARY KEY,
                order_id VARCHAR(7),
                transaction_id VARCHAR(11),
                customer_id VARCHAR(50),
                product_id INTEGER,
                store_id INTEGER,
                quantity INTEGER,
                order_date VARCHAR(10),
                payment_method VARCHAR(20),
                FOREIGN KEY(customer_id) REFERENCES customer(customer_id),
                FOREIGN KEY(product_id) REFERENCES product(product_id),
                FOREIGN KEY(store_id) REFERENCES store(store_id)
            );"""    
    cursor.execute(table) 
    print("transaction table is created ^_^")

    # insert data from csv file
    header = ','.join(next(csvreader))
    
    for row in csvreader:
        value = row.copy()
        for i in range(len(value)):
            if value[i] == '':
                value[i] = "null"
            elif i in [1,2,3,7,8]:
                value[i] = '"' + value[i] + '"'
        value = ','.join(value)
        
        cursor.execute("INSERT INTO transaction_table (" + header + ") VALUES (" + value + ");")
    
    # Commit changes in the database 
    conn.commit() 
    
    print("data is inserted into transaction table successfully ^_^")





def create_tables():
    print("Creating is tables is started...")
    try:
        # connect database and Creating a cursor object 
        conn = sqlite3.connect('../database/data.sqlite3') 
        cursor = conn.cursor() 

        create_product_table(conn, cursor)
        create_customer_table(conn, cursor)
        create_store_table(conn, cursor)
        create_transaction_table(conn, cursor)


        # Closing the connection 
        conn.close()
        print("All tables are created ^_^\n") 


    except Error as e:
        print(e)


