import sqlite3
import csv
from sqlite3 import Error

try:
    # type of glasses
    glasses = ["eyeglass_data", "computer_glass_data", "sunglass_data"]
    
    # connect database
    conn = sqlite3.connect('../database/data.sqlite3') 
    
    # Creating a cursor object 
    cursor = conn.cursor() 


    for Gtype in glasses:

        # open the csv file and read data
        filename = "../data/"+Gtype+".csv"
        file = open(filename,'r')
        csvreader = csv.reader(file)
        
        # create table
        table = """CREATE TABLE """ + Gtype + """(
                    pkey INTEGER PRIMARY KEY AUTOINCREMENT,
                    id INTEGER NOT NULL,
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
                    qty INTEGER, 
                    isCashbackApplicable VARCHAR(5)
                );"""    
        cursor.execute(table) 

        # insert data from csv file
        header = next(csvreader)
        temp = ""
        for item in header:
            temp += "'"+item+"',"

        for row in csvreader:
            value = ""
            for i in range(len(row)):
                if row[i] == '':
                    value += "null,"
                else:
                    if i in [1,2,3,4,5,8,13]:
                        value += "'" + row[i] + "',"
                    elif i == 15:
                        value += "'" + row[i] + "'"
                    else:
                        value += row[i] + ","

            cursor.execute(
                '''INSERT INTO ''' + Gtype + ''' (''' + temp[:-1] + ") " +
                '''VALUES (''' + value + ") "
            )

        # close file
        file.close()
        print("Data inserted for " + Gtype[:-5] + "...\n")

    
    # Commit your changes in the database     
    conn.commit() 
    
    # Closing the connection 
    conn.close()

except Error as e:
    print(e)