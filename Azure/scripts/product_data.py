import logging
import requests
import json
import pandas as pd
import csv
import pyodbc
from dotenv import load_dotenv
import os


load_dotenv(dotenv_path='./.env')
database_conn_str = os.getenv('database_connection_string')



def scrapping(types):
    # id for type of glasses correspondingly
    glassType = {
        "computer_glass": ['8427'],
        "eyeglass": ['3363'],
        "sunglass": ['3362'],
        "contact_lenses": ['16637', '16631', '16634', '16639', '16522', '16632', '16553', '16633', '16612', '4592', '8460', '4585', '10413', '16609', '16641', '16630', '16607', '16638', '16643', '16635', '16541', '16640']
    }

    # attributes of dataframe
    cols = [
        'id', 
        'color',
        'size',
        'width',
        'brand_name',
        'model_name',
        'lenskart_price',
        'classification',
        'wishlistCount',
        'purchaseCount',
        'avgRating',
        'totalNoOfRatings',
        'qty'
    ]

    # dataframe to store product data from apis
    df = pd.DataFrame(columns = cols)

    
    # extract data from API
    for Gtype in types:
        logging.info(f"Start collecting data for {Gtype}...")
        for id in glassType[Gtype]:
            # current page
            curr_page = 0

            # API
            api = "https://api-gateway.juno.lenskart.com/v2/products/category/" + id + "?page-size=25&page=" 

            # get data from api
            res = requests.get(api + str(curr_page))

            # extract data from api
            while res.status_code == 200:
                data = json.loads(res.text)
                products = data['result']['product_list']
                if len(products) == 0:
                    break
                for product in products:
                    details = []
                    for item in cols:   
                        try:
                            if item == "lenskart_price":
                                value = product['prices'][1]['price']
                            else:
                                value = product[item]  
                        except:
                            value = None
                        details.append(value)
                    df.loc[len(df.index)] = details
                curr_page += 1
                res = requests.get(api + str(curr_page))
        logging.info(f"Data collection is completed for {Gtype} ^_^")

    # delete duplicate records
    df.drop_duplicates(inplace=True)

    # convert the data into csv
    df.to_csv("/tmp/product_data.csv", index=False)
    logging.info("Data collection is completed ^_^")



def cleaning():
    unique_id = set()

    # file path
    raw_file_path = "/tmp/product_data.csv"
    clean_file_path = "/tmp/product_clean.csv"


    # open files
    csvreader = csv.reader(open(raw_file_path,'r'))
    clean_file = open(clean_file_path,'w')

    header = next(csvreader)
    header[0] = 'product_id'
    header = ','.join(header) + '\n'

    clean_file.write(header)
    
    for row in csvreader:
        if row[0] not in unique_id and row[7] in ['eyeframe','sunglasses','contact_lens']:
            unique_id.add(row[0])
            clean_file.write(','.join(row) + '\n')
    
    # close file
    clean_file.close()

    logging.info("Product data cleaning completed ^_^")

    

def insert_data():
    # create connection with database
    conn = pyodbc.connect(database_conn_str)
    cursor = conn.cursor()
    query = '''
        MERGE INTO product AS target
        USING (VALUES (146012,'Green','Small','134 mm','Lenskart Air','LA E13517',2000,'eyeframe',2904,88591,4.65,153,5442)) AS source (
            product_id, color, size, width, brand_name, model_name, lenskart_price,classification, wishlistCount, purchaseCount, avgRating, totalNoOfRatings, qty
        )
        ON target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET
                color = source.color,
                size = source.size,
                width = source.width,
                brand_name = source.brand_name,
                model_name = source.model_name,
                lenskart_price = source.lenskart_price,
                classification = source.classification,
                wishlistCount = source.wishlistCount,
                purchaseCount = source.purchaseCount,
                avgRating = source.avgRating,
                totalNoOfRatings = source.totalNoOfRatings,
                qty = source.qty
        WHEN NOT MATCHED THEN
            INSERT (product_id, color, size, width, brand_name, model_name, lenskart_price, classification, wishlistCount, purchaseCount, avgRating, totalNoOfRatings, qty)
            VALUES (source.product_id, source.color, source.size, source.width, source.brand_name, source.model_name, source.lenskart_price, source.classification, source.wishlistCount, source.purchaseCount, source.avgRating, source.totalNoOfRatings, source.qty);
    '''
    query1 = '''
        MERGE INTO product AS target
        USING (VALUES ('''
    query2 = ''')) AS source (
            product_id, color, size, width, brand_name, model_name, lenskart_price,classification, wishlistCount, purchaseCount, avgRating, totalNoOfRatings, qty
        )
        ON target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET
                color = source.color,
                size = source.size,
                width = source.width,
                brand_name = source.brand_name,
                model_name = source.model_name,
                lenskart_price = source.lenskart_price,
                classification = source.classification,
                wishlistCount = source.wishlistCount,
                purchaseCount = source.purchaseCount,
                avgRating = source.avgRating,
                totalNoOfRatings = source.totalNoOfRatings,
                qty = source.qty
        WHEN NOT MATCHED THEN
            INSERT (product_id, color, size, width, brand_name, model_name, lenskart_price, classification, wishlistCount, purchaseCount, avgRating, totalNoOfRatings, qty)
            VALUES (source.product_id, source.color, source.size, source.width, source.brand_name, source.model_name, source.lenskart_price, source.classification, source.wishlistCount, source.purchaseCount, source.avgRating, source.totalNoOfRatings, source.qty);
    '''

    # open the csv file and read data
    csvreader = csv.reader(open("/tmp/product_clean.csv",'r'))

    # insert data from csv file
    header = next(csvreader)
    header = ','.join(header)

    # insert data into tables
    for row in csvreader:
        for i in range(len(row)):
            if row[i] == '':
                row[i] = "null"
            elif i in [1,2,3,4,5,7]:
                row[i] = "'" + row[i] + "'"
        value = ','.join(row)
        
        cursor.execute(query1 + value + query2)

    logging.info('Data uploaded into the table ^_^')


    # Commit changes in the database and close connection 
    conn.commit()
    cursor.close()
    conn.close()



def scrap_n_store_product_data1():
    scrapping(['eyeglass'])
    cleaning()
    insert_data()


def scrap_n_store_product_data2():
    scrapping(['sunglass','contact_lenses'])
    cleaning()
    insert_data()