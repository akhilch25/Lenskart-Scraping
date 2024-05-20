import csv
import os
import pandas as pd  



# clean product data
def clean_product_data():
    unique_id = set()

    # file path
    raw_file_path = "../data/raw/product_data.csv"
    clean_file_path = "../data/clean/product_clean.csv"

    # delete clean_file if a copy exists
    if os.path.isfile(clean_file_path):
        os.remove(clean_file_path)

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

    print("Product data cleaning completed ^_^")



# clean store data
def clean_store_data():

    # file path
    raw_file_path = "../data/raw/store_final.csv"
    clean_file_path = "../data/clean/store_clean.csv"

    # convert into data frame
    df = pd.read_csv(raw_file_path)
    df = df[['id', 'address_full', 'address_city', 'address_state', 'address_pin_code', 'store_email','store_phone']]
    df.columns = ['store_id','address','city','state','pincode','email','phone']
    # delete clean_file if a copy exists
    if os.path.isfile(clean_file_path):
        os.remove(clean_file_path)

    # convert into csv
    df.to_csv(clean_file_path, index=False)
    print("Store data cleaning completed ^_^")



# clean customer data
def clean_customer_data():

    # file path
    raw_file_path = "../data/raw/customers.csv"
    clean_file_path = "../data/clean/customer_clean.csv"

    # delete clean_file if a copy exists
    if os.path.isfile(clean_file_path):
        os.remove(clean_file_path)

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

    print("Customer data cleaning completed ^_^")



# clean transaction data
def clean_transaction_data():
    product_df = pd.read_csv("../data/clean/product_clean.csv")
    customer_df = pd.read_csv("../data/clean/customer_clean.csv")
    store_df = pd.read_csv("../data/clean/store_clean.csv")
    transaction_df = pd.read_csv("../data/raw/transaction.csv")

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


    # delete clean_file if a copy exists
    if os.path.isfile("../data/clean/transaction_clean.csv"):
        os.remove("../data/clean/transaction_clean.csv")

    # convert into csv
    # transaction_df.to_csv("../data/clean/transaction_clean.csv", index=False)
    transaction_df.to_csv("../data/clean/transaction_clean.csv")

    file = open("../data/clean/transaction_clean.csv",'r')
    data = file.readlines()
    file.close()

    data[0] = 'id'+data[0]
    file = open("../data/clean/transaction_clean.csv",'w')
    file.writelines(data)
    file.close()

    print("Transaction data cleaning completed ^_^")





def clean_data():
    print("Cleaning process is started...")
    clean_product_data()
    clean_store_data()
    clean_customer_data()
    clean_transaction_data()
    print("Cleaning process is done ^_^\n")


