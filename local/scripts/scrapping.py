import requests
import json
import pandas as pd
from bs4 import BeautifulSoup 
from selenium import webdriver 
from selenium.webdriver.common.keys import Keys 
import time 
import csv



def scrap_product_data():

    ##### Required variables #####

    # API format
    # "https://api-gateway.juno.lenskart.com/v2/products/category/[ID for glass]?page-size=25&page="

    # id for type of glasses correspondingly
    glassType = {
        # "computer_glass": ['8427']
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



    ##### Collecting Data #####

    # extract data from API
    for Gtype in glassType:
        print("Start collecting data for",Gtype,"...")
        for id in glassType[Gtype]:
            # current page
            curr_page = 0

            # API
            api = "https://api-gateway.juno.lenskart.com/v2/products/category/" + id + "?page-size=25&page=" 

            # get data from api
            res = requests.get(api + str(curr_page))

            # extract data from api
            while res.status_code == 200:
                print("Page:", curr_page, end='\r')
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
        print("Data collection is completed for",Gtype,"^_^")

    # delete duplicate records
    df.drop_duplicates(inplace=True)

    # show data
    # print(df)

    # file name
    filename = "product_data.csv"

    # convert the data into csv
    df.to_csv("../data/raw/"+filename, index=False)



def scrap_store_data():

    states = [
        'Andhra-Pradesh','Andmaan-nicobar','Arunachal-Pradesh','Assam','Bihar','Chennai','Chhattisgarh',
        'Dadra-and-Nagar-Haveli','Delhi','Goa','Gujarat','Gwalior','Haryana','Himachal-Pradesh','Hyderabad',
        'Jammu-and-Kashmir','Jharkhand','Karnataka','Kerala','Madhya-Pradesh','Maharashtra','Manipur','Nagaland',
        'New-Delhi','Odisha','Patna','Puducherry','Punjab','Rajasthan','Sikkim','Tamil-Nadu','Telangana',
        'Tripura','Uttar-Pradesh','Uttarakhand','West-Bengal'
    ]

    itemfull=[]

    # initiating the webdriver. 
    driver = webdriver.Chrome()

    for i in states:
        url = "https://www.lenskart.com/stores/location/{}".format(i)

        # Parameter includes the path of the webdriver. from selenium import webdriver
        driver.get(url)  

        # this is just to ensure that the page is loaded 
        time.sleep(3)  
        html = driver.page_source 
        
        # this renders the JS code and stores all 
        # of the information in static HTML code. 
        # Now, we could simply apply bs4 to html variable 
        soup = BeautifulSoup(html, "html.parser") 
        content=soup.find_all('div',class_='StoreCard_storeAddressContainer__pBYqN')
        header=["Store Location","Address","Phone","Timings"]
        
        for item in content:
            items=[]
            store=item.find('a',class_='StoreCard_name__mrTXJ')
            address=item.find('a',class_='StoreCard_cursor_pointer___SGuZ')
            phone=item.find('div',class_='StoreCard_wrapper__xhJ0A')
            hours=item.find('div',class_='StoreCard_storeAddress__PfC_v')
            # ratings=item.find('div',class_='StoreCard_storeRating__dJst3')

            items.append(store.text)
            items.append(address.text)
            # items.append(ratings.text)
            items.append(phone.text)
            items.append(hours.text)
            
            # print("Store Location:",store.text)
            # print("Address:",address.text)
            # print("Ratings:",ratings.text)
            # print("Phone:",phone.text)
            # print(hours.text)

            itemfull.append(items)

    with open('../data/raw/store_data.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(itemfull)
        
    driver.close() # closing the webdriver 
    print("Data collection is completed for stores ^_^")





def scrap_data():
    print("Data scrapping process is started...")
    scrap_product_data()
    # scrap_store_data()
    print("Data scrapping is done ^_^\n")


