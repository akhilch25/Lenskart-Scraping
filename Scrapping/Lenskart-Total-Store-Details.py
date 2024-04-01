from bs4 import BeautifulSoup 
from selenium import webdriver 
from selenium.webdriver.common.keys import Keys 
import time 
import csv

states=['Andhra-Pradesh','Andmaan-nicobar',
'Arunachal-Pradesh','Assam','Bihar','Chennai','Chhattisgarh','Dadra-and-Nagar-Haveli','Delhi','Goa','Gujarat',
'Gwalior','Haryana','Himachal-Pradesh','Hyderabad','Jammu-and-Kashmir','Jharkhand','Karnataka','Kerala','Madhya-Pradesh',
'Maharashtra','Manipur','Nagaland','New-Delhi','Odisha','Patna','Puducherry','Punjab','Rajasthan','Sikkim','Tamil-Nadu',
'Telangana','Tripura','Uttar-Pradesh','Uttarakhand','West-Bengal']

itemfull=[]
for i in states:
    url = "https://www.lenskart.com/stores/location/{}".format(i)
    # initiating the webdriver. Parameter includes the path of the webdriver. from selenium import webdriver
    driver = webdriver.Chrome()
    driver.get(url)  
    # this is just to ensure that the page is loaded 
    time.sleep(5)  
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
        #ratings=item.find('div',class_='StoreCard_storeRating__dJst3')

        items.append(store.text)
        items.append(address.text)
        #items.append(ratings.text)
        items.append(phone.text)
        items.append(hours.text)
        
        print("Store Location:",store.text)
        print("Address:",address.text)
        #print("Ratings:",ratings.text)
        print("Phone:",phone.text)
        print(hours.text)

        itemfull.append(items)
    with open('Lenskart-Total-Stores.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(itemfull)
    
    driver.close() # closing the webdriver 