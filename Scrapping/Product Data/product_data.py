##### Required Libraries #####
import requests
import json
import pandas as pd





##### Required variables #####

# apis to collect data for eyeglasses, computer glasses, and sunglasses
eyeglass_api = "https://api-gateway.juno.lenskart.com/v2/products/category/3363?page-size=25&page="
computer_Glass_api = "https://api-gateway.juno.lenskart.com/v2/products/category/8427?page-size=25&page="
sunglass_api = "https://api-gateway.juno.lenskart.com/v2/products/category/3362?page-size=25&page=0"


# attributes of dataframe
cols = [
    'id', 
    'color',
    'size',
    'width',
    'brand_name',
    'model_name',
    'market_price',
    'lenskart_price',
    'classification',
    'wishlistCount',
    'purchaseCount',
    'avgRating',
    'totalNoOfRatings',
    'offerName',
    'qty',
    'isCashbackApplicable'
]


# dataframes to store data from apis
eyeglass_df = pd.DataFrame(columns = cols)
computer_Glass_df = pd.DataFrame(columns = cols)
sunglass_df = pd.DataFrame(columns = cols)





##### Eyeglasses data #####

# # current page
# curr_page = 0

# # get data from api
# res = requests.get(eyeglass_api + str(curr_page))

# # extract data from api
# print("Start collecting data for eyeglasses...")
# while res.status_code == 200:
#     data = json.loads(res.text)
#     products = data['result']['product_list']
#     if len(products) == 0:
#         break
#     for product in products:
#         details = []
#         for item in cols:
#             try:
#                 if item == "market_price":
#                     value = product['prices'][0]['price']
#                 elif item == "lenskart_price":
#                     value = product['prices'][1]['price']
#                 else:
#                     value = product[item]
#             except:
#                 value = None
#             details.append(value)
#         eyeglass_df.loc[len(eyeglass_df.index)] = details
#     curr_page += 1
#     res = requests.get(eyeglass_api + str(curr_page))


# # show data
# print(eyeglass_df)

# # convert the data into csv
# eyeglass_df.to_csv('eyeglass_data.csv')
# print("Data collection is completed for Eyeglasses...")





##### Computer Glasses data #####

# # current page
# curr_page = 0

# # get data from api
# res = requests.get(computer_Glass_api + str(curr_page))

# # extract data from api
# print("Start collecting data for Computer Glasses...")
# while res.status_code == 200:
#     data = json.loads(res.text)
#     products = data['result']['product_list']
#     if len(products) == 0:
#         break
#     for product in products:
#         details = []
#         for item in cols:
#             try:
#                 if item == "market_price":
#                     value = product['prices'][0]['price']
#                 elif item == "lenskart_price":
#                     value = product['prices'][1]['price']
#                 else:
#                     value = product[item]
#             except:
#                 value = None
#             details.append(value)
#         computer_Glass_df.loc[len(computer_Glass_df.index)] = details
#     curr_page += 1
#     res = requests.get(computer_Glass_api + str(curr_page))

# # show data
# print(computer_Glass_df)

# # convert the data into csv
# computer_Glass_df.to_csv('computer_Glass_data.csv')
# print("Data collection is completed for Computer Glasses...")





##### Sunglasses data #####
# current page
curr_page = 0

# get data from api
res = requests.get(sunglass_api + str(curr_page))

# extract data from api
print("Start collecting data for Sunglasses...")
while res.status_code == 200:
    data = json.loads(res.text)
    products = data['result']['product_list']
    if len(products) == 0:
        break
    for product in products:
        details = []
        for item in cols:
            try:
                if item == "market_price":
                    value = product['prices'][0]['price']
                elif item == "lenskart_price":
                    value = product['prices'][1]['price']
                else:
                    value = product[item]  
            except:
                value = None
            details.append(value)
        sunglass_df.loc[len(sunglass_df.index)] = details
    curr_page += 1
    res = requests.get(sunglass_api + str(curr_page))

# show data
print(sunglass_df)

# convert the data into csv
sunglass_df.to_csv('sunglass_data.csv')
print("Data collection is completed for Sunglasses...")