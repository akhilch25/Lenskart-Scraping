##### Required Libraries #####
import requests
import json
import pandas as pd





##### Required variables #####

# API format
# "https://api-gateway.juno.lenskart.com/v2/products/category/[ID for glass]?page-size=25&page="

# id for type of glasses correspondingly
glassType = {
    # "eyeglass": "3363",
    "computer_glass": "8427"
    # "sunglass": "3362"
}

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





##### Collecting Data #####

for Gtype in glassType:
    # current page
    curr_page = 0

    # empty dataframe
    df = pd.DataFrame(columns = cols)

    # API
    api = "https://api-gateway.juno.lenskart.com/v2/products/category/" + glassType[Gtype] + "?page-size=25&page=" 

    # get data from api
    res = requests.get(api + str(curr_page))

    # extract data from api
    print("Start collecting data for",Gtype,"...")
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
                    if item == "market_price":
                        value = product['prices'][0]['price']
                    elif item == "lenskart_price":
                        value = product['prices'][1]['price']
                    else:
                        value = product[item]  
                except:
                    value = None
                details.append(value)
            df.loc[len(df.index)] = details
        curr_page += 1
        res = requests.get(api + str(curr_page))

    # show data
    print(df)

    # file name
    filename = Gtype + "_data.csv"

    # convert the data into csv
    df.to_csv(filename, index=False)
    print("Data collection is completed for for",Gtype,"...")