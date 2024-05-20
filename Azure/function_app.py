from scripts.product_data import scrap_n_store_product_data1, scrap_n_store_product_data2
from scripts.customer_store import clean_n_upload_customer_n_store_data
from scripts.transaction import clean_n_upload_transaction_data
import azure.functions as func
import logging


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="trigger_1")
def trigger_1(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Working start...')

    try:
        # scrap, clean & upload product data
        scrap_n_store_product_data1()

    except Exception as e:
        logging.info(e)
        return func.HttpResponse("Something went wrong. Srt_Saa.", status_code=400)
    

    logging.info('process complete')
    return func.HttpResponse("This HTTP triggered function1 executed successfully. Srt_Saa.", status_code=200)



@app.route(route="trigger_2", auth_level=func.AuthLevel.ANONYMOUS)
def trigger_2(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Working start...')

    try:
        # scrap, clean & upload product data
        scrap_n_store_product_data2()

        # clean & upload customer and store data
        # clean_n_upload_customer_n_store_data()

    except Exception as e:
        logging.info(e)
        return func.HttpResponse("Something went wrong. Srt_Saa.", status_code=400)
    

    logging.info('process complete')
    return func.HttpResponse("This HTTP triggered function1 executed successfully. Srt_Saa.", status_code=200)



@app.route(route="trigger_3", auth_level=func.AuthLevel.ANONYMOUS)
def trigger_3(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Working start...')

    try:
        # clean & upload transaction data
        clean_n_upload_transaction_data()

    except Exception as e:
        logging.info(e)
        return func.HttpResponse("Something went wrong. Srt_Saa.", status_code=400)
    

    logging.info('process complete')
    return func.HttpResponse("This HTTP triggered function1 executed successfully. Srt_Saa.", status_code=200)




