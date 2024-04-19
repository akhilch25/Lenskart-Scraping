from scrapping import scrap_data
from cleaning import clean_data
from creating_tables import create_tables


if __name__ == '__main__':
    try:
        # scrap product data and store data
        scrap_data()

        # clean data
        clean_data()

        # create tables
        create_tables()

    except Exception as e:
        print(e)

