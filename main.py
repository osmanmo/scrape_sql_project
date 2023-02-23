import os
import time

import pandas as pd
import sqlalchemy
import zipfile
from bs4 import BeautifulSoup
import requests
import logging
from selenium import webdriver
from config import db_uri

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

base_url = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download'


def get_list_of_urls_in_business_section(base_url):
    logger.info('Getting list of urls in business section')
    driver = webdriver.Chrome()
    driver.get(base_url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    key_word = 'Business'
    h2s = soup.find_all('h2', text=key_word)
    links = h2s[0].parent.find_all('a')
    urls = [link['href'] for link in links]
    urls = set(urls)
    driver.close()
    absolute_base_url = base_url.split('/')
    absolute_base_url = absolute_base_url[0] + '//' + absolute_base_url[2]
    complete_urls = [absolute_base_url + url for url in urls]
    logger.info('Found {} urls'.format(len(complete_urls)))

    return complete_urls


def download_file(url, file_name):
    with open(file_name, "wb") as file:
        response = requests.get(url)
        file.write(response.content)


def unzip_file(file_name):
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall()

        # check if it is a file or a folder
        if len(zip_ref.namelist()) == 1:
            # return the name of the file
            return zip_ref.namelist()[0]
        else:
            # return a list of files
            return zip_ref.namelist()


def get_file_name_from_url(url):
    return url.split('/')[-1]

def create_table(df, file_name, engine):
    # remove the ending
    table_name = file_name.split('.')[0]
    table_name = table_name.replace('-', '_')
    table_name = table_name.replace(' ', '_')
    table_name = table_name.lower()
    # take the first 60 character if the table name is longer than 60
    if len(table_name) > 60:
        table_name = table_name[:60]
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, schema='public')


def main():
    engine = sqlalchemy.create_engine(db_uri)
    all_urls = get_list_of_urls_in_business_section(base_url)
    for url in all_urls:
        file_name = get_file_name_from_url(url)
        if file_name.endswith('csv'):
            df = pd.read_csv(url, encoding='latin-1')
            logger.info(f"was able to read csv file {file_name}")
            create_table(df, file_name, engine)

        elif file_name.endswith('zip'):
            download_file(url, file_name)
            logger.info(f"I am trying to unzip {file_name} let's see if it works")
            extracted_files = unzip_file(file_name)
            if isinstance(extracted_files, list):
                for file in extracted_files:
                    logger.info(f"trying to read {file}")
                    if file.endswith('csv'):
                        df = pd.read_csv(file, encoding='latin-1')
                        create_table(df, file, engine)
                    elif file.endswith('xlsx'):
                        df = pd.read_excel(file)

                    os.remove(file)

                    # print(df.head())

            else:
                if extracted_files.endswith('csv'):
                    df = pd.read_csv(extracted_files, encoding='latin-1')
                    create_table(df, extracted_files, engine)
                elif extracted_files.endswith('xlsx'):
                    df = pd.read_excel(extracted_files)
                # print(df.head())
                os.remove(extracted_files)

            os.remove(file_name)

    engine.dispose()


if __name__ == '__main__':
    main()
