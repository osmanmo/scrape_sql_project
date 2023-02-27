import json
import logging
import os
import zipfile

import pandas as pd
import requests
import sqlalchemy
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

base_url = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download'


def get_list_of_urls_in_business_section(base_url):
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    data_value = soup.find('div', id='pageViewData')['data-value']
    data_value = json.loads(data_value)
    page_blocks = data_value['PageBlocks']
    business_block = [block for block in page_blocks if block['Title'] == 'Business'][0]
    block_documents = business_block['BlockDocuments']
    urls = [block_document['DocumentLink'] for block_document in block_documents]
    urls = set(urls)
    absolute_base_url = base_url.split('/')[0] + '//' + base_url.split('/')[2]
    complete_urls = [absolute_base_url + url for url in urls]
    logger.info(complete_urls)
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
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, chunksize=1000)


def main(config):
    db_uri = config.SQLALCHEMY_DATABASE_URI
    engine = sqlalchemy.create_engine(db_uri)
    logger.info(f"here is the db_uri {db_uri}")
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
                    os.remove(file)

                    # print(df.head())

            else:
                if extracted_files.endswith('csv'):
                    df = pd.read_csv(extracted_files, encoding='latin-1')
                    create_table(df, extracted_files, engine)
                os.remove(extracted_files)

            os.remove(file_name)

    engine.dispose()
