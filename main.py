import os
import time

import pandas as pd
import sqlalchemy
import zipfile
from bs4 import BeautifulSoup
import requests

from selenium import webdriver


base_url = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download'
def get_list_of_urls_in_business_section(base_url):
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
    return complete_urls




def download_file(url, file_name):
    with open(file_name, "wb") as file:
        response = requests.get(url)
        file.write(response.content)

def unzip_file(file_name):
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall()

def get_file_name_from_url(url):
    return url.split('/')[-1]




def main():
    all_urls = get_list_of_urls_in_business_section(base_url)
    for url in all_urls:
        file_name = get_file_name_from_url(url)
        if file_name.endswith('csv'):
            df = pd.read_csv(url)
            print(df.head())



if __name__ == '__main__':
    this_url = 'https://www.stats.govt.nz/assets/Uploads/Business-operations-survey/Business-operations-survey-2021/Download-data/bos2021ModC.csv'
    df = pd.read_csv(this_url, encoding='latin-1')
    print(df.head())














