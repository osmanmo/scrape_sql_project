import os
import pandas as pd
import sqlalchemy
import zipfile
from bs4 import BeautifulSoup
import requests

url = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download/'

# Make the request to the URL
response = requests.get(url)

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(response.content, 'html.parser')

# Find the "Business" header
h2s = soup.find_all('h2')
for h2 in h2s:
    print(h2.text)
