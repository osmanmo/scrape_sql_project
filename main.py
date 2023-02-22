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
business_header = soup.find('h2', string=lambda text: text and 'business' in text.lower())

# Find all links after the "Business" header
links = business_header.find_next_siblings('a')
