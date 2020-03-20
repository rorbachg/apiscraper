import requests
import logging
import pandas as pd
from pandas.io.json import json_normalize
import yaml
import re
import glob, os
import urllib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(name)s : %(message)s')
logger = logging.getLogger('APIScraper')  

class Scraper():
    def __init__(self, config):
        self._endpoints = [config.get('api').get('url') + endpoint for endpoint in config.get('api').get('endpoints')]

    @property
    def endpoints(self):
        return self._endpoints

    def run(self):
        for endpoint in self.endpoints:
            self.scrape_endpoint(endpoint)
        self.download_photos()

    @staticmethod
    def scrape_endpoint(endpoint):
        logger.info(F'Scraping endpoint {endpoint}')
        response = requests.get(endpoint)
        dataframe = json_normalize(response.json())
        filename = re.findall('https://.*?\/([a-z]+).*$', endpoint)[0] + '.csv'
        logger.info(F'Filename is {filename}')
        dataframe.to_csv(filename, index=False)

    def download_photos(self):
        if 'photos.csv' in glob.glob('*'):
            Path("photos/").mkdir(parents=True, exist_ok=True)
            dataframe = pd.read_csv('photos.csv')
            filenames = dataframe['url'].apply(lambda x: 'photos/' + re.findall('.*/([0-9a-z]+)', x)[0] + '.jpg').to_list()
            urls = dataframe['url'].to_list()
            try:
                for url, filename in zip(urls[:5], filenames[:5]):
                    self.download_photo(url, filename)
            finally:
                dataframe['file_path'] = filenames
                dataframe.to_csv('photos.csv', index=False)

    @staticmethod
    def download_photo(url, filename):
        response = requests.get(url)
        if response.status_code == 200:
            logger.info(F'Saving {filename}')
            with open(filename, 'wb+') as f:
                f.write(response.content)

        
if __name__ == '__main__':
    with open('config.yml', 'r') as ymlfile:
        config = yaml.load(ymlfile, Loader=yaml.FullLoader)
    scraper = Scraper(config)
    scraper.run()
    logger.info('Done')