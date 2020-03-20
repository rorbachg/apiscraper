import requests
import logging
import pandas as pd
import yaml
import re
import glob, os
import urllib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable
from datetime import datetime

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(name)s : %(message)s')
logger = logging.getLogger('APIScraper')


def tictoc(func: Callable) -> Callable:
    def counter(*args, **kwargs):
        start = datetime.now()
        func(*args, **kwargs)
        end = datetime.now()
        print(F'Elapsed time doing "{func.__name__}": {end-start}')
    return counter     


class Scraper():
    def __init__(self, config):
        self._endpoints = [config.get('api').get('url') + endpoint for endpoint in config.get('api').get('endpoints')]
        self._max_threads = config.get('app').get('threads')

    @property
    def endpoints(self):
        return self._endpoints

    @property
    def max_threads(self):
        return self._max_threads
        
    @tictoc
    def run(self):
        for endpoint in self.endpoints:
            self.scrape_endpoint(endpoint)
        self.download_photos()

    @staticmethod
    def scrape_endpoint(endpoint):
        logger.info(F'Scraping endpoint {endpoint}')
        response = requests.get(endpoint)
        dataframe = pd.json_normalize(response.json())
        filename = re.findall('https://.*?\/([a-z]+).*$', endpoint)[0] + '.csv'
        logger.info(F'Filename is {filename}')
        dataframe.to_csv(filename, index=False)

    def download_photos(self):
        if 'photos.csv' in glob.glob('*'):
            Path("photos/").mkdir(parents=True, exist_ok=True)
            dataframe = pd.read_csv('photos.csv')
            filenames = dataframe['url'].apply(lambda x: 'photos/' + re.findall('.*/([0-9a-z]+)', x)[0] + '.jpg').to_list()
            urls = dataframe['url'].to_list()
            urltuples = zip(urls, filenames)
            try:
                logger.info(F'Running on {self.max_threads} threads')
                with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                    executor.map(self.download_photo, urltuples)
            finally:
                dataframe['file_path'] = filenames
                dataframe.to_csv('photos.csv', index=False)

    @staticmethod
    def download_photo(urltuple):
        url = urltuple[0]
        filename = urltuple[1]
        logger.info(F'Loading {filename}')
        response = requests.get(url)
        if response.status_code == 200:
            logger.info(F'Saving {filename}')
            with open(filename, 'wb+') as f:
                f.write(response.content)
        return None

   

if __name__ == '__main__':
    with open('config.yml', 'r') as ymlfile:
        config = yaml.load(ymlfile, Loader=yaml.FullLoader)
    level = logging.getLevelName(config.get('app').get('logger'))
    logger.setLevel(level)

    scraper = Scraper(config)
    scraper.run()
    logger.info('Done')