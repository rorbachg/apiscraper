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


def tictoc(func):
    """This is my custom decorator snippet for measuring time.
    """

    def counter(*args, **kwargs):
        start = datetime.now()
        func(*args, **kwargs)
        end = datetime.now()
        print(F'Elapsed time doing "{func.__name__}": {end-start}')
    return counter     


class Scraper():
    """Main program class.
    """

    def __init__(self, config):
        self._endpoints = [config.get('api').get('url') + endpoint for endpoint in config.get('api').get('endpoints')]
        self._max_threads = config.get('app').get('threads')
        level = logging.getLevelName(config.get('app').get('logger'))
        logger.setLevel(level)
        Path("data/").mkdir(parents=True, exist_ok=True)
        Path("data/photos/").mkdir(parents=True, exist_ok=True)

    @property
    def endpoints(self):
        return self._endpoints

    @property
    def max_threads(self):
        return self._max_threads
        
    @tictoc
    def run(self):
        """Method that runs scraping each of endpoints and then downloading all photos.
        """

        for endpoint in self.endpoints:
            self.scrape_endpoint(endpoint)

        self.download_photos('data/photos.csv')

    @staticmethod
    def scrape_endpoint(endpoint):
        """Method for scraping endpoint.

        This method uses request library to send GET request to API. If response code is 200,
        it converts JSON format to pandas DataFrame and saves it as .csv file.
        """

        logger.info(F'Scraping endpoint {endpoint}')
        response = requests.get(endpoint)
        if response.ok:
            dataframe = pd.json_normalize(response.json())
            filename = 'data/' + re.findall('https://.*?\/([a-z]+).*$', endpoint)[0] + '.csv'
            logger.info(F'Filename is {filename}')
            dataframe.to_csv(filename, index=False)
        else:
            raise Exception('Failed connection to API')

    def download_photos(self, file):
        """Method for downloading photos.
        
        This method reads previously created .csv file containing photo urls and parses its names.
        Then it creates ThreadPoolExecutor object and runs download_photo() method on as meny workers as declared in config.
        After successful finish, csv file is appended with column containing local path to images.
        """
        dataframe = pd.read_csv(file)
        filenames = dataframe['url'].apply(lambda x: 'data/photos/' + re.findall('.*/([0-9a-z]+)', x)[0] + '.jpg').to_list()
        urls = dataframe['url'].to_list()
        urltuples = zip(urls, filenames)
        try:
            logger.info(F'Running on {self.max_threads} threads')
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                executor.map(self.download_photo, urltuples)
        finally:
            dataframe['file_path'] = filenames
            dataframe.to_csv(file, index=False)

    @staticmethod
    def download_photo(urltuple):
        """Method for downloading single photo.

        This method uses requests library in order oto download photo.
        It call API, and if response code is 200 it saves it.

        """
        url = urltuple[0]
        filename = urltuple[1]
        logger.info(F'Loading {filename}')
        response = requests.get(url)
        if response.ok:
            logger.info(F'Saving {filename}')
            with open(filename, 'wb+') as f:
                f.write(response.content)
        return None

   

if __name__ == '__main__':
    with open('config.yml', 'r') as ymlfile:
        config = yaml.load(ymlfile, Loader=yaml.FullLoader)
    scraper = Scraper(config)
    scraper.run()
    logger.info('Done')