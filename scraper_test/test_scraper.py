import nose2
import unittest
import pandas as pd
from main import Scraper
from unittest.mock import patch
import os

def mock_requests_get(*args, **kwargs):
    """Mock method that will be used to mock API.

    This method creates MockResponse class that mimics APIs response.
    There are two cases implemented for testing:
    1. Returning sample of 'users' endpoint data
    2. Returning previosuly manually downloaded image.

    """
    class MockResponse:
        def __init__(self, json_data, content, status_code):
            self.json_data = json_data
            self.content = content
            self.status_code = status_code
            self.ok = status_code == 200

        def json(self):
            return self.json_data
    if args[0] == 'https://jsonplaceholder.typicode.com/test':
        return MockResponse([
                                {
                                    "id": 1,
                                    "name": "Leanne Graham",
                                    "username": "Bret",
                                    "email": "Sincere@april.biz",
                                    "address": {
                                    "street": "Kulas Light",
                                    "suite": "Apt. 556",
                                    "city": "Gwenborough",
                                    "zipcode": "92998-3874",
                                    "geo": {
                                        "lat": "-37.3159",
                                        "lng": "81.1496"
                                    }
                                    },
                                    "phone": "1-770-736-8031 x56442",
                                    "website": "hildegard.org",
                                    "company": {
                                    "name": "Romaguera-Crona",
                                    "catchPhrase": "Multi-layered client-server neural-net",
                                    "bs": "harness real-time e-markets"
                                    }
                                }, 
                            ],
                            None,
                            200)
    elif  'https://via.placeholder.com/photos/' in args[0]:
        print('Mocking photo...')
        img = open('scraper_test/testphoto.jpg', 'rb').read()
        return MockResponse(None,
                            img,
                            200)
    return MockResponse(None, None, 404)
    

class UnitTests(unittest.TestCase):
    """UnitTest class

    """
    @patch('main.requests.get', side_effect=mock_requests_get)
    def test_scrape_endpoint(self, mock_get):
        """ scrape_ednpoints() test
        This creates scraper object with some test endpoints that will be mocked and will return sample 
        of users endpoint.

        """
        config = {'app': {'threads': 1, 'logger': 'DEBUG'}, 
            'api': {'url': 'https://jsonplaceholder.typicode.com/',
                    'endpoints': ['test', ]}
            }
        scraper = Scraper(config)
        scraper.scrape_endpoint(scraper.endpoints[0])
        df = pd.read_csv('data/test.csv')
        gold_df = pd.DataFrame({
                                "id": 1,
                                "name": "Leanne Graham",
                                "username": "Bret",
                                "email": "Sincere@april.biz",
                                "phone": "1-770-736-8031 x56442",
                                "website": "hildegard.org",
                                "address.street": "Kulas Light",
                                "address.suite": "Apt. 556",
                                "address.city": "Gwenborough",
                                "address.zipcode": "92998-3874",
                                "address.geo.lat": -37.3159,
                                "address.geo.lng": 81.1496,
                                "company.name": "Romaguera-Crona",
                                "company.catchPhrase": "Multi-layered client-server neural-net",
                                "company.bs": "harness real-time e-markets"                                    
                                }, index=[0])
        
        pd.testing.assert_frame_equal(df,
                                      gold_df)
        os.remove('data/test.csv')

    @patch('main.requests.get', side_effect=mock_requests_get)
    def test_download_photo(self, mock_get):
        """download_photo() test

        This test creates scraper object with test endpoint and its given test url to download image from.
        API is mocked and 'requests.get()' method returns previously downloaded image.

        """
        config = {'app': {'threads': 1, 'logger': 'DEBUG'}, 
            'api': {'url': 'https://jsonplaceholder.typicode.com/',
                    'endpoints': ['test', ]}
            }
        scraper = Scraper(config)
        test_urltuple = ('https://via.placeholder.com/photos/testphot', 'testphoto.jpg')
        scraper.download_photo(test_urltuple)
        gold_image = open('scraper_test/testphoto.jpg', 'rb').read()
        image = open('testphoto.jpg', 'rb').read()
        self.assertEqual(image, gold_image)
        os.remove('testphoto.jpg')

    @patch('main.requests.get', side_effect=mock_requests_get)
    def test_download_photos(self, mock_get):
        """download_photos() test

        This test creates test photos.csv which containes different urls to photos.
        API is mocked and it returns everytime the same image that was previously downloaded.

        """
        config = {'app': {'threads': 1, 'logger': 'DEBUG'}, 
            'api': {'url': 'https://jsonplaceholder.typicode.com/',
                    'endpoints': ['test', ]}
            }
        scraper = Scraper(config)
        urls = ['https://via.placeholder.com/photos/testphoto'] * 5
        urls = [y+str(x) for x, y in enumerate(urls)]
        testphotos = pd.DataFrame( urls, columns=['url'])
        testphotos.to_csv('testphotos.csv', index=False)
        scraper.download_photos('testphotos.csv')

        testphotos = pd.read_csv('testphotos.csv')
        gold_image = open('scraper_test/testphoto.jpg', 'rb').read()
        for file_path in testphotos['file_path']:
            image = open(file_path, 'rb').read()
            self.assertEqual(image, gold_image)
            os.remove(file_path)    
        os.remove('testphotos.csv')



if __name__ == "__main__":
    unittest.main()