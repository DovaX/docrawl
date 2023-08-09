import psutil
import itertools
import time

from scrapy.crawler import CrawlerRunner
from dataclasses import dataclass
from contextlib import suppress
from crochet import setup

from docrawl.docrawl_logger import docrawl_logger
from docrawl.docrawl_core import DocrawlSpider

from keepvariable.keepvariable_core import KeepVariableDummyRedisServer


@dataclass
class DocrawlSettings:
    pid: int
    driver: str
    headless: bool
    last_function: str


class DocrawlClient:
    id_iter = itertools.count()

    def __init__(self, kv_redis=KeepVariableDummyRedisServer(), kv_redis_keys=None):
        self._client_id = next(self.id_iter)

        self.kv_redis = kv_redis
        self.kv_redis_keys = kv_redis_keys or dict()

        self._kv_redis_key_browser_metadata = self.kv_redis_keys.get('browser_meta_data', 'browser_meta_data')
        self._kv_redis_key_scanned_elements = self.kv_redis_keys.get('elements', 'elements')
        self._kv_redis_key_screenshot = self.kv_redis_keys.get('screenshot', 'screenshot')

        docrawl_logger.info(f'Initialised DocrawlClient with ID {self._client_id}')

    def set_browser_meta_data(self, browser_meta_data: dict):
        self.kv_redis.set(key=self._kv_redis_key_browser_metadata, value=browser_meta_data)

    def get_browser_meta_data(self):
        return self.kv_redis.get(key=self._kv_redis_key_browser_metadata)

    def set_browser_scanned_elements(self, elements: list):
        self.kv_redis.set(key=self._kv_redis_key_scanned_elements, value=elements)

    def get_browser_scanned_elements(self):
        self.kv_redis.get(key=self._kv_redis_key_scanned_elements)

    def set_browser_screenshot(self, screenshot: str):
        self.kv_redis.set(key=self._kv_redis_key_screenshot, value=screenshot)

    def get_browser_screenshot(self):
        self.kv_redis.get(key=self._kv_redis_key_screenshot)

    def is_browser_active(self):
        # TODO: finish later
        pid = self.get_browser_meta_data()['browser']['pid']

        return psutil.pid_exists(pid)

    def _initialize_browser_metadata(self, driver, headless, proxy=None):
        browser_meta_data = {
            "browser": {"driver": driver, "headless": headless, "proxy": proxy},
            "function": {"name": "init_function", "input": None, "done": False}
        }

        self.set_browser_meta_data(browser_meta_data)

    def _wait_until_page_is_loaded(self, timeout=20):
        # Load spider_requests and spider_functions
        try:
            is_page_loaded = self.get_browser_meta_data()['request']['loaded']
        except Exception as e:
            docrawl_logger.error(f'Error while loading is_page_loaded: {e}')
            is_page_loaded = True

        # First check if page is loaded
        timeout_start = time.time()
        while not is_page_loaded and time.time() < timeout_start + timeout:
            try:
                is_page_loaded = self.get_browser_meta_data()['request']['loaded']
            except:
                is_page_loaded = False
            time.sleep(0.5)
            docrawl_logger.info('Page is still loading, waiting 0.5 sec ...')

        if is_page_loaded:
            docrawl_logger.success('Page loaded')
        else:
            docrawl_logger.error('Page was not loaded')

    def _wait_until_function_is_done(self, timeout):
        # Load spider_requests and spider_functions
        try:
            is_function_done = self.get_browser_meta_data()['function']['done']
        except Exception as e:
            docrawl_logger.error(f'Error while loading is_function_done: {e}')
            is_function_done = True

        # Then check if function is done
        timeout_start = time.time()
        while not is_function_done and time.time() < timeout_start + timeout:
            try:
                is_function_done = self.get_browser_meta_data()['function']['done']
            except:
                is_function_done = False
            time.sleep(0.5)
            docrawl_logger.info('Function is still running, waiting 0.5 sec ...')

        if is_function_done:
            docrawl_logger.success('Function finished')
        else:
            docrawl_logger.error('Function was not finished')

    def _execute_function(self, function, function_input=None, timeout=30):
        docrawl_logger.info(f'Running function {function} with input: {function_input}')

        if self.is_browser_active():
            browser_meta_data = self.get_browser_meta_data()
            function = {"name": function, "input": function_input, "done": False}
            browser_meta_data['function'] = function
            self.set_browser_meta_data(browser_meta_data)

            # self._wait_until_page_is_loaded()
            self._wait_until_function_is_done(timeout)
        else:
            docrawl_logger.warning('Browser instance is not active')

    def run_spider(self, driver='Firefox', in_browser: bool = False, proxy: dict = None):
        self._initialize_browser_metadata(driver=driver, headless=not in_browser, proxy=proxy)

        setup()
        crawler = CrawlerRunner()
        crawler.crawl(DocrawlSpider, docrawl_client=self)

    def load_website(self, url, timeout=20):
        if "http" not in url:
            url = "http://" + url

        request = {"url": url, "loaded": False}
        browser_meta_data = self.get_browser_meta_data()
        browser_meta_data['request'] = request
        self.set_browser_meta_data(browser_meta_data)

        self._wait_until_page_is_loaded(timeout)

    def take_screenshot(self, timeout=20):
        self._execute_function('take_screenshot', None, timeout)

    def take_png_screenshot(self, filename, timeout=20):
        """
        Launches take_screenshot from core.
            :param filename: string, output filename (where to save the screenshot)
        """

        inp = {
            'filename': str(filename)  # Cast to str, e.g. when Path object is passed
        }

        self._execute_function('take_png_screenshot', inp, timeout)

    def extract_page_source(self, filename, timeout=20):
        """
        Launches extract_page_source from core.
            :param filename: string, name of file that will be used for storing page source
        """

        inp = {
            'filename': filename
        }

        self._execute_function('extract_page_source', inp, timeout)

    def scan_web_page(self, incl_tables=False, incl_bullets=False, incl_texts=False, incl_headlines=False,
                      incl_links=False,
                      incl_images=False, incl_buttons=False, by_xpath=None, context_xpath=None, cookies_xpath=None,
                      output_folder='output/scraped_data', timeout=30):
        """
        Launches find_tables function from core.
            :param incl_tables: boolean, search for tables
            :param incl_bullets: boolean, search for bullet lists
            :param incl_texts: boolean, search for text elements
            :param incl_headlines: boolean, search for headlines
            :param incl_links: boolean, search for links
            :param incl_images: boolean, search for images
            :param incl_buttons: boolean, search for buttons
            :param by_xpath: str, search elements by custom XPath
            :param output_folder: str, path to output folder
        """

        inp = {
            'incl_tables': incl_tables,
            'incl_bullets': incl_bullets,
            'incl_texts': incl_texts,
            'incl_headlines': incl_headlines,
            'incl_links': incl_links,
            'incl_images': incl_images,
            'incl_buttons': incl_buttons,
            'by_xpath': by_xpath,
            'context_xpath': context_xpath,
            'cookies_xpath': cookies_xpath,
            'output_folder': output_folder,
        }

        self._execute_function('scan_web_page', inp, timeout)

    def wait_until_element_is_located(self, xpath, timeout=20):
        """
        Launches wait_until_element_is_located function from core.
            :param xpath: str, xpath of element to be located
        """

        inp = {
            'xpath': xpath
        }

        self._execute_function('wait_until_element_is_located', inp, timeout)

    def get_current_url(self, filename, timeout=20):
        """
        Launches get_current_url function from core.
            :param filename: string, name of file that will be used for storing the URL
        """

        inp = {
            'filename': filename
        }

        self._execute_function('get_current_url', inp, timeout)

    def close_browser(self, timeout=10):
        """
        Launches close_browser function from core.
        """

        self._execute_function('close_browser', None, timeout)

        pid = self.get_browser_meta_data()['browser']['pid']

        with suppress(Exception):
            psutil.Process(pid).terminate()

        docrawl_logger.warning(f'Is browser closed: {self.is_browser_active()}')

    def scroll_web_page(self, scroll_to, scroll_by, scroll_max, timeout=20):
        """
        Launches scroll_web_page function from core.
            :param scroll_to: string, scroll direction (Up/Down)
            :param scroll_by: int, scroll distance
            :param scroll_max: bool, scroll to maximum
        """

        inp = {
            'scroll_to': scroll_to,
            'scroll_by': scroll_by,
            'scroll_max': scroll_max
        }

        self._execute_function('scroll_web_page', inp, timeout)

    def download_images(self, image_xpath, filename, timeout=20):
        """
        Launches download_image function from core.
            :param image_xpath: string, url of image
            :param filename: string, output filename
        """

        inp = {
            'image_xpath': image_xpath,
            'filename': filename,
        }

        self._execute_function('download_images', inp, timeout)

    def extract_xpath(self, xpath, filename, write_in_file_mode="w+", timeout=20):
        inp = {
            'xpath': xpath,
            'filename': filename,
            'write_in_file_mode': write_in_file_mode
        }

        self._execute_function('extract_xpath', inp, timeout)

    def extract_multiple_xpath(self, xpaths, filename="extracted_data.xlsx", timeout=20):
        inp = {
            'xpaths': xpaths,
            'filename': filename
        }

        self._execute_function('extract_multiple_xpaths', inp, timeout)

    def extract_table_xpath(self, xpath_row, xpath_col, first_row_header, filename="extracted_data.xlsx", timeout=20):
        inp = {
            'xpath_row': xpath_row,
            'xpath_col': xpath_col,
            'first_row_header': first_row_header,
            'filename': filename
        }

        self._execute_function('extract_table_xpath', inp, timeout)

    def click_xpath(self, xpath, timeout=20):
        inp = {
            'xpath': xpath
        }

        self._execute_function('click_xpath', inp, timeout)

    def click_name(self, text, timeout=20):
        inp = {
            'text': text
        }

        self._execute_function('click_name', inp, timeout)

    def __exit__(self):
        self.close_browser()
