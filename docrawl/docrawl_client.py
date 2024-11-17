import itertools
import time
from contextlib import suppress
from dataclasses import dataclass

import psutil
from crochet import setup
from scrapy.crawler import CrawlerRunner

from docrawl.docrawl_core import DocrawlSpider
from docrawl.docrawl_logger import docrawl_logger
from docrawl.errors import PageDidNotLoadError, SpiderFunctionError
from keepvariable.keepvariable_core import KeepVariableDummyRedisServer


@dataclass
class DocrawlSettings:
    pid: int
    driver: str
    headless: bool
    last_function: str


class DocrawlClient:
    id_iter = itertools.count()

    def __init__(self, kv_redis=None, kv_redis_keys=None, number_of_spawn_browsers=0, redis_key_prefix=""):
        """Number of spawn browsers = how many browser processes are ready in standby mode to not initialize + close the browser, currently support 0 and 1."""
        self._client_id = redis_key_prefix.split(':')[1] or next(self.id_iter)

        self.kv_redis = kv_redis or KeepVariableDummyRedisServer()
        self.kv_redis_keys = kv_redis_keys or {}
        self.redis_key_prefix = redis_key_prefix

        self._kv_redis_key_browser_metadata = self.kv_redis_keys.get('browser_meta_data', f'{self.redis_key_prefix}:browser_meta_data')
        self._kv_redis_key_scanned_elements = self.kv_redis_keys.get('elements', f'{self.redis_key_prefix}:elements')
        self._kv_redis_key_screenshot = self.kv_redis_keys.get('screenshot', f'{self.redis_key_prefix}:screenshot')
        
        self.browser_headers = None
        self.browser_cookies = None
        self.browser_requests = None

        docrawl_logger.info(f'Initialised DocrawlClient with ID {self._client_id}')

        if number_of_spawn_browsers > 0: #TODO: increase number of spawned browsers, support just 1 standby browser at this moment
            self.run_spider()

    def set_browser_meta_data(self, browser_meta_data: dict):
        self.kv_redis.set(key=self._kv_redis_key_browser_metadata, value=browser_meta_data)

    def get_browser_meta_data(self):
        return self.kv_redis.get(key=self._kv_redis_key_browser_metadata)
    
    def set_browser_headers(self, headers: dict):
        self.browser_headers = headers
        # self.kv_redis.set(key=self._kv_redis_key_browser_headers, value=headers)
    
    def get_browser_headers(self):
        return self.browser_headers
    
    def set_browser_cookies(self, cookies: dict):
        self.browser_cookies = cookies
    
    def get_browser_cookies(self):
        return self.browser_cookies
    
    def set_browser_requests(self, requests: list):
        self.browser_requests = requests
    
    def get_browser_requests(self):
        return self.browser_requests

    def set_browser_scanned_elements(self, elements: list):
        self.kv_redis.set(key=self._kv_redis_key_scanned_elements, value=elements)

    def get_browser_scanned_elements(self):
        return self.kv_redis.get(key=self._kv_redis_key_scanned_elements)

    def set_browser_screenshot(self, screenshot: str):
        self.kv_redis.set(key=self._kv_redis_key_screenshot, value=screenshot)

    def get_browser_screenshot(self) -> str:
        return self.kv_redis.get(key=self._kv_redis_key_screenshot)

    def is_browser_active(self):
        # NOTE: Not used anywhere, it only checks if the process exists in OS process list, not if
        # it's active
        # TODO: finish later
        pid = self.get_browser_meta_data()['browser']['pid']
        if pid is None:
            return False
        else:
            return psutil.pid_exists(pid)

    def _initialize_browser_metadata(self, driver, headless, proxy=None):
        browser_meta_data = {
            "browser": {"driver": driver, "headless": headless, "proxy": proxy, "pid": None},
            "function": {"name": "init_function", "input": None, "done": False, "error": None},
            "request": {"url": None, "loaded": False}
        }

        self.set_browser_meta_data(browser_meta_data)

    def _wait_until_page_is_loaded(self, timeout=60):
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
            docrawl_logger.warning(f'Page loaded: {self.get_browser_meta_data()["request"]["url"]}')
        else:
            docrawl_logger.error('Page was not loaded')
            raise PageDidNotLoadError()

    def _wait_until_function_is_done(self, timeout=60):
        # Load spider_requests and spider_functions
        try:
            spider_function = self.get_browser_meta_data()['function']
            is_function_done = spider_function['done']
        except Exception as e:
            docrawl_logger.error(f'Error while loading is_function_done: {e}')
            is_function_done = True

        # Then check if function is done
        timeout_start = time.time()
        while not is_function_done and time.time() < timeout_start + timeout:
            spider_function = self.get_browser_meta_data()['function']
            is_function_done = spider_function['done']
            time.sleep(0.5)
            docrawl_logger.info('Function is still running, waiting 0.5 sec ...')

        if is_function_done:
            if spider_function["error"] is None:
                docrawl_logger.success('Spider function finished successfully')
            else:
                docrawl_logger.error(f'Spider function failed: {spider_function["error"]}')
                raise SpiderFunctionError(spider_function['error'])

        else:
            docrawl_logger.error('Function was not finished')
            raise TimeoutError('Spider function timed out')

    def _execute_function(self, function, function_input=None, timeout=30):
        # docrawl_logger.info(f'Running function {function} with input: {function_input}')

        if True:#self.is_browser_active(): #The browser seemed inactive but it was actually active
            browser_meta_data = self.get_browser_meta_data()
            function = {"name": function, "input": function_input, "done": False, "error": None}
            browser_meta_data['function'] = function
            self.set_browser_meta_data(browser_meta_data)

            # self._wait_until_page_is_loaded()
            self._wait_until_function_is_done(timeout)
        else:
            docrawl_logger.warning('Browser instance is not active / crashed, function '+str(function)+' could not be executed')

    def acquire_browser(self, driver, in_browser=False, proxy=None):
        # Need to update browser metadata mainly for proxy
        self._initialize_browser_metadata(driver=driver, headless=not in_browser, proxy=proxy)

        self.active_browser = "browser1"

        docrawl_logger.warning(f"Acquired browser {self.active_browser}")

    def release_browser(self):
        self.active_browser = None


    def run_spider(self, driver='Firefox', in_browser: bool = False, proxy: dict = None):
        self._initialize_browser_metadata(driver=driver, headless=not in_browser, proxy=proxy)

        setup()
        crawler = CrawlerRunner()
        crawler.crawl(DocrawlSpider, docrawl_client=self)

    def restart_browser(self, driver='Firefox', in_browser=False, proxy=None, as_new=False):
        """
        Terminate any active browser (if exists/crashed) and open a new one.

        param as_new: bool, if True, the browser will be opened with a new `browser_metadata`
        """
        if as_new:
            self._initialize_browser_metadata(driver=driver, headless=not in_browser, proxy=proxy)
        self._execute_function('restart_browser', None, timeout=120)

    def load_website(self, url, timeout=120):
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
            :param filename: string, output filename (where to save the screenshot).
        """
        inp = {
            'filename': str(filename)  # Cast to str, e.g. when Path object is passed
        }

        self._execute_function('take_png_screenshot', inp, timeout)

    def extract_page_source(self, filename, timeout=20):
        """
        Launches extract_page_source from core.
            :param filename: string, name of file that will be used for storing page source.
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
            :param output_folder: str, path to output folder.
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
            :param xpath: str, xpath of element to be located.
        """
        inp = {
            'xpath': xpath
        }

        self._execute_function('wait_until_element_is_located', inp, timeout)

    def get_current_url(self, filename, timeout=20):
        """
        Launches get_current_url function from core.
            :param filename: string, name of file that will be used for storing the URL.
        """
        inp = {
            'filename': filename
        }

        self._execute_function('get_current_url', inp, timeout)

    def close_browser(self, timeout=10):
        """Launch close_browser function from core."""
        self._execute_function('close_browser', None, timeout)
        browser_metadata = self.get_browser_meta_data()
        browser_metadata['browser']['proxy'] = None
        self.set_browser_meta_data(browser_metadata)

        # pid = self.get_browser_meta_data()['browser']['pid']

        # with suppress(Exception):
        #     psutil.Process(pid).terminate()

        # docrawl_logger.warning(f'Is browser closed: {self.is_browser_active()}')

    def scroll_web_page(self, scroll_to, scroll_by, scroll_max, timeout=20):
        """
        Launches scroll_web_page function from core.
            :param scroll_to: string, scroll direction (Up/Down)
            :param scroll_by: int, scroll distance
            :param scroll_max: bool, scroll to maximum.
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
            :param filename: string, output filename.
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

    def refresh_page_source(self, timeout=30):
        self._execute_function('refresh_page_source', None, timeout)

    def send_text(self, xpath, text, timeout=20):
        inp = {
            'xpath': xpath,
            'text': text
        }
        self._execute_function('send_text', inp, timeout)

    def __exit__(self):
        self.close_browser()
