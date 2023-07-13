"""

TO BE DEPRECATED

"""


import docrawl.docrawl_core as docrawl_core
from scrapy.crawler import CrawlerRunner
from crochet import setup
import keepvariable.keepvariable_core as kv
import time
from typing import Optional
from docrawl.docrawl_logger import docrawl_logger


def load_website(url):
    if "http" not in url:
        url = "http://" + url

    spider_requests = {"url": url, "loaded": False}

    spider_requests = kv.VarSafe(spider_requests, "request", "request")
    docrawl_logger.info(f"SPIDER_REQUESTS: {spider_requests}")
    kv.save_variables(kv.kept_variables, "browser_meta_data.kpv")

    # docrawl_core.spider_requests={"url":url,"loaded":False}


def take_screenshot():
    """
    Launches take_screenshot from core.
    """

    # inp = {
    #     'filename': filename
    # }

    inp = None

    run_function('take_screenshot', inp)


def take_png_screenshot(filename):
    """
    Launches take_screenshot from core.
        :param filename: string, output filename (where to save the screenshot)
    """

    inp = {
        'filename': str(filename)       # Cast to str, e.g. when Path object is passed
    }
    run_function('take_png_screenshot', inp)


def extract_page_source(filename):
    """
    Launches extract_page_source from core.
        :param filename: string, name of file that will be used for storing page source
    """

    inp = {
        'filename': filename
    }

    run_function('extract_page_source', inp)


def scan_web_page(incl_tables=False, incl_bullets=False, incl_texts=False, incl_headlines=False, incl_links=False,
                  incl_images=False, incl_buttons=False, by_xpath=None, context_xpath=None, cookies_xpath=None,
                  output_folder='output/scraped_data'):
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

    run_function('scan_web_page', inp)


def wait_until_element_is_located(xpath):
    """
    Launches wait_until_element_is_located function from core.
        :param xpath: str, xpath of element to be located
    """

    inp = {
        'xpath': xpath
    }

    run_function('wait_until_element_is_located', inp)


def get_current_url(filename):
    """
    Launches get_current_url function from core.
        :param filename: string, name of file that will be used for storing the URL
    """

    inp = {
        'filename': filename
    }

    run_function('get_current_url', inp)


def close_browser():
    """
    Launches close_browser function from core.
    """

    inp = None

    run_function('close_browser', inp)


def scroll_web_page(scroll_to, scroll_by, scroll_max):
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

    run_function('scroll_web_page', inp)


def download_images(image_xpath, filename):
    """
    Launches download_image function from core.
        :param image_xpath: string, url of image
        :param filename: string, output filename
    """

    inp = {
        'image_xpath': image_xpath,
        'filename': filename,
    }

    run_function('download_images', inp)


def extract_xpath(xpath, filename, write_in_file_mode="w+"):
    inp = {
        'xpath': xpath,
        'filename': filename,
        'write_in_file_mode': write_in_file_mode
    }

    run_function('extract_xpath', inp)


def extract_multiple_xpath(xpaths, filename="extracted_data.xlsx"):
    inp = {
        'xpaths': xpaths,
        'filename': filename
    }

    run_function('extract_multiple_xpaths', inp)


def extract_table_xpath(xpath_row, xpath_col, first_row_header, filename="extracted_data.xlsx"):
    inp = {
        'xpath_row': xpath_row,
        'xpath_col': xpath_col,
        'first_row_header': first_row_header,
        'filename': filename
    }

    run_function('extract_table_xpath', inp)


def click_xpath(xpath):
    inp = {
        'xpath': xpath
    }

    run_function('click_xpath', inp)


def click_name(text):
    inp = {
        'text': text
    }

    run_function('click_name', inp)


def run_function(function, function_input):
    docrawl_logger.info(f'Running function {function}')
    docrawl_logger.info(f'Function input: {function_input}')

    spider_functions = {"name": function, "input": function_input, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "function", "function")
    kv.save_variables(kv.kept_variables, "browser_meta_data.kpv")



def run_spider(number, in_browser=True, driver='Firefox', kv_redis=None, kv_redis_keys=Optional[dict]):
    """
    Starts crawler.
        :param in_browser: bool, show browser GUI (-headless option).
        :param driver: string, driver instance to use (Firefox/Geckodriver, Chrome)
        :param kv_redis: instance of KeepVariableRedis, used to store scraped data
        :param kv_redis_keys: dictionary with redis keys (naming)

        Note: param in_browser is reverse to -headless option, meaning:
            - in_browser=True -> no -headless in driver options
            - in_browser=False -> add -headless to driver options
    """

    headless = not in_browser

    setup()
    crawler = CrawlerRunner()
    browser = {'headless': headless, 'driver': driver}
    browser = kv.VarSafe(browser, "browser", "browser")

    kv.save_variables(kv.kept_variables, "browser_meta_data.kpv")

    time.sleep(1)

    crawler.crawl(docrawl_core.DocrawlSpider, kv_redis=kv_redis, kv_redis_keys=kv_redis_keys)