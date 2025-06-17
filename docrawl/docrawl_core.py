import datetime
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Type

import lxml.html
import pandas as pd
import psutil
import requests
import scrapy
from scrapy.selector import Selector
from selenium.common.exceptions import (
    NoSuchElementException,
    WebDriverException,
)
from selenium.webdriver import ChromeOptions, FirefoxOptions, ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.exceptions import MaxRetryError
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager

from docrawl.elements import classify_element_by_xpath, TableElement, \
    InputElement, BulletListElement, ImageElement, LinkElement, ButtonElement, HeadlineElement, TextElement, \
    ContextElement, CookiesElement, AbstractElement
from docrawl.errors import SpiderFunctionError
from docrawl.utils import build_abs_url, get_logger

# Due to the problems with selenium wire on linux systems
try:
    from seleniumwire import webdriver
except:
    print('Error while importing selenium-wire, using selenium instead')
    print('TRY: pip install blinker==1.7.0')
    from selenium import webdriver

import threading


class ScreenshotThread(threading.Thread):
    def __init__(self, docrawl_spider, screenshot_filename, interval=0.5):
        threading.Thread.__init__(self)
        self.docrawl_spider = docrawl_spider
        self.screenshot_filename=screenshot_filename
        self.interval = interval
        self.stop_event = threading.Event()
        self.number=0

    def run(self):
        while not self.stop_event.is_set():
            self.take_screenshot()
            time.sleep(self.interval)

    def take_screenshot(self):
        self.number+=1
        #screenshot_name = f"screenshot_{int(time.time())}.png"
        #self.browser.save_screenshot(screenshot_name)
        inp = {
            'filename': str(self.screenshot_filename)  # Cast to str, e.g. when Path object is passed
        }
        self.docrawl_spider._take_png_screenshot(inp)

        #screenshot = self.docrawl_spider.browser.get_full_page_screenshot_as_file(screenshot_name)
        self.docrawl_spider.logger.info(f"Screenshot thread: Screenshot taken - {self.screenshot_filename}")

    def stop(self):
        self.stop_event.set()


class DocrawlSpider(scrapy.spiders.CrawlSpider):
    name = "forloop"

    custom_settings = {
        'LOG_LEVEL': 'ERROR',
        'USER_AGENT': "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
        'DEFAULT_REQUEST_HEADERS': {
            'Referer': 'https://forloop.ai'
        }
        #   'CONCURRENT_REQUESTS' : '20',
    }

    def __init__(self, *a, **kw):
        self.docrawl_client = kw['docrawl_client']

        self.kv_redis_key_screenshot = self.docrawl_client.kv_redis_keys.get(
            'screenshot', 'screenshot'
        )
        self.kv_redis_key_elements = self.docrawl_client.kv_redis_keys.get('elements', 'elements')

        # "logger" property already exists in parent class, so using "_logger"
        self._logger = get_logger(f'DocrawlCore#{self.docrawl_client._client_id[:5]}')

        self.browser = self._initialise_browser()
        
        self.screenshot_thread = None  # needs to be initialized to None before execution
        self.start_requests()

    def _initialise_browser(self):
        browser_meta_data = self.docrawl_client.get_browser_meta_data()
        self.driver_type = browser_meta_data['browser']['driver']
        self.headless = browser_meta_data['browser']['headless']
        proxy_info = browser_meta_data['browser']['proxy']

        if self.driver_type == 'Firefox':
            self.options = FirefoxOptions()
            self.options.set_preference("marionette", True)

            sw_options = self._set_proxy(proxy_info)

            if self.headless:
                self.options.add_argument("--headless")
                # For headless mode different width of window is needed
                window_size_x = 1450

            try:
                service = Service(GeckoDriverManager().install())
            except Exception as e:
                service = None
                self._logger.warning(
                    "GeckoDriverManager update was not successful - launching latest Firefox version instead"
                    + str(e)
                )

            try:
                self.browser = webdriver.Firefox(
                    options=self.options, service=service, seleniumwire_options=sw_options
                )
            except Exception as e:
                self._logger.error(f'Error while creating Firefox instance {e}')
                self.browser = webdriver.Firefox(options=self.options)

        elif self.driver_type == 'Chrome':
            self.options = ChromeOptions()

            sw_options = self._set_proxy(proxy_info)

            if self.headless:
                self.options.add_argument("--headless")

                # For headless mode different width of window is needed
                window_size_x = 1450

            try:
                self.browser = webdriver.Chrome(
                    options=self.options, service=Service(ChromeDriverManager().install()),
                    seleniumwire_options=sw_options
                )
            except Exception as e:
                self._logger.error(f'Error while creating Chrome instance {e}')
                self.browser = webdriver.Chrome(options=self.options)

        window_size_x = 1820
        self.browser.set_window_size(window_size_x, 980)

        browser_meta_data['browser']['pid'] = self._determine_browser_pid()
        if browser_meta_data.get('request'):
            browser_meta_data['request']['loaded'] = False
        self.docrawl_client.set_browser_meta_data(browser_meta_data)
        self._logger.info(f'Browser settings: {browser_meta_data}')

        return self.browser

    def _close_browser(self, inp):
        """
        Close browser (remove driver instance).

        :param browser: driver instance
        """
        try:
            self.browser.quit()
        except ConnectionRefusedError as e:
            self._logger.error(f'Error while closing the browser: {e}')
        except Exception as e:
            self._logger.error(f'Error while closing the browser: {e}')

        # # Remove proxy after closing browser instance
        # proxy = {'ip': '', 'port': '', 'username': '', 'password': ''}
        # browser_meta_data = self.docrawl_client.get_browser_meta_data()
        # browser_meta_data['browser']['proxy'] = proxy
        # # browser_meta_data['request']['loaded'] = False
        # self.docrawl_client.set_browser_meta_data(browser_meta_data)

    def _restart_browser(self, inp=None):
        """Terminate any active browser (if exists/crashed) and open a new one, while retaining `browser_metadata`."""
        if self.is_browser_active():
            self._close_browser(inp)
        else:
            self._logger.error("Browser crashed")

        self.browser = self._initialise_browser()
        self._logger.warning("Browser restarted")

    def __del__(self):
        self.browser.quit()

    def is_browser_active(self):
        try:
            pid = self.docrawl_client.get_browser_meta_data()['browser']['pid']
            proc = psutil.Process(pid)
            is_process_active = proc.status() not in [
                psutil.STATUS_ZOMBIE,
                psutil.STATUS_DEAD,
                psutil.STATUS_STOPPED,
                psutil.STATUS_TRACING_STOP,
            ]
            return is_process_active
        except (KeyError, psutil.NoSuchProcess):
            return False

    def _prepare_proxy_string(self, proxy_info: dict):
        if proxy_info is None or any([not proxy_info['ip'], not proxy_info['port']]):
            return None
        else:
            proxy_ip = proxy_info['ip']
            proxy_port = proxy_info['port']
            proxy_username = proxy_info['username']
            proxy_password = proxy_info['password']

            if proxy_username and proxy_password:
                proxy = f'http://{proxy_username}:{proxy_password}@{proxy_ip}:{proxy_port}'
            else:
                proxy = f'{proxy_ip}:{proxy_port}'

            return proxy

    def _update_proxy(self, proxy_info: dict):
        if proxy_info is None or any([not proxy_info['ip'], not proxy_info['port']]):
            return None
        else:
            proxy = self._prepare_proxy_string(proxy_info)

            self.browser.proxy = {"http": proxy, "https": proxy, "verify_ssl": False}
            self._logger.warning("Proxy updated")

    def _set_proxy(self, proxy_info: dict) -> dict:
        """
        Sets proxy before launching browser instance.
        :param proxy_info: proxy params (ip, port, username, password)
        """

        # If proxy was not set
        if proxy_info is None or any([not proxy_info['ip'], not proxy_info['port']]):
            return None
        else:
            proxy = self._prepare_proxy_string(proxy_info)

            # Proxy with authentication
            if 'http://' in proxy:
                # selenium-wire proxy settings
                sw_options = {
                    'proxy': {'http': proxy, 'https': proxy, 'no_proxy': 'localhost,127.0.0.1'}
                }

            # Proxy without authentication
            else:
                sw_options = None
                firefox_proxies = Proxy()
                firefox_proxies.ssl_proxy = proxy
                firefox_proxies.http_proxy = proxy
                firefox_proxies.proxy_type = ProxyType.MANUAL
                self.options.proxy = firefox_proxies

            return sw_options

    def _determine_browser_pid(self):
        if self.driver_type == 'Firefox':
            browser_pid = self.browser.capabilities['moz:processID']
        elif self.driver_type == 'Chrome':
            browser_pid = self.browser.service.process.pid
        return browser_pid

    def start_requests(self):
        URLS = ['https://www.forloop.ai']
        FUNCTIONS = [self.parse]
        for i in range(len(URLS)):
            yield scrapy.Request(url=URLS[i], callback=FUNCTIONS[i])  # yield

    # # # # # # # SCRAPING FUNCTIONS # # # # # # #

    def _init_function(self, inp):
        self._logger.warning("_init_function is being executed")

    def _click_class(self, inp):
        class_input = inp.get("filename")
        index = inp.get("index", 0)
        tag = inp.get("tag", "div")

        name_input = self.browser.find_elements(By.XPATH, f'//{tag}[@class="{class_input}"]')
        name_input[index].click()

        return name_input

    def _take_screenshot(self, inp):
        """
        Take screenshot of current page and save it.

        :param browser: Selenium driver, browser instance
        """
        if isinstance(self.browser, webdriver.Firefox):
            root_element = self.browser.find_element(By.XPATH, '/html')
            string = self.browser.get_full_page_screenshot_as_base64()
            self.browser.execute_script("return arguments[0].scrollIntoView(true);", root_element)

        elif isinstance(self.browser, webdriver.Chrome):
            # Get params needed for fullpage screenshot
            page_rect = self.browser.execute_cdp_cmd('Page.getLayoutMetrics', {})
            # Set the width and height of the viewport to screenshot, same as the site's content size
            screenshot_config = {
                'captureBeyondViewport': True,
                'fromSurface': True,
                'clip':
                    {
                        'width': page_rect['cssContentSize']['width'],
                        'height': page_rect['cssContentSize']['height'], 'x': 0, 'y': 0, 'scale': 1
                    },
            }
            # Dictionary with 1 key: data
            string = self.browser.execute_cdp_cmd('Page.captureScreenshot', screenshot_config)['data']
        else:
            raise NotImplementedError(f"Screenshot is not implemented for {self.browser} browser")

        self.docrawl_client.set_browser_screenshot(string)

    def _take_png_screenshot(self, inp):
        """
        Takes screenshot of current page and saves it.
            :param browser: Selenium driver, browser instance
            :param inp, list, inputs from launcher (filename)
        """

        filename = inp["filename"]
        '''
        try:
            root_element = browser.find_element(By.XPATH, '/html')
            browser.save_full_page_screenshot(filename)

            browser.execute_script("return arguments[0].scrollIntoView(true);", root_element)
        except Exception as e:
            print('Error while taking page screenshot!', e)
        '''

        browser_type = type(self.browser)

        if type(self.browser) == webdriver.Firefox:

            try:
                WebDriverWait(self.browser, 5).until(EC.presence_of_element_located((By.XPATH, '/html')))
                root_element = self.browser.find_element(By.XPATH, '/html')

                screenshot = self.browser.get_full_page_screenshot_as_file(filename)
                try: #TEMPORARY HOT FIX
                    screenshot = self.browser.get_full_page_screenshot_as_file("./tmp/screenshots/website.png")
                except Exception as e:
                    print("Docrawl: TEMPORARY TRY EXCEPT")
                try:
                    self.browser.execute_script("return arguments[0].scrollIntoView(true);", root_element)
                except Exception as e:
                    self._logger.warning("Warning: Website wasn't scrolled while taking screenshot: "+str(e))
                self._logger.info(f'Png screenshot created')
            except Exception as e:
                self._logger.error(f'Error while taking page png screenshot: {e}')

    def _extract_page_source(self, inp):
        """
        Extracts the source of currently scraped page.
            :param page: Selenium Selector, page to export source from
            :param inp: list, inputs from launcher (incl_tables, incl_bullets, output_dir)
        """

        filename = inp['filename']

        with open(filename, 'w+', encoding="utf-8") as f:
            f.write(self.browser.page_source)

    def _process_element(self, i, selenium_element, lxml_element, elem_type: Type[AbstractElement]):
        try:
            xpath = lxml_element.getroottree().getpath(lxml_element)
            # xpath = xpath.split('/')
            # xpath[2] = 'body'  # For some reason getpath() generates <div> instead of <body>
            # xpath = '/'.join(xpath)

            element_html = selenium_element.get_attribute('innerHTML')

            if not element_html:
                return None

            instance = elem_type(element_html, xpath)

            # Filter out invalid elements
            if not instance.is_sized() or instance.is_empty():
                return None

            return {
                "name": f"{elem_type.ELEMENT_TYPE}_{i}",
                "type": elem_type.ELEMENT_TYPE,
                "rect": selenium_element.rect,
                "xpath": xpath.replace('/html/div', '/html/body'),   # Due to "special" behaviour of lxml lib
                "data": asdict(instance.element_data),
            }

        except Exception as e:
            self._logger.error(f"Error processing element: {e}")
            return None

    def _process_elements_in_parallel(self, selenium_elements, lxml_elements, elem_type):
        with ThreadPoolExecutor() as executor:
            results = executor.map(
                lambda args: self._process_element(args[0], *args[1], elem_type),
                enumerate(zip(selenium_elements, lxml_elements))
            )

        return [result for result in results if result is not None]

    def _search_elements(self, tree, elem_type, custom_tags=None):
        tags = custom_tags or elem_type.PREDEFINED_TAGS

        prefix = '' if custom_tags is not None else '//'

        joined_tags = ' | '.join([f'{prefix}{tag}' for tag in tags])

        selenium_elements = self.browser.find_elements(By.XPATH, joined_tags)

        if custom_tags:
            # Due to "special" behaviour of lxml lib
            joined_tags = joined_tags.replace('/html/body', '/html/div')

        lxml_elements = tree.xpath(joined_tags)

        if len(selenium_elements) != len(lxml_elements):
            self._logger.warning(f'Number of Selenium elements ({len(selenium_elements)}) do not match number of lxml elements ({len(lxml_elements)})')

        return selenium_elements, lxml_elements

    def _scan_web_page(self, inp):
        """
        Finds different elements (tables, bullet lists) on page.
            :param inp: list, inputs from launcher (incl_tables, incl_bullets, output_dir)
        """

        self._logger.warning("Scan web page has started")

        by_xpath = inp['by_xpath']

        config = {
            TableElement: inp['incl_tables'],
            BulletListElement: inp['incl_bullets'],
            TextElement: inp['incl_texts'],
            HeadlineElement: inp['incl_headlines'],
            LinkElement: inp['incl_links'],
            ImageElement: inp['incl_images'],
            ButtonElement: inp['incl_buttons'],
            InputElement: inp.get('incl_input', True),
            CookiesElement: bool(inp['cookies_xpath']),
            ContextElement: bool(inp['context_xpath']),
        }

        # First remove old data
        self.docrawl_client.set_browser_scanned_elements(elements=[])

        time_start_f = datetime.datetime.now()

        def timedelta_format(end, start):
            delta = end - start
            sec = delta.seconds
            microsec = delta.microseconds

            return f'{sec}:{microsec}'

        self._logger.info("Find elements phase has started")

        html_content = self.browser.execute_script("return document.body.innerHTML")
        tree = lxml.html.fromstring(html_content)

        elements_all = []
        for elem_type, include in config.items():
            if include:
                if isinstance(elem_type, CookiesElement):
                    custom_tags = inp['cookies_xpath']
                elif isinstance(elem_type, ContextElement):
                    custom_tags = inp['context_xpath']
                else:
                    custom_tags = None

                selenium_elements, lxml_elements = self._search_elements(tree, elem_type, custom_tags)

                elements_all.extend(self._process_elements_in_parallel(selenium_elements, lxml_elements, elem_type))

        if by_xpath:
            # Temporary workaround due to weird behaviour of lists in kpv
            if ';' in by_xpath:
                list_of_xpaths = by_xpath.split(';')[:-1]
            else:
                list_of_xpaths = [by_xpath]

            for i, elem in enumerate(list_of_xpaths):
                # With text() at the end will not work
                xpath = elem.removesuffix('/text()').rstrip('/').replace('/html/div', '/html/body')

                custom_tags = [xpath]
                element_type = classify_element_by_xpath(xpath)

                selenium_elements, lxml_elements = self._search_elements(tree, element_type, custom_tags)

                elements_all.extend(self._process_elements_in_parallel(selenium_elements, lxml_elements, element_type))

        self.docrawl_client.set_browser_scanned_elements(elements_all)
        self._logger.info(
            f'Scan Web Page function duration {timedelta_format(datetime.datetime.now(), time_start_f)}. Found {len(elements_all)} elements')

    def _wait_until_element_is_located(self, inp):
        """
        Waits until certain element is located on page and then clicks on it.
            :param browser: Selenium driver, browser instance
            :param inp, list, inputs from launcher (xpath)

        Note: click() method may be replaced with another
        """

        xpath = inp['xpath']

        try:
            WebDriverWait(self.browser, 5).until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
        except Exception as e:
            self._logger.error(f'Error while locating element: {e}')

    def _get_current_url(self, inp):
        """
        Returns the URL of current opened website
            :param page: :param browser: driver instance
            :param inp: list, inputs from launcher (filename)
        """
        filename = inp['filename']
        url = str(self.browser.current_url)
        with open(filename, 'w+', encoding="utf-8") as f:
            f.write(url)

    def _scroll_web_page(self, inp):
        """
        Scrolls page up / down by n-pixels.
            :param browser: driver instance
            :param inp: list, inputs from launcher (scroll_to, scroll_by, scroll_max)
        """

        scroll_to = inp['scroll_to']
        scroll_by = inp['scroll_by']
        scroll_max = inp['scroll_max']

        script = ''

        if scroll_to == 'Down':
            if scroll_max:
                script = 'window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;'
            else:
                try:
                    # script = f'window.scrollBy(0, {scroll_by});'       # instant scrolling
                    script = f'window.scrollBy({{top: {scroll_by}, left: 0, behavior: "smooth"}});'  # smooth scrolling
                except:
                    pass
        elif scroll_to == 'Up':
            if scroll_max:
                script = 'window.scrollTo(0, 0)'
            else:
                try:
                    # script = f'window.scrollBy(0, -{scroll_by});'      # instant scrolling
                    script = f'window.scrollBy({{top: -{scroll_by}, left: 0, behavior: "smooth"}});'  # smooth scrolling
                except:
                    pass

        if script:
            self.browser.execute_script(script)

    def _download_images(self, inp):
        """
        Downloads images using XPath
            :param browser: driver instance
            :param inp: list, inputs from launcher (image xpath, filename)
        """

        image_xpath = inp['image_xpath']
        filename = inp['filename']
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'
        }

        # If entered filename contains extension -> drop extension
        if '.' in filename:
            filename = filename.split('.')[0]

        images = self.browser.find_elements(By.XPATH, image_xpath)

        if len(images) == 0:
            pass
        elif len(images) == 1:
            image_url = images[0].get_attribute('data-src')

            if not image_url:
                image_url = images[0].get_attribute('src')

            image_extension = image_url.split('.')[-1].split('?')[0]  # Drop ?xxx after image extension

            r = requests.get(image_url, headers=headers)
            with open(f'{filename}.{image_extension}', 'wb') as outfile:
                outfile.write(r.content)
        else:
            # Use provided filename as directory name. Images themselves will be filename_base_0, filename_base_1, filename_base_2 etc.
            images_directory = filename
            if not os.path.exists(images_directory):
                os.mkdir(images_directory)

            filename_base = filename

            for i, image in enumerate(images):
                try:
                    # Sometimes the image url is stored within data-src tag -> TODO: add new argument to handler with tag?
                    image_url = image.get_attribute('data-src')
                    if not image_url:
                        image_url = image.get_attribute('src')

                    image_extension = image_url.split('.')[-1].split('?')[0]  # Drop ?xxx after image extension
                    filename = f'{filename_base}_{i}.{image_extension}'

                    r = requests.get(image_url, headers=headers)
                    with open(f'{images_directory}/{filename}', 'wb') as outfile:
                        outfile.write(r.content)
                except:
                    pass

    def _click_xpath(self, inp):
        xpath = inp['xpath']

        xpath = xpath.removesuffix('//text()').rstrip('/')
        self._logger.info(f'Searching for element to click: {xpath}')

        try:
            element = self.browser.find_element(By.XPATH, xpath)

            if element.is_enabled():
                self._logger.info('Button is enabled')
                element.click()
            else:
                self._logger.warning('Button is not enabled, trying to enable it')
                self.browser.execute_script("arguments[0].removeAttribute('disabled','disabled')", element)
                element.click()
        except NoSuchElementException:
            self._logger.error('Element not found')

    def _click_name(self, inp):
        text = inp['text']

        self.browser.find_element(By.LINK_TEXT(text)).click()

    def send_text(self, inp):
        xpath = inp['xpath']
        text = inp['text']
        try:
            element = self.browser.find_element(By.XPATH, xpath)
            element.clear()
            ActionChains(self.browser).send_keys_to_element(element, text).perform()
            WebDriverWait(self.browser, 2).until(
                lambda: EC.text_to_be_present_in_element_value(xpath, text) or
                EC.text_to_be_present_in_element(xpath, text)
            )
        # Reraise as `SpiderFunctionError` as any `WebDriverException` subclassed error
        # will get caught by the `except WebDriverException` block in `parse()` loop.
        except NoSuchElementException as e:
            raise SpiderFunctionError(str(e)) from e

    def _prepare_xpath_for_extraction(self, xpath: str):
        # Extract link from "a" tags
        if xpath.split('/')[-1] == 'a' or xpath.split('/')[-1] == '/a' or xpath.split('/')[-1].startswith('a['):
            xpath += '/@href'
        elif not xpath.endswith('/text()') and not '@' in xpath.split('/')[-1]:
            xpath += '/text()'

        return xpath

    def _extract_xpath(self, inp):
        """
        write_in_file_mode ... w+, a+
        """
        xpath = inp['xpath']
        filename = inp['filename']  # "extracted_data.txt"

        tag = xpath.split('/')[-1]
        xpath = self._prepare_xpath_for_extraction(xpath)
        if tag == 'a':
            data = self.page.xpath(xpath).extract()
            data = [build_abs_url(scraped_link, self.browser.current_url) for scraped_link in data]
        else:
            data = self.page.xpath(xpath).extract()

        try:
            write_in_file_mode = inp['write_in_file_mode']
        except:
            write_in_file_mode = "w+"

        if not data:
            data = ['None']

        with open(filename, write_in_file_mode, encoding="utf-8") as f:
            if isinstance(data, list):
                for i, row in enumerate(data):
                    row = row.strip()

                    if row:
                        f.write(row.strip() + "\n")
            else:
                f.write(data.strip())

    def _extract_multiple_xpaths(self, inp):
        result = []
        xpaths = inp['xpaths']
        filename = inp['filename']  # "extracted_data.txt"

        for xpath in xpaths:
            xpath = self._prepare_xpath_for_extraction(xpath)
            tag = xpath.split('/')[-1]
            if tag == 'a':
                data = self.page.xpath(xpath).extract()
                data = [build_abs_url(scraped_link, self.browser.current_url) for scraped_link in data]
            else:
                data = self.page.xpath(xpath).extract()

            if not data:
                data = ['None']

            self._logger.info(f'Data from extracted XPath: {data}')
            result.append(data)

        short_filename = filename.split(".txt")[0]
        df = pd.DataFrame(result)
        df.to_excel(short_filename + ".xlsx")

        with open(filename, "w+", encoding="utf-8") as f:
            output_list = ["\n".join(result_element) for result_element in result]
            output = "\n".join(output_list)
            f.write(output)

    def _extract_table_xpath(self, inp):
        row_xpath = inp['xpath_row']
        column_xpath = inp['xpath_col']
        filename = inp['filename']  # "extracted_data.txt"
        first_row_header = inp['first_row_header']

        result = []
        trs = self.page.xpath(row_xpath)
        ths = self.page.xpath(row_xpath + '//th')
        headers = []

        # Try to find headers within <th> tags
        for th_tag in ths:
            headers.append(
                ''.join(th_tag.xpath('.//text()').extract()).replace('\n', '').replace('\t', '')
            )

        if trs:
            for j, tr in enumerate(trs):
                xp = row_xpath + "[" + str(j + 1) + "]" + column_xpath

                td_tags = self.page.xpath(xp)
                row = []
                for td in td_tags:
                    data = td.xpath('.//text()').getall()
                    '''r
                        Some table cells include \n or unicode symbols,
                        so that creates unneccesary "empty" columns and thus
                        the number of columns doesn't meet the real one
                    '''

                    data = [''.join(x.strip()).replace('\\', '') for x in data]  # Cleaning the text

                    # data = list(filter(None, data))  # Deleting empty strings

                    row.append('\n'.join(data).strip())  # Making one string value from list

                # details = page.xpath(xp).extract() #j+1 because xpath indices start with 1 (not with 0)

                # If first row should be headers and headers were not defined before
                if first_row_header and not headers:
                    headers = row
                else:
                    result.append(row)

        short_filename = filename.split(".pickle")[0]

        if headers:
            # Could be the situation when len of headers is not the same as len of row
            try:
                df = pd.DataFrame(result, columns=headers)
            except:
                df = pd.DataFrame(result)
        else:
            df = pd.DataFrame(result)

        # Remove empty rows
        df.dropna(axis=0, how='all', inplace=True)
        df.to_excel(short_filename + '.xlsx')

        self.docrawl_client.kv_redis.set(key='extracted_table', value=df)

    def _refresh_page_source(self, inp):
        self.page = Selector(text=self.browser.page_source)

    def initialize_screenshot_thread_if_not_existing(self, screenshot_filename = "website_loading_screenshot.png"):
        
        if self.screenshot_thread is None:
            self.screenshot_thread = ScreenshotThread(docrawl_spider = self, screenshot_filename = screenshot_filename)
            self.screenshot_thread.start()
            self.screenshot_time=0
            self._logger.info("Screenshot thread created with screenshot_filename: "+str(screenshot_filename))
        else:
            self.screenshot_thread.screenshot_filename = screenshot_filename #make sure the filename is correct if there is second attempt to initialize screenshot thread with different instructions (can happen e.g. load website and then take_screenshot immediately after that)
            self._logger.info("Screenshot screenshot_filename was updated: "+str(screenshot_filename))
            
    def increment_time_of_screenshot_thread(self,screenshot_refreshing_timespan = 5):
        """ screenshot refreshing timespan is in seconds"""
        
        if self.screenshot_thread is not None:
            self._logger.info("Screenshot thread update"+str(self.screenshot_time))
            
            self.screenshot_time+=1
            if self.screenshot_time > screenshot_refreshing_timespan:
                
                print("Screenshot thread stopping",self.screenshot_time)
                self.screenshot_thread.stop()
                self.screenshot_thread.join()
                self.screenshot_thread = None

    def parse(self, response):
        while True:
            self.increment_time_of_screenshot_thread()

            browser_meta_data = self.docrawl_client.get_browser_meta_data()
            spider_request = browser_meta_data['request']
            spider_function = browser_meta_data['function']
            proxy = browser_meta_data['browser']['proxy']
            url = spider_request['url']

            try:
                if url and not spider_request['loaded']:
                    if hasattr(self.browser, "proxy"):
                        if proxy != self.browser.proxy:
                            self._logger.warning('Proxy was updated in meanwhile')
                            self._update_proxy(proxy)

                    self.browser.get(url)
                    
                    page_source = self.browser.page_source
                    if isinstance(page_source, bytes):
                        page_source = page_source.decode('utf8')
                    
                    self.page = Selector(text=page_source)
                    
                    # collect headers for current page
                    headers = next((dict(req.headers) for req in self.browser.requests if req.response and req.url == url), None)
                    self.docrawl_client.set_browser_headers(headers)
                    
                    # collect cookies for current page
                    cookies = [dict(cookie) for cookie in self.browser.get_cookies()]
                    self.docrawl_client.set_browser_cookies(cookies)
                    
                    # collects requests, which contain: url, status code, headers from response, content from response 
                    requests = []
                    for _req in self.browser.requests:
                        _type = _req.headers.get('content-type')
                        if _req.response and  _type == 'application/json':
                            requests.append({
                                'url': _req.url,
                                'status_code': _req.response.status_code,
                                'headers': dict(_req.response.headers),
                                'content': str(_req.response.body),
                            })
                    self.docrawl_client.set_browser_requests(requests)

                    def _scroll_incrementally(pixels=300, pause_time=0.5, timeout=30, n_scrolls=1):
                        start_time = time.time()
                        last_position = self.browser.execute_script("return window.pageYOffset;")

                        for _ in range(n_scrolls):
                            # Scroll down by the specified number of pixels
                            self.browser.execute_script(f"window.scrollBy(0, {pixels});")

                            time.sleep(pause_time)

                            # Get the current position after scrolling
                            current_position = self.browser.execute_script("return window.pageYOffset;")

                            if current_position == last_position:
                                self._logger.warning("Reached the bottom of the page.")
                                break

                            last_position = current_position

                            # Break if scrolling takes too long
                            if time.time() - start_time > timeout:
                                self._logger.warning("Timeout reached while scrolling.")
                                break

                    self._logger.info('Initial page load completed, proceeding to scrolling for full render.')

                    # Scroll to bottom to ensure rendering is complete
                    _scroll_incrementally(pixels=700, pause_time=0.5, timeout=60, n_scrolls=1)
                    spider_request['loaded'] = True
                    browser_meta_data['request'] = spider_request
                    self.docrawl_client.set_browser_meta_data(browser_meta_data)
                    time.sleep(1) #make sure the screenshot is made after website is loaded
                elif not spider_function['done']:
                    function_str = spider_function['name']
                    inp = spider_function['input']

                    if f'_{function_str}' == "_take_png_screenshot":
                        # skip standard execution and run in a different thread
                        self.initialize_screenshot_thread_if_not_existing(inp["filename"])
                    else:  # Standard behaviour
                        self._logger.warning("Running docrawl function:" + f'_{function_str}')
                        getattr(self, f'_{function_str}')(inp=inp)

                    spider_function['done'] = True
                    spider_function['error'] = None
                    browser_meta_data['function'] = spider_function
                    self.docrawl_client.set_browser_meta_data(browser_meta_data)
                    
                time.sleep(1) #keep time increment in all cases, otherwise screenshots do not load correctly if the website renders dynamic elements (e.g. forloop blog)

            except (WebDriverException, MaxRetryError) as e:
                self._logger.error('Browser not responding')
                self._logger.error(traceback.format_exc())
                self._restart_browser()

            except Exception as e:
                self._logger.error(f'Error while executing docrawl loop: {e}')
                self._logger.error(traceback.format_exc())

                browser_meta_data = self.docrawl_client.get_browser_meta_data()
                spider_function['done'] = True
                spider_function['error'] = str(e)
                browser_meta_data['function'] = spider_function
                self.docrawl_client.set_browser_meta_data(browser_meta_data)

            except KeyboardInterrupt:
                break
