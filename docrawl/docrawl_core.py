import datetime
import os
import re
import time
import traceback

import lxml.html
import pandas as pd
import psutil
import requests
import scrapy
from scrapy.selector import Selector
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    WebDriverException,
)

from selenium.webdriver import ChromeOptions, FirefoxOptions, ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.remote.webdriver import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.exceptions import MaxRetryError
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager

from docrawl.errors import SpiderFunctionError
from docrawl.docrawl_logger import docrawl_logger
from docrawl.elements import PREDEFINED_TAGS, Element, ElementType, classify_element_by_xpath
from docrawl.utils import build_abs_url

# Due to the problems with selenium wire on linux systems
try:
    from seleniumwire import webdriver
except:
    docrawl_logger.error('Error while importing selenium-wire, using selenium instead')
    docrawl_logger.error('TRY: pip install blinker==1.7.0')
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
        docrawl_logger.info(f"Screenshot thread: Screenshot taken - {self.screenshot_filename}")

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
                docrawl_logger.warning(
                    "GeckoDriverManager update was not successful - launching latest Firefox version instead"
                    + str(e)
                )

            try:
                self.browser = webdriver.Firefox(
                    options=self.options, service=service, seleniumwire_options=sw_options
                )
            except Exception as e:
                docrawl_logger.error(f'Error while creating Firefox instance {e}')
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
                docrawl_logger.error(f'Error while creating Chrome instance {e}')
                self.browser = webdriver.Chrome(options=self.options)

        window_size_x = 1820
        self.browser.set_window_size(window_size_x, 980)

        browser_meta_data['browser']['pid'] = self._determine_browser_pid()
        if browser_meta_data.get('request'):
            browser_meta_data['request']['loaded'] = False
        self.docrawl_client.set_browser_meta_data(browser_meta_data)
        docrawl_logger.info(f'Browser settings: {browser_meta_data}')

        return self.browser

    def _close_browser(self, inp):
        """
        Close browser (remove driver instance).

        :param browser: driver instance
        """
        try:
            self.browser.quit()
        except ConnectionRefusedError as e:
            docrawl_logger.error(f'Error while closing the browser: {e}')
        except Exception as e:
            docrawl_logger.error(f'Error while closing the browser: {e}')

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
            docrawl_logger.error("Browser crashed")

        self.browser = self._initialise_browser()
        docrawl_logger.warning("Browser restarted")

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
            docrawl_logger.warning("Proxy updated")

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
        docrawl_logger.warning("_init_function is being executed")

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
                    docrawl_logger.warning("Warning: Website wasn't scrolled while taking screenshot: "+str(e))
                docrawl_logger.info(f'Png screenshot created')
            except Exception as e:
                docrawl_logger.error(f'Error while taking page png screenshot: {e}')

    def _extract_page_source(self, inp):
        """
        Extracts the source of currently scraped page.
            :param page: Selenium Selector, page to export source from
            :param inp: list, inputs from launcher (incl_tables, incl_bullets, output_dir)
        """

        filename = inp['filename']

        with open(filename, 'w+', encoding="utf-8") as f:
            f.write(self.browser.page_source)

    def _scan_web_page(self, inp):
        """
        Finds different elements (tables, bullet lists) on page.
            :param page: Selenium Selector, page to search elements in
            :param inp: list, inputs from launcher (incl_tables, incl_bullets, output_dir)
            :param browser: webdriver, browser instance
        """

        docrawl_logger.warning("Scan web page has started")

        incl_tables = inp['incl_tables']
        incl_bullets = inp['incl_bullets']
        incl_texts = inp['incl_texts']
        incl_headlines = inp['incl_headlines']
        incl_links = inp['incl_links']
        incl_images = inp['incl_images']
        incl_buttons = inp['incl_buttons']
        incl_inputs = inp.get('incl_input', True)
        by_xpath = inp['by_xpath']
        cookies_xpath = inp['cookies_xpath']  # dev param only
        context_xpath = inp['context_xpath']
        output_folder = inp['output_folder']

        # First removed old data
        self.docrawl_client.set_browser_scanned_elements(elements=[])

        # Dictionary with elements (XPaths, data)
        final_elements = {}

        # Page source for parser
        innerHTML = self.browser.execute_script("return document.body.innerHTML")
        tree = lxml.html.fromstring(innerHTML)

        # Url pattern, used to get main page url from current url
        url_pattern = re.compile('^(((https|http):\/\/|www\.)*[a-zA-Z0-9\.\/\?\:@\-_=#]{2,100}\.[a-zA-Z]{2,6}\/)')

        time_start_f = datetime.datetime.now()

        def timedelta_format(end, start):
            delta = end - start
            sec = delta.seconds
            microsec = delta.microseconds

            return f'{sec}:{microsec}'

        def string_cleaner(string):
            """
            Removes whitespaces from string.
                :param string: string, string to clean
                :return - cleaned string
            """

            return ''.join(string.strip()).replace('\\', '')

        def process_bullet(xpath):
            """
            Processes (cleans) bullet element, e.g. one <li> element per line
                :param xpath: XPath of element
            """

            tag_2 = self.page.xpath(xpath)[0]
            result = []
            li_tags = tag_2.xpath('.//li')

            # <li> inside <ol> don't contain numbers, but they could be added here
            for li_tag in li_tags:
                data = li_tag.xpath('.//text()').getall()
                data = [string_cleaner(x) for x in data]  # Cleaning the text
                data = list(filter(None, data))
                element = ' '.join(data).replace(u'\xa0', u' ')

                result.append(element + '\n')

            return result

        def extract_element_data(element: WebElement, xpath: str, element_type: ElementType):
            attributes = self.browser.execute_script(
                'var items = {}; for (index = 0; index < arguments[0].attributes.length; ++index) { items[arguments[0].attributes[index].name] = arguments[0].attributes[index].value }; return items;',
                element
            )

            if element_type == ElementType.LINK:
                xpath_new = xpath + '//text()'
                text = ''.join(self.page.xpath(xpath_new).extract()).strip()
                attributes['href'] = build_abs_url(attributes['href'], self.browser.current_url)
            elif element_type == ElementType.BUTTON:
                xpath_new = xpath + '//text()'
                text = ''.join(self.page.xpath(xpath_new).extract()).strip()
            elif element_type == ElementType.IMAGE:
                xpath_new = xpath + '//text()'
                text = self.page.xpath(xpath_new).extract()

                # if data:
                #     # Handle images with relative path. E.g. images/logo.png
                #     if not any([data.startswith(x) for x in ['http', 'www']]):
                #         data = url_pattern.match(browser.current_url).group(1) + data.replace('./', '')
                #
                #     with open(path + '.pickle', 'wb') as pickle_file:
                #         pickle.dump(data, pickle_file)
            elif element_type == ElementType.BULLET:
                text = process_bullet(xpath)
                # xpath_new = xpath + '//li//text()'
            elif element_type == ElementType.TABLE:
                table_2 = self.page.xpath(xpath)[0]

                result = []  # data
                titles = []  # columns' names
                tr_tags = table_2.xpath('.//tr')  # <tr> = table row
                th_tags = table_2.xpath('.//th')  # <th> = table header (non-essential, so if any)

                for th_tag in th_tags:
                    titles.append(''.join(th_tag.xpath('.//text()').extract()).replace('\n', '').replace('\t', ''))

                for tr_tag in tr_tags:
                    td_tags = tr_tag.xpath('.//td')  # <td> = table data

                    row = []
                    '''
                        # Sometimes between td tags there more than 1 tag with text, so that would
                        # be proceeded as separate values despite of fact it should be in one cell.
                        # That's why the further loop is needed. The result of it is a list of strings,
                        # that should be in one cell later in dataframe.

                        # Example: 

                        <td>
                            <a>Text 1</a>
                            <a>Text 2</a>
                        </td>

                        # Without loop it would be two strings ("Text 1", "Text 2") and thus they 
                        # will be in 2 differrent columns. With loop the result would be ["Text 1", "Text 2"] 
                        # and after join method - "Text 1 Text 2" in just one cell (column).
                   '''

                    for td_tag in td_tags:
                        data = td_tag.xpath('.//text()').getall()
                        '''
                            Some table cells include \n or unicode symbols,
                            so that creates unneccesary "empty" columns and thus
                            the number of columns doesn't meet the real one
                        '''

                        data = [string_cleaner(x) for x in data]  # Cleaning the text

                        # data = list(filter(None, data))  # Deleting empty strings

                        row.append('\n'.join(data))  # Making one string value from list

                    result.append(row)

                    if not titles:  # If table doesn't have <th> tags -> use first row as titles
                        titles = row  # TODO: IF USER SELECTS THE TABLE, ASK HIM, WHETHER HE WANTS TO HAVE 1 ROW AS TITLES

                try:
                    # If number of columns' names (titles) is the same as number of columns
                    df = pd.DataFrame(result, columns=titles)
                except Exception:
                    df = pd.DataFrame(result)

                df = df.iloc[1:, :]  # Removing empty row at the beginning of dataframe

                df.dropna(axis=0, how='all', inplace=True)

                text = df.to_json()

                # # If dataframe is not empty
                # if not df.dropna().empty and len(df.columns) > 1 and len(df) > 1:
                #     # Serialize DataFrame
                #
                #     with open(path + '.pickle', 'wb') as pickle_file:
                #         pickle.dump(df, pickle_file)
            elif element_type == ElementType.INPUT:
                text = element.text
            else:
                xpath += '//text()'

                # Try to extract text from element
                try:
                    text = ''.join(self.page.xpath(xpath).extract()).strip()
                # Extract element otherwise
                except:
                    xpath = xpath.removesuffix('//text()')
                    text = ''.join(self.page.xpath(xpath).extract()).strip()

            # docrawl_logger.warning(attributes)
            element_data = {
                'tagName': element.tag_name,
                'textContent': text,
                'attributes': attributes
            }

            return element_data

        global new_elements_all
        new_elements_all = []

        def find_elements(element_type: ElementType, custom_tags: list = None):
            """
            Finds elements on page using Selenium Selector and HTML Parser
                :param element_type: type of element (table, bullet, text, headline, link, ...)
                :param custom_tags: list of custom tags
            """

            global new_elements_all

            tags = PREDEFINED_TAGS[element_type] if not custom_tags else custom_tags
            elements = []
            elements_tree = []

            # If tag is not predefined -> there is no need to add prefix
            prefix = '' if custom_tags else '//'

            for tag in tags:
                elements.extend(self.browser.find_elements(By.XPATH, f'{prefix}{tag}'))
                if custom_tags:
                    tag = tag.replace('/body/', '/div/')  # Otherwise, elements_tree will be empty
                elements_tree.extend(tree.xpath(f'{prefix}{tag}'))

            if elements:
                added_xpaths = []  # For deduplication of elements
                for i, element in enumerate(elements):
                    if not is_element_sized(element):
                        continue

                    elem_name = f'{element_type}_{i}'

                    # Skip tables with no rows
                    if element_type == ElementType.TABLE and len(element.find_elements(By.XPATH, './/tr')) < 2:
                        continue

                    try:
                        xpath = find_element_xpath(elements_tree, i)

                        if xpath not in added_xpaths:
                            element_data = extract_element_data(element=element, xpath=xpath, element_type=element_type)
                            element_c = Element(name=elem_name, type=element_type, rect=element.rect, xpath=xpath,
                                                data=element_data)
                            if is_element_empty(element_c):
                                continue
                            new_elements_all.append(element_c.dict())
                            added_xpaths.append(xpath)
                        # serialize_and_append_data(f'{element_name}_{i}', element, xpath)

                    except Exception as e:
                        docrawl_logger.error(f'Error while extracting data for element {elem_name}: {e}')

        def is_element_sized(element: WebElement) -> bool:
            """Skip elements with no width or height."""
            element_size = element.size
            if element_size['width'] == 0 or element_size['height'] == 0:
                return False
            return True

        def is_element_empty(element: Element) -> bool:
            """Skip elements based on their type and 'emptiness' rules."""
            if element.type in [ElementType.TEXT, ElementType.HEADLINE]:
                # Skip text-based elements with no text or whitespaces only
                return element.data['textContent'].strip() == ''
            else:
                # TODO: Add checks for other types of elements if necessary
                pass
            return False

        def find_element_xpath(tree, i):
            """
            Finds the XPath of element using HTML parser.
                :param tree: list of elements
                :param i: element's number
            """
            xpath = tree[i].getroottree().getpath(tree[i])
            xpath = xpath.split('/')
            xpath[2] = 'body'  # For some reason getpath() generates <div> instead of <body>
            xpath = '/'.join(xpath)

            return xpath

        docrawl_logger.info("Find elements phase has started")

        ##### INPUT SECTION #####
        if incl_inputs:
            find_elements(element_type=ElementType.INPUT)

        ##### TABLES SECTION #####
        if incl_tables:
            find_elements(element_type=ElementType.TABLE)

        ##### BULLET SECTION #####
        if incl_bullets:
            find_elements(element_type=ElementType.BULLET)

        ##### TEXTS SECTION #####
        if incl_texts:
            find_elements(element_type=ElementType.TEXT)

        ##### HEADLINES SECTION #####
        if incl_headlines:
            find_elements(element_type=ElementType.HEADLINE)

        ##### LINKS SECTION #####
        if incl_links:
            find_elements(element_type=ElementType.LINK)

        ##### IMAGES SECTION #####
        if incl_images:
            find_elements(element_type=ElementType.IMAGE)

        ##### BUTTONS SECTION #####
        if incl_buttons:
            find_elements(element_type=ElementType.BUTTON)

        ##### CUSTOM XPATH SECTION #####
        if by_xpath:
            # Temporary workaround due to weird behaviour of lists in kpv
            if ';' in by_xpath:
                list_of_xpaths = by_xpath.split(';')[:-1]
            else:
                list_of_xpaths = [by_xpath]

            for i, elem in enumerate(list_of_xpaths):
                # With text() at the end will not work
                xpath = elem.removesuffix('/text()').rstrip('/')

                custom_tags = [xpath]
                element_type = classify_element_by_xpath(xpath)

                find_elements(element_type=element_type, custom_tags=custom_tags)

        if context_xpath:
            try:
                find_elements(element_type=ElementType.CONTEXT, custom_tags=[context_xpath])
            except Exception as e:
                docrawl_logger.error(f'Error while retrieving context elements: {e}')

        if cookies_xpath:
            find_elements(element_type=ElementType.COOKIES, custom_tags=[cookies_xpath])

        ##### SAVING COORDINATES OF ELEMENTS #####

        self.docrawl_client.set_browser_scanned_elements(new_elements_all)
        docrawl_logger.info(
            f'Scan Web Page function duration {timedelta_format(datetime.datetime.now(), time_start_f)}')

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
            docrawl_logger.error(f'Error while locating element: {e}')

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
        docrawl_logger.info(f'Searching for element to click: {xpath}')

        try:
            element = self.browser.find_element(By.XPATH, xpath)

            if element.is_enabled():
                docrawl_logger.info('Button is enabled')
                element.click()
            else:
                docrawl_logger.warning('Button is not enabled, trying to enable it')
                self.browser.execute_script("arguments[0].removeAttribute('disabled','disabled')", element)
                element.click()
        except NoSuchElementException:
            docrawl_logger.error('Element not found')

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

            docrawl_logger.info(f'Data from extracted XPath: {data}')
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
            docrawl_logger.info("Screenshot thread created with screenshot_filename: "+str(screenshot_filename))
        else:
            self.screenshot_thread.screenshot_filename = screenshot_filename #make sure the filename is correct if there is second attempt to initialize screenshot thread with different instructions (can happen e.g. load website and then take_screenshot immediately after that)
            docrawl_logger.info("Screenshot screenshot_filename was updated: "+str(screenshot_filename))
            
    def increment_time_of_screenshot_thread(self,screenshot_refreshing_timespan = 5):
        """ screenshot refreshing timespan is in seconds"""
        
        if self.screenshot_thread is not None:
            docrawl_logger.info("Screenshot thread update"+str(self.screenshot_time))
            
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
                            docrawl_logger.warning('Proxy was updated in meanwhile')
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
                        docrawl_logger.warning("Running docrawl function:" + f'_{function_str}')
                        getattr(self, f'_{function_str}')(inp=inp)

                    spider_function['done'] = True
                    spider_function['error'] = None
                    browser_meta_data['function'] = spider_function
                    self.docrawl_client.set_browser_meta_data(browser_meta_data)
                    
                time.sleep(1) #keep time increment in all cases, otherwise screenshots do not load correctly if the website renders dynamic elements (e.g. forloop blog)

            except (WebDriverException, MaxRetryError) as e:
                docrawl_logger.error('Browser not responding')
                docrawl_logger.error(traceback.format_exc())
                self._restart_browser()

            except Exception as e:
                docrawl_logger.error(f'Error while executing docrawl loop: {e}')
                docrawl_logger.error(traceback.format_exc())

                browser_meta_data = self.docrawl_client.get_browser_meta_data()
                spider_function['done'] = True
                spider_function['error'] = str(e)
                browser_meta_data['function'] = spider_function
                self.docrawl_client.set_browser_meta_data(browser_meta_data)

            except KeyboardInterrupt:
                break
