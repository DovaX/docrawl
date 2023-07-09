from docrawl.docrawl_logger import docrawl_logger
from docrawl.elements import Element, ElementType, classify_element_by_xpath, PREDEFINED_TAGS
import datetime
import scrapy
import requests
import time
import pickle
import os
import re
import psutil
import lxml.html
import pandas as pd

# Due to the problems with selenium wire on linux systems
try:
    from selenium import webdriver
except:
    docrawl_logger.error('Error while importing selenium-wire, using selenium instead')
    from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver import FirefoxOptions, ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.remote.webdriver import WebElement

from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.chrome import ChromeDriverManager

from scrapy.selector import Selector


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
        # TODO: get rid of global variables
        global kv_redis
        global kv_redis_key_webpage_elements
        global kv_redis_key_screenshot

        kv_redis = kw['kv_redis']
        kv_redis_keys = kw['kv_redis_keys']

        kv_redis_key_webpage_elements = kv_redis_keys.get('elements', kv_redis_key_webpage_elements)
        kv_redis_key_screenshot = kv_redis_keys.get('screenshot', kv_redis_key_screenshot)

        self.kv_redis = kv_redis
        self.kv_redis_keys = kv_redis_keys

        self.browser = self._initialise_browser()
        browser_meta_data = kv_redis.get('browser_meta_data')
        browser_meta_data['browser']['pid'] = self.browser_pid
        kv_redis.set('browser_meta_data', browser_meta_data)

        self.start_requests()

    def _initialise_browser(self):
        try:
            self.driver_type = self.kv_redis.get('browser_meta_data')['browser']['driver']
        except Exception as e:
            docrawl_logger.error(f'Error while loading driver type information: {e}')
            self.driver_type = 'Firefox'

        try:
            self.headless = self.kv_redis.get('browser_meta_data')['browser']['headless']
        except Exception as e:
            docrawl_logger.error(f'Error while loading headless mode information: {e}')
            self.headless = False

        try:
            proxy_info = self.kv_redis.get('browser_meta_data')['proxy']
        except Exception as e:
            docrawl_logger.error(f'Error while loading proxy information: {e}')
            proxy_info = None

        if self.driver_type == 'Firefox':
            capabilities = DesiredCapabilities.FIREFOX.copy()
            self.options = FirefoxOptions()
            capabilities["marionette"] = True

            sw_options = self._set_proxy(proxy_info)

            if self.headless:
                self.options.add_argument("--headless")

                # For headless mode different width of window is needed
                window_size_x = 1450

            try:
                self.browser = webdriver.Firefox(options=self.options, capabilities=capabilities,
                                                 service=Service(GeckoDriverManager().install()))
            except Exception as e:
                docrawl_logger.error(f'Error while creating Firefox instance {e}')
                self.browser = webdriver.Firefox(options=self.options, capabilities=capabilities)

        elif self.driver_type == 'Chrome':
            capabilities = DesiredCapabilities.CHROME
            self.options = ChromeOptions()

            sw_options = self._set_proxy(proxy_info)

            if self.headless:
                self.options.add_argument("--headless")

                # For headless mode different width of window is needed
                window_size_x = 1450

            try:
                self.browser = webdriver.Chrome(options=self.options, desired_capabilities=capabilities,
                                                executable_path=ChromeDriverManager().install())
            except:
                pass

        window_size_x = 1820

        self.browser.set_window_size(window_size_x, 980)

        return self.browser

    def __del__(self):
        self.browser.quit()

    def _set_proxy(self, proxy_info: dict) -> dict:
        """
        Sets proxy before launching browser instance.
        :param proxy_info: proxy params (ip, port, username, password)
        """

        # If proxy was not set
        if proxy_info is None or any([not proxy_info['ip'], not proxy_info['port']]):
            return None

        proxy_ip = proxy_info['ip']
        proxy_port = proxy_info['port']
        proxy_username = proxy_info['username']
        proxy_password = proxy_info['password']

        if proxy_username and proxy_password:
            proxy = f'http://{proxy_username}:{proxy_password}@{proxy_ip}:{proxy_port}'
        else:
            proxy = f'{proxy_ip}:{proxy_port}'

        # Proxy with authentication
        if 'http://' in proxy:
            # selenium-wire proxy settings
            sw_options = {
                'proxy': {
                    'http': proxy,
                    'https': proxy,
                    'no_proxy': 'localhost,127.0.0.1'
                }
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
        self.browser_pid = None
        try:
            if self.driver_type == 'Firefox':
                self.browser_pid = self.browser.capabilities['moz:processID']
            elif self.driver_type == 'Chrome':
                self.browser.service.process
                browser_pid = psutil.Process(self.browser.service.process.pid)
                self.browser_pid = browser_pid.pid

            # self.browser_pid = VarSafe(self.browser_pid, "browser_pid", "browser_pid")
            # save_variables(kept_variables, "scr_vars.kpv")
            docrawl_logger.success(f'Browser PID: {self.browser_pid}')
        except Exception as e:
            docrawl_logger.error(f'Error while determining browser PID: {e}')

    def start_requests(self):
        URLS = ['https://www.forloop.ai']
        FUNCTIONS = [self.parse]
        for i in range(len(URLS)):
            yield scrapy.Request(url=URLS[i], callback=FUNCTIONS[i])  # yield

    # # # # # # # SCRAPING FUNCTIONS # # # # # # #

    def _init_function(self, inp):
        pass

    def _click_class(self, inp):
        class_input = inp.get("filename")
        index = inp.get("index", 0)
        tag = inp.get("tag", "div")

        name_input = self.browser.find_elements(By.XPATH, f'//{tag}[@class="{class_input}"]')
        name_input[index].click()

        return name_input

    def _take_screenshot(self, inp):
        """
        Takes screenshot of current page and saves it.
            :param browser: Selenium driver, browser instance
            :param inp, list, inputs from launcher (filename)
        """

        # filename = inp['filename']
        browser_type = type(self.browser)

        if browser_type == webdriver.Firefox:

            try:
                docrawl_logger.warning('START SCREENSHOT CREATED')
                root_element = self.browser.find_element(By.XPATH, '/html')
                string = self.browser.get_full_page_screenshot_as_base64()
                self.browser.execute_script("return arguments[0].scrollIntoView(true);", root_element)

                # with open(filename, "w+") as fh:
                #     fh.write(string)
                #     docrawl_logger.warning('SCRENSHOT CREATED')
            except Exception as e:
                string = ""
                docrawl_logger.error(f'Error while taking page screenshot: {e}')

        elif browser_type == webdriver.Chrome:
            # Get params needed for fullpage screenshot
            page_rect = self.browser.execute_cdp_cmd('Page.getLayoutMetrics', {})

            # Set the width and height of the viewport to screenshot, same as the site's content size
            screenshot_config = {'captureBeyondViewport': True,
                                 'fromSurface': True,
                                 'clip': {'width': page_rect['cssContentSize']['width'],
                                          'height': page_rect['cssContentSize']['height'],
                                          'x': 0,
                                          'y': 0,
                                          'scale': 1},
                                 }
            # Dictionary with 1 key: data
            string = self.browser.execute_cdp_cmd('Page.captureScreenshot', screenshot_config)['data']  # Taking screenshot
            # with open(filename, "w+") as fh:
            #     fh.write(string)
        else:
            string = ""

        self.docrawl_client.set_browser_screenshot(string)
        docrawl_logger.warning('SCRENSHOT CREATED')

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
                root_element = self.browser.find_element(By.XPATH, '/html')

                screenshot = self.browser.get_full_page_screenshot_as_file(filename)
                self.browser.execute_script("return arguments[0].scrollIntoView(true);", root_element)

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

        incl_tables = inp['incl_tables']
        incl_bullets = inp['incl_bullets']
        incl_texts = inp['incl_texts']
        incl_headlines = inp['incl_headlines']
        incl_links = inp['incl_links']
        incl_images = inp['incl_images']
        incl_buttons = inp['incl_buttons']
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

        def save_coordinates_of_elements(selectors, names, xpaths, data):
            """
            Saves coordinates of elements (tables, bullet lists, text elements, headlines, links), that could be potentially exported.
            """
            elems_pos = []

            existing_xpaths = []

            for selector, name, xpath, data in zip(selectors, names, xpaths, data):

                # Deduplicate elements by xpath (on some pages the same element is selected twice)
                if xpath in existing_xpaths:
                    continue

                elems_pos.append({'rect': selector.rect,
                                  'name': name,
                                  'xpath': xpath,
                                  'data': data})

                existing_xpaths.append(xpath)

            elements_positions = {"elements_positions": elems_pos}
            filename = "elements_positions.kpv"
            with open(filename, "w+", encoding="utf8",
                      errors='ignore') as file:  # TODO: improve KPV to enable multiple keep variable files -> it collided with browser_metadata_kpv,
                file.write(str(elements_positions))

            # elements_positions = VarSafe(elements_positions, 'elements_positions', 'elements_positions')
            # save_variables(kept_variables, 'elements_positions.kpv')

        def extract_element_data(element: WebElement, xpath: str, element_type: ElementType):
            if element_type in [ElementType.LINK, ElementType.BUTTON]:
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

                text = df

                # # If dataframe is not empty
                # if not df.dropna().empty and len(df.columns) > 1 and len(df) > 1:
                #     # Serialize DataFrame
                #
                #     with open(path + '.pickle', 'wb') as pickle_file:
                #         pickle.dump(df, pickle_file)
            else:
                xpath += '//text()'

                # Try to extract text from element
                try:
                    text = ''.join(self.page.xpath(xpath).extract()).strip()
                # Extract element otherwise
                except:
                    xpath = xpath.removesuffix('//text()')
                    text = ''.join(self.page.xpath(xpath).extract()).strip()

            # attributes = element.get_property('attributes')
            attributes = self.browser.execute_script(
                'var items = {}; for (index = 0; index < arguments[0].attributes.length; ++index) { items[arguments[0].attributes[index].name] = arguments[0].attributes[index].value }; return items;',
                element)

            # docrawl_logger.warning(attributes)
            element_data = {
                'tag_name': re.split('//|/', xpath)[-1].split('[')[0],
                'text': text,
                'attributes': attributes
            }

            return element_data

        def serialize_and_append_data(element_name, selector: WebElement, xpath):
            """
            Serializes data behind the element and updates dictionary with final elements
                :param element_name: variable name to save
                :param selector: Selenium Selector
                :param xpath: XPath of element
            """

            path = os.path.join(output_folder, element_name)

            if 'table' in element_name:
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

                # If dataframe is not empty
                if not df.dropna().empty and len(df.columns) > 1 and len(df) > 1:
                    # Serialize DataFrame

                    with open(path + '.pickle', 'wb') as pickle_file:
                        pickle.dump(df, pickle_file)

            elif 'bullet' in element_name:
                data = process_bullet(xpath)
                xpath += '//li//text()'

                if len(data) > 1:
                    with open(path + '.pickle', 'wb') as pickle_file:
                        pickle.dump(data, pickle_file)

            elif 'image' in element_name:
                xpath += '/@src'
                data = self.page.xpath(xpath).extract()[0]

                if data:
                    # Handle images with relative path. E.g. images/logo.png
                    if not any([data.startswith(x) for x in ['http', 'www']]):
                        data = url_pattern.match(self.browser.current_url).group(1) + data.replace('./', '')

                    with open(path + '.pickle', 'wb') as pickle_file:
                        pickle.dump(data, pickle_file)
            else:
                if 'link' in element_name:
                    # Link tag may contain 2 types od data: link itself (href) and text, so prepare both
                    xpath_href = xpath + '/@href'
                    xpath_text = xpath + '//text()'

                    data = {
                        'link': ''.join(self.page.xpath(xpath_href).extract()).strip(),
                        'text': ''.join(self.page.xpath(xpath_text).extract()).strip()
                    }

                elif 'button' in element_name:
                    data = ''.join(self.page.xpath(xpath).extract()).strip()
                else:
                    xpath += '//text()'

                    # Try to extract text from element
                    try:
                        data = ''.join(self.page.xpath(xpath).extract()).strip()
                    # Extract element otherwise
                    except:
                        xpath = xpath.removesuffix('//text()')
                        data = ''.join(self.page.xpath(xpath).extract()).strip()

                if len(data) > 0:
                    with open(path + '.pickle', 'wb') as pickle_file:
                        pickle.dump(data, pickle_file)

            if 'context' in element_name:
                data = None
            else:
                try:
                    data = pickle_file.name
                except Exception as e:
                    data = None
                    docrawl_logger.error(f'Error while retrieving file with data: {e}')

            final_elements.update({element_name:
                                       {'selector': selector,
                                        'data': data,
                                        'xpath': xpath}})

        new_elements_all = []

        def find_elements(element_type: ElementType, custom_tags: list = None):
            """
            Finds elements on page using Selenium Selector and HTML Parser
                :param element_type: type of element (table, bullet, text, headline, link, ...)
                :param custom_tags: list of custom tags
            """

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
                            new_elements_all.append(element_c.dict())
                            added_xpaths.append(xpath)
                        # serialize_and_append_data(f'{element_name}_{i}', element, xpath)

                    except Exception as e:
                        docrawl_logger.error(f'Error while extracting data for element {elem_name}: {e}')

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

        # names = list(final_elements.keys())
        # selectors = [x['selector'] for x in final_elements.values()]
        # xpaths = [x['xpath'] for x in final_elements.values()]
        # data = [x['data'] for x in final_elements.values()]
        #
        # save_coordinates_of_elements(selectors, names, xpaths, data)

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

        try:
            with open(filename, 'w+', encoding="utf-8") as f:
                f.write(url)
        except Exception as e:
            docrawl_logger.error(f'Error while getting current URL: {e}')

    def _close_browser(self, inp):
        """
        Closes browser (removes driver instance).
            :param browser: driver instance
        """

        try:
            self.browser.quit()
            '''
            if not browser.current_url:
                time.sleep(1)
                close_browser(browser)
            '''


            # Remove proxy after closing browser instance
            proxy = {'ip': '', 'port': '', 'username': '', 'password': ''}
            browser_meta_data = self.docrawl_client.get_browser_meta_data()
            browser_meta_data['browser']['proxy'] = proxy

            self.docrawl_client.set_browser_meta_data(browser_meta_data)

        except ConnectionRefusedError:
            pass
        except Exception as e:
            docrawl_logger.error(f'Error while closing the browser: {e}')

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
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'}

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
        element = self.browser.find_element(By.XPATH, xpath)

        if element.is_enabled():
            docrawl_logger.info('Button is enabled')
            element.click()
        else:
            docrawl_logger.warning('Button is not enabled, trying to enable it')
            self.browser.execute_script("arguments[0].removeAttribute('disabled','disabled')", element)
            element.click()

    def _click_name(self, inp):
        text = inp['text']

        self.browser.find_element(By.LINK_TEXT(text)).click()

    def _extract_xpath(self, inp):
        """
        write_in_file_mode ... w+, a+
        """
        xpath = inp['xpath']
        filename = inp['filename']  # "extracted_data.txt"

        if not xpath.endswith('/text()') and not '@' in xpath.split('/')[-1]:
            xpath += '/text()'

        # Extract link from "a" tags
        if xpath.split('/')[-1] == 'a' or xpath.split('/')[-1] == '/a' or xpath.split('/')[-1].startswith('a['):
            xpath += '/@href'

        try:
            write_in_file_mode = inp['write_in_file_mode']
        except:
            write_in_file_mode = "w+"

        data = self.page.xpath(xpath).extract()

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

        for i, xpath in enumerate(xpaths):
            data = self.page.xpath(xpath).extract()
            docrawl_logger.info(f'Data from extracted XPath: {data}')
            result.append(data)

        short_filename = filename.split(".txt")[0]
        df = pd.DataFrame(result)
        df.to_excel(short_filename + ".xlsx")

        with open(filename, "w+", encoding="utf-8") as f:
            pass

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
            headers.append(''.join(th_tag.xpath('.//text()').extract()).replace('\n', '').replace('\t', ''))

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

    def parse(self, response):
        self.browser.get(self.kv_redis.get('browser_meta_data')['request']['url'])

        docrawl_core_done = False
        page = Selector(text=self.browser.page_source)

        while not docrawl_core_done:
            browser_meta_data = self.kv_redis.get('browser_meta_data')

            spider_request = browser_meta_data['request']
            spider_function = browser_meta_data['function']

            try:
                time.sleep(1)
                docrawl_logger.info('Docrawl core loop')
                docrawl_logger.info(f'Browser meta data: {browser_meta_data}')

                if not spider_request['loaded']:
                    self.browser.get(spider_request['url'])
                    page = Selector(text=self.browser.page_source)

                    spider_request['loaded'] = True
                    browser_meta_data['request'] = spider_request

                if not spider_function['done']:
                    function_str = spider_function['name']
                    function = eval(function_str)

                    inp = spider_function['input']
                    docrawl_logger.info(f'Function input from docrawl core: {inp}')

                    if function_str in FUNCTIONS.keys():
                        FUNCTIONS[function_str](browser=self.browser, page=page, inp=inp)
                    else:
                        function(inp)

                    spider_function['done'] = True
                    browser_meta_data['function'] = spider_function

                page = Selector(text=self.browser.page_source)
            except KeyboardInterrupt:
                break
            except Exception as e:
                docrawl_logger.error(f'Error while executing docrawl loop: {e}')
