# import sys #PATH initialization of modules
# try:
#    sys.path.append("C:\\Users\\EUROCOM\\Documents\\Git\\DovaX")
# except:
#    pass
from docrawl_logger import docrawl_logger
from collections import UserDict
import datetime
import scrapy
import requests
import time
import pickle
import os
import shutil
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

from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.chrome import ChromeDriverManager

from scrapy.selector import Selector
from keepvariable.keepvariable_core import VarSafe, kept_variables, save_variables, load_variable_safe


def click_class(browser, class_input, index=0, tag="div", wait1=1):
    name_input = browser.find_elements_by_xpath('//' + tag + '[@class="' + class_input + '"]')
    name_input[index].click()
    time.sleep(wait1)
    return (name_input)


def take_screenshot(browser, page, inp):
    """
    Takes screenshot of current page and saves it.
        :param browser: Selenium driver, browser instance
        :param inp, list, inputs from launcher (filename)
    """

    filename = inp['filename']

    if type(browser) == webdriver.Firefox:

        try:
            root_element = browser.find_element(By.XPATH, '/html')
            string = browser.get_full_page_screenshot_as_base64()
            browser.execute_script("return arguments[0].scrollIntoView(true);", root_element)

            with open(filename, "w+") as fh:
                fh.write(string)
        except Exception as e:
            docrawl_logger.error(f'Error while taking page screenshot: {e}')

    elif type(browser) == webdriver.Chrome:
        # Get params needed for fullpage screenshot
        page_rect = browser.execute_cdp_cmd('Page.getLayoutMetrics', {})

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
        string = browser.execute_cdp_cmd('Page.captureScreenshot', screenshot_config)['data']  # Taking screenshot
        with open(filename, "w+") as fh:
            fh.write(string)


def extract_page_source(browser, page, inp):
    """
    Extracts the source of currently scraped page.
        :param page: Selenium Selector, page to export source from
        :param inp: list, inputs from launcher (incl_tables, incl_bullets, output_dir)
    """

    filename = inp['filename']

    with open(filename, 'w+', encoding="utf-8") as f:
        f.write(browser.page_source)


def scan_web_page(browser, page, inp):
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
    context_xpath = inp['context_xpath']
    output_folder = inp['output_folder']

    # Predefined tags by type
    TABLE_TAG = ['table']
    BULLET_TAGS = ['ul', 'ol']
    TEXT_TAGS = ['p', 'strong', 'em', 'div[normalize-space(text())]', 'span[normalize-space(text())]']
    HEADLINE_TAGS = ['h1', 'h2']
    IMAGE_TAGS = ['img']
    BUTTON_TAGS = ['button', 'a[@role="button"]', 'a[contains(@class, "button")]',
                   'a[contains(@id, "button")]', 'a[@type="button"]', 'a[contains(@class, "btn")]']

    # <a> tags, excluding links in menu, links as images, mailto links and links with scripts
    LINK_TAGS = ["""
                    a[@href
                    and not(contains(@id, "Menu"))  
                    and not(contains(@id, "menu"))  
                    and not(contains(@class, "Menu"))  
                    and not(contains(@class, "menu"))   
                    and not(descendant::img) 
                    and not(descendant::svg)  
                    and not(contains(@href, "javascript"))  
                    and not(contains(@href, "mailto"))]
                    """]

    # All predefined tags
    PREDEFINED_TAGS = {'table': TABLE_TAG,
                       'bullet': BULLET_TAGS,
                       'text': TEXT_TAGS,
                       'headline': HEADLINE_TAGS,
                       'image': IMAGE_TAGS,
                       'button': BUTTON_TAGS,
                       'link': LINK_TAGS + ['a']}  # + ['a'] is to identify link tags when using custom XPath

    try:
        shutil.rmtree(output_folder)
    except:
        pass
    finally:
        os.mkdir(output_folder)

    # Dictionary with elements (XPaths, data)
    final_elements = {}

    # Page source for parser
    innerHTML = browser.execute_script("return document.body.innerHTML")
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

        tag_2 = page.xpath(xpath)[0]
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

        elements_positions = elems_pos
        elements_positions = VarSafe(elements_positions, 'elements_positions', 'elements_positions')

        save_variables(kept_variables, 'elements_positions.kpv')

    def serialize_and_append_data(element_name, selector, xpath):
        """
        Serializes data behind the element and updates dictionary with final elements
            :param element_name: variable name to save
            :param selector: Selenium Selector
            :param xpath: XPath of element
        """

        path = os.path.join(output_folder, element_name)

        if 'table' in element_name:
            table_2 = page.xpath(xpath)[0]

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
            data = page.xpath(xpath).extract()[0]

            if data:
                # Handle images with relative path. E.g. images/logo.png
                if not any([data.startswith(x) for x in ['http', 'www']]):
                    data = url_pattern.match(browser.current_url).group(1) + data.replace('./', '')

                with open(path + '.pickle', 'wb') as pickle_file:
                    pickle.dump(data, pickle_file)
        else:
            if 'link' in element_name:
                xpath += '/@href'
            elif 'element' in element_name:
                pass
            elif 'button' in element_name:
                pass
            else:
                xpath += '//text()'

            data = ''.join(page.xpath(xpath).extract()).strip()

            if data:
                with open(path + '.pickle', 'wb') as pickle_file:
                    pickle.dump(data, pickle_file)

        if 'context' in element_name:
            data = None
        else:
            data = pickle_file.name

        final_elements.update({element_name:
                                   {'selector': selector,
                                    'data': data,
                                    'xpath': xpath}})

    def find_elements(tags, element_name, custom_tag=False):
        """
        Finds elements on page using Selenium Selector and HTML Parser
            :param tags: list of tags
            :param element_name: type of element (table, bulelt, text, headline, link, ...)
            :param custom_tag: if provided tag is custom (not predefined)
        """

        elements = []
        elements_tree = []

        # If tag is not predefined -> there is no need to add prefix
        if not custom_tag:
            prefix = '//'
        else:
            prefix = ''

        for tag in tags:
            elements.extend(browser.find_elements(By.XPATH, f'{prefix}{tag}'))
            if custom_tag:
                tag = tag.replace('/body/', '/div/')  # Otherwise, elements_tree will be empty
            elements_tree.extend(tree.xpath(f'{prefix}{tag}'))

        if elements:
            for i, element in enumerate(elements):
                # Skip tables with no rows

                if element_name == 'table' and len(element.find_elements(By.XPATH, './/tr')) < 2:
                    continue

                try:
                    xpath = find_element_xpath(elements_tree, i)
                    serialize_and_append_data(f'{element_name}_{i}', element, xpath)

                except:
                    pass

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
        find_elements(TABLE_TAG, 'table')

    ##### BULLET SECTION #####
    if incl_bullets:
        find_elements(BULLET_TAGS, 'bullet')

    ##### TEXTS SECTION #####
    if incl_texts:
        find_elements(TEXT_TAGS, 'text')

    ##### HEADLINES SECTION #####
    if incl_headlines:
        find_elements(HEADLINE_TAGS, 'headline')

    ##### LINKS SECTION #####
    if incl_links:
        find_elements(LINK_TAGS, 'link')

    ##### IMAGES SECTION #####
    if incl_images:
        find_elements(IMAGE_TAGS, 'image')

    ##### BUTTONS SECTION #####
    if incl_buttons:
        find_elements(BUTTON_TAGS, 'button')

    ##### CUSTOM XPATH SECTION #####
    if by_xpath:
        # With text() at the end will not work
        by_xpath = by_xpath.removesuffix('/text()').rstrip('/')

        custom_tag = [by_xpath]
        custom_tag_splitted = re.split('//|/', by_xpath)  # Split XPath in parts
        last_element_in_xpath = custom_tag_splitted[-1]  # Last element in XPath

        # Default element's name
        element_name = 'element'

        # Try to find last element in XPath in predefined tags to identify element name
        for element_type, predefined_tags in PREDEFINED_TAGS.items():
            if any([last_element_in_xpath.startswith(x) for x in predefined_tags]):
                element_name = element_type
                break

        find_elements(custom_tag, element_name, custom_tag=True)

    if context_xpath:
        find_elements([context_xpath], 'context', custom_tag=True)

    ##### SAVING COORDINATES OF ELEMENTS #####

    names = list(final_elements.keys())
    selectors = [x['selector'] for x in final_elements.values()]
    xpaths = [x['xpath'] for x in final_elements.values()]
    data = [x['data'] for x in final_elements.values()]

    save_coordinates_of_elements(selectors, names, xpaths, data)

    docrawl_logger.info(f'Scan WEb Page function duration {timedelta_format(datetime.datetime.now(), time_start_f)}')


def wait_until_element_is_located(browser, page, inp):
    """
    Waits until certain element is located on page and then clicks on it.
        :param browser: Selenium driver, browser instance
        :param inp, list, inputs from launcher (xpath)

    Note: click() method may be replaced with another
    """

    xpath = inp['xpath']

    try:
        WebDriverWait(browser, 5).until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
    except Exception as e:
        docrawl_logger.error(f'Error while locating element: {e}')


def get_current_url(browser, page, inp):
    """
    Returns the URL of current opened website
        :param page: :param browser: driver instance
        :param inp: list, inputs from launcher (filename)
    """

    filename = inp['filename']
    url = str(browser.current_url)

    try:
        with open(filename, 'w+', encoding="utf-8") as f:
            f.write(url)
    except Exception as e:
        docrawl_logger.error(f'Error while getting current URL: {e}')


def close_browser(browser, page, inp):
    """
    Closes browser (removes driver instance).
        :param browser: driver instance
    """

    try:
        browser.quit()
        '''
        if not browser.current_url:
            time.sleep(1)
            close_browser(browser)
        '''

        # Remove proxy after closing browser instance
        proxy = {'ip': '', 'port': '', 'username': '', 'password': ''}
        proxy = VarSafe(proxy, "proxy", "proxy")

        save_variables(kept_variables, 'browser_meta_data.kpv')

    except ConnectionRefusedError:
        pass
    except Exception as e:
        docrawl_logger.error(f'Error while closing the browser: {e}')


def scroll_web_page(browser, page, inp):
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
        browser.execute_script(script)


def download_images(browser, page, inp):
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

    images = browser.find_elements(By.XPATH, image_xpath)

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


def click_xpath(browser, page, inp):
    xpath = inp['xpath']

    docrawl_logger.info('Searching for element to click')
    element = browser.find_element(By.XPATH, xpath)

    if element.is_enabled():
        docrawl_logger.info('Button is enabled')
        element.click()
    else:
        docrawl_logger.warning('Button is not enabled, trying to enable it')
        browser.execute_script("arguments[0].removeAttribute('disabled','disabled')", element)
        element.click()


def click_name(browser, page, inp):
    text = inp['text']

    browser.find_element_by_link_text(text).click()


def extract_xpath(browser, page, inp):
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

    data = page.xpath(xpath).extract()

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


def extract_multiple_xpaths(browser, page, inp):
    result = []
    xpaths = inp['xpaths']
    filename = inp['filename']  # "extracted_data.txt"

    for i, xpath in enumerate(xpaths):
        data = page.xpath(xpath).extract()
        docrawl_logger.info(f'Data from extracted XPath: {data}')
        result.append(data)

    short_filename = filename.split(".txt")[0]
    df = pd.DataFrame(result)
    df.to_excel(short_filename + ".xlsx")

    with open(filename, "w+", encoding="utf-8") as f:
        pass


def extract_table_xpath(browser, page, inp):
    row_xpath = inp['xpath_row']
    column_xpath = inp['xpath_col']
    filename = inp['filename']  # "extracted_data.txt"
    first_row_header = inp['first_row_header']

    result = []
    trs = page.xpath(row_xpath)
    ths = page.xpath(row_xpath + '//th')
    headers = []

    # Try to find headers within <th> tags
    for th_tag in ths:
        headers.append(''.join(th_tag.xpath('.//text()').extract()).replace('\n', '').replace('\t', ''))

    if trs:
        for j, tr in enumerate(trs):
            xp = row_xpath + "[" + str(j + 1) + "]" + column_xpath

            td_tags = page.xpath(xp)
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

    with open(filename, 'wb') as pickle_file:
        pickle.dump(df, pickle_file)


class BrowserMetaData(UserDict):
    """
    Stores browser metadata such as driver, proxy, request and function info.

    Ensures synchronisation with .kpv file.

    Expected structure:

    browser_metadata = {
        "browser": {"driver": "Firefox", "headless": True, "browser_pid": 1234},
        "proxy": {"ip": "192.123.23.2", "port": 17, "username": "qwerty", "password": "12345"},
        "request": {"url": "https://forloop.ai", "loaded": True},
        "function": {"name": "extract_xpath", "input": "/html/main/div[4]/span", "done": False}
    }
    """

    def __init__(self, kpv_file: str = 'browser_meta_data.kpv'):
        super().__init__()

        self.kpv_file = kpv_file
        self._set_init_values()

    def __str__(self):
        """
        Prints metadata in certain order.
        """

        elements_order = ['browser', 'proxy', 'request', 'function']
        ordered_dict = dict()

        for elem in elements_order:
            ordered_dict[elem] = self.data.get(elem)

        return str(ordered_dict)

    def _set_init_values(self):
        """
        Initial value for function must be set so crawler is able to start requests.
        """

        init_function = {"name": "print", "input": "Bla", "done": False}
        self.__setitem__('function', init_function)

    def __setitem__(self, key, value):
        VarSafe(value, key, key)
        save_variables(kept_variables, self.kpv_file)
        super().__setitem__(key, value)

    def __getitem__(self, key):
        try:
            value = load_variable_safe(self.kpv_file, key)
        except Exception as e:
            value = None

        # Update value in dict
        self.__setitem__(key, value)

        return self.data[key]


class DocrawlSpider(scrapy.spiders.CrawlSpider):
    name = "forloop"
    # allowed_domains = ['google.com']

    custom_settings = {
        'LOG_LEVEL': 'ERROR',
        'USER_AGENT': "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
        'DEFAULT_REQUEST_HEADERS': {
            'Referer': 'https://forloop.ai'
        }
        #   'CONCURRENT_REQUESTS' : '20',
    }

    def __init__(self):
        # can be replaced for debugging with browser = webdriver.FireFox()
        # self.browser = webdriver.PhantomJS(executable_path=PHANTOMJS_PATH, service_args=['--ignore-ssl-errors=true'])
        self.meta_data = BrowserMetaData()

        self.browser = self._initialise_browser()

        browser_info = {
            'driver': self.driver_type,
            'headless': self.headless,
            'browser_pid': self.browser_pid
        }

        self.meta_data['browser'] = browser_info

        self.start_requests()

    def _initialise_browser(self):
        try:
            self.driver_type = self.meta_data['browser']['driver']
        except Exception as e:
            docrawl_logger.error(f'Error while loading driver type information: {e}')
            self.driver_type = 'Firefox'

        try:
            self.headless = self.meta_data['browser']['headless']
        except Exception as e:
            docrawl_logger.error(f'Error while loading headless mode information: {e}')
            self.headless = False

        try:
            proxy_info = self.meta_data['proxy']
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

        self._determine_browser_pid()

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

    def parse(self, response):
        self.browser.get(response.url)

        docrawl_core_done = False
        page = Selector(text=self.browser.page_source)

        while not docrawl_core_done:
            spider_request = self.meta_data['request']
            spider_function = self.meta_data['function']

            try:
                time.sleep(1)
                docrawl_logger.info('Docrawl core loop')
                docrawl_logger.info(f'Browser meta data: {self.meta_data}')
                docrawl_logger.info(f'Spider function: {spider_function}')
                docrawl_logger.info(f'Spider request: {spider_request}')

                if not spider_request['loaded']:
                    self.browser.get(spider_function['url'])
                    page = Selector(text=self.browser.page_source)

                    spider_request['loaded'] = True
                    self.meta_data['request'] = spider_request

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
                    self.meta_data['function'] = spider_function

                page = Selector(text=self.browser.page_source)
            except KeyboardInterrupt:
                break


FUNCTIONS = {"click_xpath": click_xpath,
             "click_name": click_name,
             "extract_xpath": extract_xpath,
             "extract_multiple_xpaths": extract_multiple_xpaths,
             "extract_table_xpath": extract_table_xpath,
             "get_current_url": get_current_url,
             "scan_web_page": scan_web_page,
             "close_browser": close_browser,
             "extract_page_source": extract_page_source,
             "wait_until_element_is_located": wait_until_element_is_located,
             "take_screenshot": take_screenshot,
             "scroll_web_page": scroll_web_page,
             "download_images": download_images,
             }