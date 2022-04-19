# import sys #PATH initialization of modules
# try:
#    sys.path.append("C:\\Users\\EUROCOM\\Documents\\Git\\DovaX")
# except:
#    pass
import datetime
import platform
import scrapy
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pynput.keyboard
import pickle
import os
import shutil


keyboard = pynput.keyboard.Controller()
key = pynput.keyboard.Key

from scrapy.selector import Selector
from keepvariable.keepvariable_core import VarSafe, kept_variables, save_variables, load_variable_safe
import pandas as pd

spider_requests = {"url": "www.forloop.ai", "loaded": True}

spider_functions = {"function": "print", "input": "Bla", "done": False}
spider_functions = VarSafe(spider_functions, "spider_functions", "spider_functions")

browser_pid = None
docrawl_core_done = False


def click_class(browser, class_input, index=0, tag="div", wait1=1):
    name_input = browser.find_elements_by_xpath('//' + tag + '[@class="' + class_input + '"]')
    name_input[index].click()
    time.sleep(wait1)
    return (name_input)


if platform.system() == 'Windows':
    PHANTOMJS_PATH = './phantomjs/bin/phantomjs.exe'
else:
    PHANTOMJS_PATH = './phantomjs/bin/phantomjs'
LOGIN = False


def print_special(inp):
    """prints and saves the output to kv.kept_variables"""
    print(inp)
    inp = VarSafe(inp, "inp", "inp")

    save_variables({"inp": inp}, filename="input.kpv")


def take_screenshot(browser, inp):
    """
    Takes screenshot of current page and saves it.
        :param browser: Selenium driver, browser instance
        :param inp, list, inputs from launcher (filename)
    """

    filename = inp[0]

    try:
        root_element = browser.find_element(By.XPATH, '/html')
        root_element.screenshot(filename)

        browser.execute_script("return arguments[0].scrollIntoView(true);", root_element)
    except Exception as e:
        print('Error while taking page screenshot!', e)


def extract_page_source(page, inp):
    """
    Extracts the source of currently scraped page.
        :param page: Selenium Selector, page to export source from
        :param inp: list, inputs from launcher (incl_tables, incl_bullets, output_dir)
    """

    varname = inp[0]
    filename = inp[1]

    with open(filename, 'w+', encoding="utf-8") as f:
        f.write(page.page_source)


def scan_web_page(page, inp, browser):
    """
    Finds different elements (tables, bullet lists) on page.
        :param page: Selenium Selector, page to search elements in
        :param inp: list, inputs from launcher (incl_tables, incl_bullets, output_dir)
        :param browser: webdriver, browser instance
    """

    incl_tables = inp[0]
    incl_bullets = inp[1]

    # Folder for serialized dataframes
    pickle_folder = 'pickle_scraped_data'

    try:
        shutil.rmtree(pickle_folder)
    except:
        pass
    finally:
        os.mkdir(pickle_folder)

    # Dictionary with elements (XPaths, data)
    final_elements = {}

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

    def find_bullets(*tags_type):
        """
        Finds bullet lists (usual and numbered) in page.
            :param tags_type: list, type of bullet list tags (ul, ol)
        """
        i = 1

        # Bullets ready for export
        final_bullets = []

        for tags in tags_type:
            for tag in tags:
                xpath = generate_XPath(tag, '')
                tag_2 = page.xpath(xpath)[0]
                result = []
                li_tags = tag_2.xpath('.//li')

                for li_tag in li_tags:
                    data = li_tag.xpath('.//text()').getall()
                    data = [string_cleaner(x) for x in data]  # Cleaning the text
                    data = list(filter(None, data))

                    element = ' '.join(data).replace(u'\xa0', u' ')

                    result.append(element + '\n')

                # If list contains > 1 element
                if len(result) > 1:
                    final_bullets.append(tag)

                    final_elements.update({f'bullet_{i}':
                                               {'selector': tag,
                                                'data': result,
                                                'xpath': xpath}})
                    i += 1

    def generate_XPath(childElement, current):
        """
        Generates XPath of Selenium object. Recursive function.
            :param childElement: Selenium Selector
            :param current: string, current XPath

            :return - XPath
        """

        childTag = childElement.tag_name

        if childTag == 'html':
            return '/html[1]' + current

        parentElement = childElement.find_element(By.XPATH, '..')
        childrenElements = parentElement.find_elements(By.XPATH, '*')

        count = 0

        for childrenElement in childrenElements:
            childrenElementTag = childrenElement.tag_name

            if childTag == childrenElementTag:
                count += 1

            if childElement == childrenElement:
                return generate_XPath(parentElement, f'/{childTag}[{count}]{current}')

        return None

    def save_coordinates_of_elements(selectors, names, xpaths, data):
        """
        Saves coordinates of elements (tables, bullet list), that could be potentially exported.
        """
        elems_pos = []

        for selector, name, xpath, data in zip(selectors, names, xpaths, data):
            elems_pos.append({'rect': selector.rect,
                              'name': name,
                              'xpath': xpath,
                              'data': data})

        elements_positions = elems_pos
        elements_positions = VarSafe(elements_positions, 'elements_positions', 'elements_positions')

        save_variables(kept_variables, 'elements_positions.kpv')


    time_start_findtables = datetime.datetime.now()


    ##### TABLES SECTION #####
    if incl_tables:
        xpath = '//table'
        # tables = page.xpath(xpath)
        tables = browser.find_elements(By.XPATH, '//table')

        i = 1

        if tables:
            for table in tables:
                time_start_xpathtable = datetime.datetime.now()

                if len(table.find_elements(By.XPATH, './/tr')) < 2:
                    continue

                xpath = generate_XPath(table, '')
                print('[TIME] GENERATING XPATH OF TABLE ----->',
                      timedelta_format(datetime.datetime.now(), time_start_xpathtable))
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
                        data = list(filter(None, data))  # Deleting empty strings

                        row.append('\n'.join(data))  # Making one string value from list

                    row = list(filter(None, row))

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
                    path = os.path.join(pickle_folder, f'table_{i}')

                    with open(path + '.pickle', 'wb') as pickle_file:
                        pickle.dump(df, pickle_file)

                    final_elements.update({f'table_{i}':
                                               {'selector': table,
                                                'data': pickle_file.name,
                                                'xpath': xpath}})
                    i += 1

    print('[TIME] FINDING TABLES --->', timedelta_format(datetime.datetime.now(), time_start_findtables))

    ##### BULLET SECTION #####

    time_start_findbullets = datetime.datetime.now()

    if incl_bullets:
        # <li> inside <ol> don't contain numbers, but they could be added here
        ul_tags = browser.find_elements(By.XPATH, '//ul')  # Usual bullet lists
        ol_tags = browser.find_elements(By.XPATH, '//ol')  # Bullet numbered lists

        if ul_tags or ol_tags:
            bullets = find_bullets(ul_tags, ol_tags)

    print('[TIME] FINDING BULLETS --->', timedelta_format(datetime.datetime.now(), time_start_findbullets))

    ##### SAVING COORDINATES OF ELEMENTS SECTION #####

    #browser.find_element_by_xpath('/html').screenshot('browser_view.png')

    names = list(final_elements.keys())
    selectors = [x['selector'] for x in final_elements.values()]
    xpaths = [x['xpath'] for x in final_elements.values()]
    data = [x['data'] for x in final_elements.values()]

    save_coordinates_of_elements(selectors, names, xpaths, data)

    print('[TIME] WHOLE FUNCTION ------>', timedelta_format(datetime.datetime.now(), time_start_f))


def wait_until_element_is_located(browser, inp):
    """
    Waits until certain element is located on page and then clicks on it.
        :param browser: Selenium driver, browser instance
        :param inp, list, inputs from launcher (xpath)

    Note: click() method may be replaced with another
    """

    xpath = inp[0]

    try:
        WebDriverWait(browser, 5).until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
    except Exception as e:
        print('Error while locating element', e)


def get_current_url(url, inp):
    """
    Returns the URL of current opened website
        :param page: string, URL of opened page from current driver instance
        :param inp: list, inputs from launcher (filename)
    """

    filename = inp[0]
    try:
        with open(filename, 'w+', encoding="utf-8") as f:
            f.write(url)
    except Exception:
        print('Error while getting current URL!')


def close_browser(browser):
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

    except ConnectionRefusedError:
        pass
    except Exception:
        print('Error while closing the browser!')


def scroll_web_page(browser, inp):
    """
    Scrolls page up / down by n-pixels.
        :param browser: driver instance
        :param inp: list, inputs from launcher (scroll_to, scroll_by)
    """

    scroll_to = inp[0]
    scroll_by = inp[1]

    if scroll_to == 'Down':
        if scroll_by == 'max':
            script = 'window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;'
        else:
            try:
                #script = f'window.scrollBy(0, {scroll_by});'       # instant scrolling
                script = f'window.scrollBy({{top: {scroll_by}, left: 0, behavior: "smooth"}});'     # smooth scrolling
            except:
                pass
    elif scroll_to == 'Up':
        if scroll_by == 'max':
            script = 'window.scrollTo(0, 0)'
        else:
            try:
                #script = f'window.scrollBy(0, -{scroll_by});'      # instant scrolling
                script = f'window.scrollBy({{top: -{scroll_by}, left: 0, behavior: "smooth"}});'    # smooth scrolling
            except:
                pass

    browser.execute_script(script)


def click_xpath(browser, xpath):
    browser.find_element_by_xpath(xpath).click()


def extract_xpath(page, inp):
    """
    write_in_file_mode ... w+, a+
    """
    xpath = inp[0]
    filename = inp[1]  # "extracted_data.txt"
    try:
        write_in_file_mode = inp[2]
    except:
        write_in_file_mode = "w+"
    data = page.xpath(xpath).extract()

    if not data:
        data = ['EmptyElement']

    # print("DATA",data)
    with open(filename, write_in_file_mode, encoding="utf-8") as f:
        for i, row in enumerate(data):
            # print("B",i,row)
            row = row.strip()

            if row:
                f.write(row.strip() + "\n")
        # print("C")
    # print(data)


def extract_multiple_xpaths(page, inp):
    print("PAGE", page, "INP", inp)
    result = []
    xpaths = inp[0]
    filename = inp[1]  # "extracted_data.txt"
    for i, xpath in enumerate(xpaths):
        data = page.xpath(xpath).extract()
        print("data", data)
        result.append(data)

    short_filename = filename.split(".txt")[0]
    df = pd.DataFrame(result)
    df.to_excel(short_filename + ".xlsx")

    data = result

    print("DATA", data)
    with open(filename, "w+", encoding="utf-8") as f:
        pass


def extract_table_xpath(page, inp):
    row_xpath = inp[0]
    column_xpath = inp[1]
    filename = inp[2]  # "extracted_data.txt"

    result = []
    trs = page.xpath(row_xpath)
    for j, tr in enumerate(trs):
        details = page.xpath(row_xpath + "[" + str(j) + "]" + column_xpath).extract()
        print(j, details)

        result.append(details)

    short_filename = filename.split(".txt")[0]
    df = pd.DataFrame(result)
    df.to_excel(short_filename + ".xlsx")

    data = result

    print("DATA", data)
    with open(filename, "w+", encoding="utf-8") as f:
        pass
        # for i,row in enumerate(data):
        # print("B",i,row)
        # f.write(row+"\n")
        # print("C")
    # print(data)


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

    # capabilities = DesiredCapabilities.FIREFOX
    # capabilities["marionette"] = True
    # browser=webdriver.Firefox(capabilities=capabilities)
    # browser.set_window_size(1820, 980)

    def __init__(self):
        # can be replaced for debugging with browser = webdriver.FireFox()
        # self.browser = webdriver.PhantomJS(executable_path=PHANTOMJS_PATH, service_args=['--ignore-ssl-errors=true'])
        capabilities = DesiredCapabilities.FIREFOX
        capabilities["marionette"] = True

        options = FirefoxOptions()
        window_size_x = 1820

        try:
            bool_scrape_in_browser = load_variable_safe('scr_vars.kpv', 'bool_scrape_in_browser')['in_browser']
        except Exception as e:
            print('Error while loading bool_scrape_in_browser!', e)
            bool_scrape_in_browser = True

        if not bool_scrape_in_browser:
            options.add_argument("--headless")

            # For headless mode different width of window is needed
            window_size_x = 1450

        self.browser = webdriver.Firefox(options=options, capabilities=capabilities)

        self.browser.set_window_size(window_size_x, 980)
        self.start_requests()


    def __del__(self):
        self.browser.quit()

    def start_requests(self):
        URLS = ['https://www.forloop.ai']
        FUNCTIONS = [self.parse]
        for i in range(len(URLS)):
            yield scrapy.Request(url=URLS[i], callback=FUNCTIONS[i])  # yield

    def parse(self, response):
        global spider_functions
        try:
            global browser_pid
            browser_pid = self.browser.capabilities['moz:processID']
            browser_pid = VarSafe(browser_pid, "browser_pid", "browser_pid")
            save_variables(kept_variables, "scr_vars.kpv")
            print(browser_pid)
        except Exception as e:
            print(e)
        self.browser.get(response.url)
        global docrawl_core_done
        docrawl_core_done = False
        page = Selector(text=self.browser.page_source)

        while not docrawl_core_done:
            try:
                spider_requests = load_variable_safe("scr_vars.kpv", "spider_requests")
                # print("LOADED REQUESTS",spider_requests)
            except Exception as e:
                spider_requests = {"url": "www.forloop.ai", "loaded": True}
                spider_requests = VarSafe(spider_requests, "spider_requests", "spider_requests")
                # print("LOADED REQUESTS - EXCEPTION",e)
            try:
                spider_functions = load_variable_safe("scr_vars.kpv", "spider_functions")
            except:
                spider_functions = {"function": "print", "input": "Warning: function not given to docrawl",
                                    "done": False}
                spider_functions = VarSafe(spider_functions, "spider_functions", "spider_functions")
            try:
                time.sleep(1)
                print("Docrawl core loop")
                print(spider_functions)
                # print(docrawl_core_done)
                # print(spider_requests['loaded'])
                if not spider_requests['loaded']:
                    # print(spider_requests['url'])
                    self.browser.get(spider_requests['url'])
                    page = Selector(text=self.browser.page_source)

                    spider_requests['loaded'] = True
                    spider_requests = VarSafe(spider_requests, "spider_requests", "spider_requests")
                    # print(spider_requests['loaded'],"spider_requests")
                    save_variables(kept_variables, "scr_vars.kpv")

                if spider_functions['done'] == False:

                    # print("AAA",spider_functions)
                    # try:

                    function_str = spider_functions['function']
                    function = eval(function_str)
                    # print("BBB",function,function_str)

                    print("INPUT", spider_functions['input'])

                    inp = spider_functions['input']  # .replace("$","'").replace('€','"')
                    print("INP", inp)

                    if function_str == "click_xpath":
                        print("CLICK XPATH")
                        click_xpath(self.browser, inp)
                    elif function_str == "extract_xpath":
                        print("EXTRACT XPATH")
                        extract_xpath(page, inp)
                    elif function_str == "extract_multiple_xpaths":
                        print("EXTRACT MULTIPLE XPATH")
                        extract_multiple_xpaths(page, inp)
                    elif function_str == "extract_table_xpath":
                        print("EXTRACT XPATH")
                        extract_table_xpath(page, inp)
                    elif function_str == "get_current_url":
                        print("GET CURRENT URL")
                        get_current_url(str(self.browser.current_url), inp)
                    elif function_str == "scan_web_page":
                        print("SCAN WEB PAGE")
                        scan_web_page(page, inp, self.browser)
                    elif function_str == "close_browser":
                        print("CLOSE BROWSER")
                        close_browser(self.browser)
                    elif function_str == "extract_page_source":
                        print("EXTRACT PAGE SOURCE")
                        extract_page_source(self.browser, inp)
                    elif function_str == "wait_until_element_is_located":
                        print("WAIT UNTIL ELEMENT IS LOCATED")
                        wait_until_element_is_located(self.browser, inp)
                    elif function_str == "take_screenshot":
                        print("TAKE PAGE SCREENSHOT")
                        take_screenshot(self.browser, inp)
                    elif function_str == "scroll_web_page":
                        print("SCROLL WEB PAGE")
                        scroll_web_page(self.browser, inp)
                    else:
                        function(inp)

                    # except Exception as e:
                    #    print("Exception occurred:",e)
                    # print("A")
                    spider_functions['done'] = True
                    # print("B")
                    spider_functions = VarSafe(spider_functions, "spider_functions", "spider_functions")
                    # print("C")
                    save_variables(kept_variables, "scr_vars.kpv")
                    # print("D")
                page = Selector(text=self.browser.page_source)
                # save_variables(kept_variables,"scr_vars.kpv")
                # print("ABCDEFGHIJ",docrawl_core_done)
            except KeyboardInterrupt:
                # print("BLABLA")
                break
