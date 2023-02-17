import docrawl.docrawl_core as docrawl_core
from scrapy.crawler import CrawlerRunner
from crochet import setup
import keepvariable.keepvariable_core as kv
import time


def load_website(url):
    if "http" not in url:
        url = "http://" + url

    spider_requests = {"url": url, "loaded": False}
    spider_requests = kv.VarSafe(spider_requests, "spider_requests", "spider_requests")
    print("SPIDER_REQUESTS", spider_requests)
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")

    # docrawl_core.spider_requests={"url":url,"loaded":False}


def take_screenshot(filename):
    """
    Launches take_screenshot from core.
        :param filename: string, output filename (where to save the screenshot)
    """

    print('LAUNCHER', 'Taking screenshot')

    function = "take_screenshot"

    inp = [filename]

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")


def extract_page_source(filename):
    """
    Launches extract_page_source from core.
        :param filename: string, name of file that will be used for storing page source
    """

    print('LAUNCHER', 'Extracting page source')

    function = "extract_page_source"

    inp = [filename]

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")


def scan_web_page(incl_tables=False, incl_bullets=False, incl_texts=False, incl_headlines=False, incl_links=False,
                  incl_images=False, incl_buttons=False, by_xpath=None, context_xpath=None,
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

    print("LAUNCHER", "Scaning web page")

    function = "scan_web_page"

    inp = [incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links,
           incl_images, incl_buttons, by_xpath, context_xpath, output_folder]

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")


def wait_until_element_is_located(xpath):
    """
    Launches wait_until_element_is_located function from core.
        :param xpath: str, xpath of element to be located
    """

    print("LAUNCHER", "Waiting until element is located")

    function = "wait_until_element_is_located"

    inp = [xpath]

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")


def get_current_url(filename):
    """
    Launches get_current_url function from core.
        :param filename: string, name of file that will be used for storing the URL
    """

    print("LAUNCHER", "Current URL extract")

    function = "get_current_url"
    inp = [filename]

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")


def close_browser():
    """
    Launches close_browser function from core.
    """

    print("LAUNCHER", "Close browser")

    function = "close_browser"
    spider_functions = {"function": function, "input": None, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")
    # time.sleep(3)  # Without delay function is not transferred to docrawl_core


def scroll_web_page(scroll_to, scroll_by, scroll_max):
    """
    Launches scroll_web_page function from core.
        :param scroll_to: string, scroll direction (Up/Down)
        :param scroll_by: int, scroll distance
        :param scroll_max: bool, scroll to maximum
    """

    print("LAUNCHER", "Scroll web page")

    function = "scroll_web_page"
    inp = [scroll_to, scroll_by, scroll_max]

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")


def download_images(image_xpath, filename):
    """
    Launches download_image function from core.
        :param image_xpath: string, url of image
        :param filename: string, output filename
    """

    print("LAUNCHER", "Download images")

    function = "download_images"
    inp = [image_xpath, filename]

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")


def extract_xpath(xpath, filename, write_in_file_mode="w+"):
    print("LAUNCHER", xpath, filename)
    args = xpath
    if type(args) == list:
        command = args[0]
    else:
        command = args

    command = [xpath, filename, write_in_file_mode]

    function = "extract_xpath"
    inp = command

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")


def extract_multiple_xpath(xpaths, filename="extracted_data.xlsx"):
    # function = "exec"
    print("DOCRAWL LAUNCHER - extract_multiple_xpath")

    args = xpaths
    if type(args) == list:
        command = args[0]
    else:
        command = args

    command = [xpaths, filename]
    # command=command.replace("'","$")
    # command=command.replace('"','€')

    # inp = "print(page.xpath('" + command + "').extract())"
    # print("INPUT",inp)

    function = "extract_multiple_xpaths"
    inp = command

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")

    # docrawl_core.spider_requests={"url":url,"loaded":False}


def extract_table_xpath(xpath_row, xpath_col, first_row_header, filename="extracted_data.xlsx"):
    # function = "exec"
    args = xpath_row
    if type(args) == list:
        command = args[0]
    else:
        command = args

    command = [xpath_row, xpath_col, filename, first_row_header]
    # command=command.replace("'","$")
    # command=command.replace('"','€')

    # inp = "print(page.xpath('" + command + "').extract())"
    # print("INPUT",inp)

    function = "extract_table_xpath"
    inp = command

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")

    # docrawl_core.spider_requests={"url":url,"loaded":False}


def click_xpath(xpath):
    # function = "exec"
    print(xpath)

    '''
    OLD IMPLEMENTATION 

    if type(args) == list:
        command = args[0]
    else:
        command = args
    inp = "self.browser.find_element_by_xpath('" + command + "').click()"

    function = "click_xpath"
    inp = command
    '''

    function = "click_xpath"
    inp = [xpath]

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")


def click_name(args):
    function = "exec"
    print(args)

    if type(args) == list:
        command = args[0]
    else:
        command = args
    inp = "self.browser.find_element_by_link_text('" + command + "').click()"

    spider_functions = {"function": function, "input": inp, "done": False}
    spider_functions = kv.VarSafe(spider_functions, "spider_functions", "spider_functions")
    kv.save_variables(kv.kept_variables, "scr_vars.kpv")

    # docrawl_core.spider_requests={"url":url,"loaded":False}


def run_spider(number, in_browser=True, driver='Firefox'):
    """
    Starts crawler.
        :param in_browser: bool, show browser GUI (-headless option).
        :param driver: string, driver instance to use (Firefox/Geckodriver, Chrome)

        Note: param in_browser is reverse to -headless option, meaning:
            - in_browser=True -> no -headless in driver options
            - in_browser=False -> add -headless to driver options
    """

    setup()
    crawler = CrawlerRunner()
    browser = {'in_browser': in_browser, 'driver': driver}
    browser = kv.VarSafe(browser, "browser", "browser")

    kv.save_variables(kv.kept_variables, 'scr_vars.kpv')

    time.sleep(1)

    crawler.crawl(docrawl_core.DocrawlSpider)