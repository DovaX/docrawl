import docrawl.docrawl_core as docrawl_core
from scrapy.crawler import CrawlerRunner
from crochet import setup
import keepvariable.keepvariable_core as kv
import time

def load_website(url):
    
    spider_requests={"url":url,"loaded":False}
    spider_requests=kv.VarSafe(spider_requests,"spider_requests","spider_requests")
    print("SPIDER_REQUESTS",spider_requests)
    kv.save_variables(kv.kept_variables,"scr_vars.kpv")
    
    #docrawl_core.spider_requests={"url":url,"loaded":False}


def scan_web_page(incl_tables, incl_bullets, output_dir):
    """
    Launches find_tables function from core.
        :param incl_tables: boolean, search for tables
        :param incl_bullets: boolean, search for bullet lists
        :param output_dir: string, path to output directory <---- MAY BE REMOVED LATER
    """

    print("LAUNCHER", "Scaning web page")

    function = "scan_web_page"

    inp = [incl_tables, incl_bullets, output_dir]

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


def extract_xpath(xpath,filename="extracted_data.xlsx",write_in_file_mode="w+"):
    #function = "exec"
    print("LAUNCHER",xpath,filename)
    args=xpath
    if type(args)==list:
        command = args[0]
    else:
        command = args
        
        
    command=[xpath,filename,write_in_file_mode]
        #command=command.replace("'","$")
        #command=command.replace('"','€')
                                
    #inp = "print(page.xpath('" + command + "').extract())"    
    #print("INPUT",inp)
    
    function="extract_xpath"
    inp=command

    spider_functions={"function": function, "input": inp, "done": False}
    spider_functions=kv.VarSafe(spider_functions,"spider_functions","spider_functions")
    kv.save_variables(kv.kept_variables,"scr_vars.kpv")
    
    #docrawl_core.spider_requests={"url":url,"loaded":False}


def extract_multiple_xpath(xpaths,filename="extracted_data.xlsx"):
    #function = "exec"
    print("DOCRAWL LAUNCHER - extract_multiple_xpath")
    
    args=xpaths
    if type(args)==list:
        command = args[0]
    else:
        command = args
        
        
    command=[xpaths,filename]
        #command=command.replace("'","$")
        #command=command.replace('"','€')
                                
    #inp = "print(page.xpath('" + command + "').extract())"    
    #print("INPUT",inp)
    
    function="extract_multiple_xpaths"
    inp=command

    
    
    spider_functions={"function": function, "input": inp, "done": False}
    spider_functions=kv.VarSafe(spider_functions,"spider_functions","spider_functions")
    kv.save_variables(kv.kept_variables,"scr_vars.kpv")
    
    #docrawl_core.spider_requests={"url":url,"loaded":False}


def extract_table_xpath(xpath_row,xpath_col,filename="extracted_data.xlsx"):
    #function = "exec"
    args=xpath_row
    if type(args)==list:
        command = args[0]
    else:
        command = args
        
        
    command=[xpath_row,xpath_col,filename]
        #command=command.replace("'","$")
        #command=command.replace('"','€')
                                
    #inp = "print(page.xpath('" + command + "').extract())"    
    #print("INPUT",inp)
    
    function="extract_table_xpath"
    inp=command

    
    
    spider_functions={"function": function, "input": inp, "done": False}
    spider_functions=kv.VarSafe(spider_functions,"spider_functions","spider_functions")
    kv.save_variables(kv.kept_variables,"scr_vars.kpv")
    
    #docrawl_core.spider_requests={"url":url,"loaded":False}


def click_xpath(args):
    #function = "exec"
    print(args)
    
    
    if type(args)==list:
        command = args[0]
    else:
        command = args
    inp = "self.browser.find_element_by_xpath('" + command + "').click()"
    

    function="click_xpath"
    inp=command


    
    spider_functions={"function": function, "input": inp, "done": False}
    spider_functions=kv.VarSafe(spider_functions,"spider_functions","spider_functions")
    kv.save_variables(kv.kept_variables,"scr_vars.kpv")


def click_name(args):
    function = "exec"
    print(args)
    
    
    if type(args)==list:
        command = args[0]
    else:
        command = args
    inp = "self.browser.find_element_by_link_text('" + command + "').click()"


    spider_functions={"function": function, "input": inp, "done": False}
    spider_functions=kv.VarSafe(spider_functions,"spider_functions","spider_functions")
    kv.save_variables(kv.kept_variables,"scr_vars.kpv")

    #docrawl_core.spider_requests={"url":url,"loaded":False}


def run_spider(number, in_browser=True):
    """
    Starts crawler.
        :param in_browser: bool, show browser GUI (-headless option).

        Note: param in_browser is reverse to -headless option, meaning:
            - in_browser=True -> no -headless in driver options
            - in_browser=False -> add -headless to driver options
    """

    setup()
    crawler = CrawlerRunner()

    bool_scrape_in_browser = {'in_browser': in_browser}
    bool_scrape_in_browser = kv.VarSafe(bool_scrape_in_browser, "bool_scrape_in_browser", "bool_scrape_in_browser")
    kv.save_variables(kv.kept_variables, 'scr_vars.kpv')

    time.sleep(1)

    crawler.crawl(docrawl_core.DocrawlSpider)