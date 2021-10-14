import docrawl_pkg.docrawl.docrawl_core as docrawl_core
from scrapy.crawler import CrawlerRunner
from crochet import setup
import keepvariable.keepvariable_core as kv

def load_website(url):
    docrawl_core.spider_requests={"url":url,"loaded":False}

def print_xpath(args):
    #function = "exec"
    if type(args)==list:
        command = args[0]
    else:
        command = args
        
        #command=command.replace("'","$")
        #command=command.replace('"','€')
                                
    #inp = "print(page.xpath('" + command + "').extract())"    
    #print("INPUT",inp)
    
    function="extract_xpath"
    inp=command

    
    spider_functions=kv.Var({"function": function, "input": inp, "done": False})
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

    spider_functions=kv.Var({"function":function,"input":inp,"done":False})
    kv.save_variables(kv.kept_variables,"scr_vars.kpv")


def click_name(args):
    function = "exec"
    print(args)
    
    
    if type(args)==list:
        command = args[0]
    else:
        command = args
    inp = "self.browser.find_element_by_link_text('" + command + "').click()"

    spider_functions=kv.Var({"function": function, "input": inp, "done": False})
    kv.save_variables(kv.kept_variables,"scr_vars.kpv")

    #docrawl_core.spider_requests={"url":url,"loaded":False}


def run_spider(number):
    setup()
    crawler = CrawlerRunner()
    crawler.crawl(docrawl_core.DocrawlSpider)
