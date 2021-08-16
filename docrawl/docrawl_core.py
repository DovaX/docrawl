#import sys #PATH initialization of modules
#try:
#    sys.path.append("C:\\Users\\EUROCOM\\Documents\\Git\\DovaX")
#except:
#    pass

import platform
import scrapy
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
import pynput.keyboard
keyboard = pynput.keyboard.Controller()
key = pynput.keyboard.Key

from scrapy.selector import Selector
from keepvariable.keepvariable_core import Var,kept_variables,save_variables,load_variable



spider_requests={"url":"www.forloop.ai","loaded":True}


spider_functions=Var({"function":"print","input":"Bla","done":False})

browser_pid=None
docrawl_core_done=False


def click_class(browser,class_input,index=0,tag="div",wait1=1):
    name_input = browser.find_elements_by_xpath('//'+tag+'[@class="'+class_input+'"]')
    name_input[index].click()
    time.sleep(wait1)
    return(name_input)

if platform.system() == 'Windows':
    PHANTOMJS_PATH = './phantomjs/bin/phantomjs.exe'
else:
    PHANTOMJS_PATH = './phantomjs/bin/phantomjs'
LOGIN=False

def print_special(inp):
    """prints and saves the output to kv.kept_variables"""
    print(inp)
    inp=Var(inp)
    
    save_variables({"inp":inp},filename="input.kpv")
    


class DocrawlSpider(scrapy.spiders.CrawlSpider):
    name = "inspider"
    #allowed_domains = ['google.com']

    custom_settings = {
        'LOG_LEVEL' : 'ERROR',
        'USER_AGENT' : "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
        'DEFAULT_REQUEST_HEADERS' : {
            'Referer': 'https://new.instatscout.com/login'
        }
        #   'CONCURRENT_REQUESTS' : '20',
    }
    #capabilities = DesiredCapabilities.FIREFOX
    #capabilities["marionette"] = True
    #browser=webdriver.Firefox(capabilities=capabilities)
    #browser.set_window_size(1820, 980)
        
    def __init__(self):
        # can be replaced for debugging with browser = webdriver.FireFox()
        #self.browser = webdriver.PhantomJS(executable_path=PHANTOMJS_PATH, service_args=['--ignore-ssl-errors=true'])
        capabilities = DesiredCapabilities.FIREFOX
        capabilities["marionette"] = True
        self.browser=webdriver.Firefox(capabilities=capabilities)
        self.browser.set_window_size(1820, 980)          
        self.start_requests()
        
        
    def __del__(self):
        self.browser.quit()

    def start_requests(self):
        URLS=['https://www.forloop.ai']
        FUNCTIONS=[self.parse]
        for i in range(len(URLS)):
            yield scrapy.Request(url=URLS[i], callback=FUNCTIONS[i]) #yield 
                        
    def parse(self,response):
        global spider_functions
        try:
            global browser_pid
            browser_pid=self.browser.capabilities['moz:processID']
            browser_pid=Var(browser_pid)
            save_variables(kept_variables)
            print(browser_pid)
        except Exception as e:
            print(e)
        self.browser.get(response.url)
        global docrawl_core_done
        docrawl_core_done=False
        page=Selector(text=self.browser.page_source)
        
        while not docrawl_core_done:
            try:
                spider_functions=load_variable()
            except:
                spider_functions=Var({"function":"print","input":"Warning: function not given to docrawl","done":False})
            try:
                time.sleep(1)
                #print("Docrawl core loop")
                #print(docrawl_core_done)
                #print(spider_requests['loaded'])
                if not spider_requests['loaded']:
                    print(spider_requests['url'])
                    self.browser.get(spider_requests['url'])
                    
                    spider_requests['loaded']=True
                    print(spider_requests['loaded'],"spider_requests")
                    #save_variables(spider_requests)
                
                if spider_functions['done']==False:
                    
                    #print("AAA",spider_functions)
                    try:    
                        function=eval(spider_functions['function'])
                        #print("BBB",function,spider_functions['input'])
                        function(spider_functions['input'])
                    except Exception as e:
                        print("Exception occurred:",e)
                    spider_functions['done']=True
                    spider_functions=Var(spider_functions)
                    save_variables(kept_variables)
                page=Selector(text=self.browser.page_source)
                #save_variables(kept_variables)
                #print("A",docrawl_core_done)
            except KeyboardInterrupt:
                break