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
from keepvariable.keepvariable_core import VarSafe,kept_variables,save_variables,load_variable_safe
import pandas as pd


spider_requests={"url":"www.forloop.ai","loaded":True}


spider_functions={"function":"print","input":"Bla","done":False}
spider_functions=VarSafe(spider_functions,"spider_functions","spider_functions")

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
    inp=VarSafe(inp,"inp","inp")
    
    save_variables({"inp":inp},filename="input.kpv")
    



def click_xpath(browser,xpath):
    browser.find_element_by_xpath(xpath).click()
    
def extract_xpath(page,inp):
    """
    write_in_file_mode ... w+, a+
    """
    
    xpath=inp[0]
    filename=inp[1]#"extracted_data.txt"
    try:
        write_in_file_mode=inp[2]
    except:
        write_in_file_mode="w+"
    data=page.xpath(xpath).extract()
    
    
    
    #print("DATA",data)
    with open(filename,write_in_file_mode,encoding="utf-8") as f:
        for i,row in enumerate(data):
            #print("B",i,row)
            f.write(row+"\n")
        #print("C")
    #print(data)
    
    
    
def extract_multiple_xpaths(page,inp):
    print("PAGE",page,"INP",inp)
    result=[]
    xpaths=inp[0]
    filename=inp[1]#"extracted_data.txt"
    for i,xpath in enumerate(xpaths):
        
        data=page.xpath(xpath).extract()
        print("data",data)
        result.append(data)
    
    

    
    short_filename=filename.split(".txt")[0]
    df=pd.DataFrame(result)
    df.to_excel(short_filename+".xlsx")
    
        
    data=result

         
    
    print("DATA",data)
    with open(filename,"w+",encoding="utf-8") as f:
        pass

   
def extract_table_xpath(page,inp):
    
    row_xpath=inp[0]
    column_xpath=inp[1]
    filename=inp[2]#"extracted_data.txt"
    
    
    result=[]
    trs=page.xpath(row_xpath)
    for j,tr in enumerate(trs):
        details=page.xpath(row_xpath+"["+str(j)+"]"+column_xpath).extract()
        print(j,details)
        
        result.append(details)
    
    short_filename=filename.split(".txt")[0]
    df=pd.DataFrame(result)
    df.to_excel(short_filename+".xlsx")
    
        
    data=result

         
    
    print("DATA",data)
    with open(filename,"w+",encoding="utf-8") as f:
        pass
        #for i,row in enumerate(data):
            #print("B",i,row)
            #f.write(row+"\n")
        #print("C")
    #print(data)
    




class DocrawlSpider(scrapy.spiders.CrawlSpider):
    name = "forloop"
    #allowed_domains = ['google.com']

    custom_settings = {
        'LOG_LEVEL' : 'ERROR',
        'USER_AGENT' : "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
        'DEFAULT_REQUEST_HEADERS' : {
            'Referer': 'https://forloop.ai'
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
            browser_pid=VarSafe(browser_pid,"browser_pid","browser_pid")
            save_variables(kept_variables,"scr_vars.kpv")
            print(browser_pid)
        except Exception as e:
            print(e)
        self.browser.get(response.url)
        global docrawl_core_done
        docrawl_core_done=False
        page=Selector(text=self.browser.page_source)
        
        while not docrawl_core_done:
            try:
                spider_requests=load_variable_safe("scr_vars.kpv","spider_requests")
                #print("LOADED REQUESTS",spider_requests)
            except Exception as e:
                spider_requests={"url":"www.forloop.ai","loaded":True}
                spider_requests=VarSafe(spider_requests,"spider_requests","spider_requests")
                #print("LOADED REQUESTS - EXCEPTION",e)
            try:
                spider_functions=load_variable_safe("scr_vars.kpv","spider_functions")
            except:
                spider_functions={"function":"print","input":"Warning: function not given to docrawl","done":False}
                spider_functions=VarSafe(spider_functions,"spider_functions","spider_functions")
            try:
                time.sleep(1)
                print("Docrawl core loop")
                print(spider_functions)
                #print(docrawl_core_done)
                #print(spider_requests['loaded'])
                if not spider_requests['loaded']:
                    #print(spider_requests['url'])
                    self.browser.get(spider_requests['url'])
                    page=Selector(text=self.browser.page_source)
                    
                    spider_requests['loaded']=True
                    spider_requests=VarSafe(spider_requests,"spider_requests","spider_requests")
                    #print(spider_requests['loaded'],"spider_requests")
                    save_variables(kept_variables,"scr_vars.kpv")
                
                if spider_functions['done']==False:
                    
                    #print("AAA",spider_functions)
                    #try:    
                        
                    function_str=spider_functions['function']
                    function=eval(function_str)
                    #print("BBB",function,function_str)
                    
                    print("INPUT",spider_functions['input'])
                    
                    inp=spider_functions['input']#.replace("$","'").replace('€','"')
                    print("INP",inp)
                    
                    
                    
                    if function_str=="click_xpath":
                        print("CLICK XPATH")
                        click_xpath(self.browser,inp)
                    elif function_str=="extract_xpath":
                        print("EXTRACT XPATH")
                        extract_xpath(page,inp)
                    elif function_str=="extract_multiple_xpaths":
                        print("EXTRACT MULTIPLE XPATH")
                        extract_multiple_xpaths(page,inp)
                    elif function_str=="extract_table_xpath":
                        print("EXTRACT XPATH")
                        extract_table_xpath(page,inp)
                    else:
                        function(inp)
                    
                    #except Exception as e:
                    #    print("Exception occurred:",e)
                    #print("A")
                    spider_functions['done']=True
                    #print("B")
                    spider_functions=VarSafe(spider_functions,"spider_functions","spider_functions")
                    #print("C")
                    save_variables(kept_variables,"scr_vars.kpv")
                    #print("D")
                page=Selector(text=self.browser.page_source)
                #save_variables(kept_variables,"scr_vars.kpv")
                #print("ABCDEFGHIJ",docrawl_core_done)
            except KeyboardInterrupt:
                #print("BLABLA")
                break
            
            
       