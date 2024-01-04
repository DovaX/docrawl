import docrawl.docrawl_client as dc

from itertools import product

import dogui.dogui_core as dg
client=dc.DocrawlClient()







def open_browser():
    client.run_spider(driver="Firefox",in_browser=False)


def load_website():
    url=entry1.text.get()
    client.load_website(url)

def take_png_screenshot():
    client.take_png_screenshot("screenshot.png")
    
def detect_cookies_xpath_preparation():

    #if glc.browser_view1.img is None:
    #    return

    print('Trying to detect cookies popup window')

    # In which attributes to search in
    attributes = ('class', 'id', 'name', 'value', 'href')

    # What values to search for
    values = ('accept-none', 'accept-all', 'acceptAll', 'AcceptAll', 'acceptall', 'acceptAllCookies'
              'deny-all', 'DenyAll', 'denyAll', 'denyall',
              'reject-all', 'RejectAll', 'rejectAll', 'rejectall',
              'allowall', 'allowAll', 'AllowAll', 'allowAllCookies'
              'Allow All', 'Allow all', 'allow all', 'Accept All', 'Accept all', 'accept all')
              #'Cookie', 'cookie', 'Cookies', 'cookies')

    # Possible button text
    text_options = ('Accept all', 'Accept All', 'Accept', 'Accept cookies', 'Accept Cookies', 'Accept all cookies',
                    'Allow all', 'Allow All', 'Allow', 'Allow cookies', 'Allow Cookies',
                    'Agree', 'I agree', 'I Agree',
                    'Consent', 'consent', 'Přijmout vše', 'Souhlasím', 'Prijať všetko')

    attributes_value_combinations = list(product(attributes, values))

    # TODO: not always button is as "button" tag -> need to extend
    xpath = '//*[('

    # First part of XPath (searching for mark words in attributes)
    for combination in attributes_value_combinations:
        attribute, value = combination
        xpath += f'contains(@{attribute}, "{value}") or '

    # Second part of XPath (searching for button text)
    for text_option in text_options[:-1]:
        xpath += f'*[contains(text(), "{text_option}")] or '

    xpath += f'*[contains(text(), "{text_options[-1]}")]) and '

    xpath += '(self::button or self::a)]'

    print(xpath)
    return(xpath)
    
    
    
def scan_webpage():
    xpath = detect_cookies_xpath_preparation()
    client.scan_web_page(incl_tables=True, incl_bullets=True, incl_texts=True,
                                       incl_headlines=True, incl_links=True, incl_images=True,
                                       incl_buttons=True, by_xpath=None, cookies_xpath=xpath)
    

def extract_xpath():
    client.extract_xpath("/html/body/div[4]/div/div/div/div/div[1]/a/div/h4", "blabla.txt")


def close_browser():
    client.close_browser()



gui1=dg.GUI()


entry1=dg.Entry(gui1.window,2,2,text_input="www.forloop.ai/blog",width=40)
dg.Button(gui1.window,"Open browser",open_browser,1,1)
dg.Button(gui1.window,"Load website",load_website,2,1)
dg.Button(gui1.window,"Take screenshot",take_png_screenshot,3,1)
dg.Button(gui1.window,"Scan webpage",scan_webpage,4,1)
dg.Button(gui1.window,"Extract Xpath",extract_xpath,5,1)
dg.Button(gui1.window,"Close browser",close_browser,6,1)





gui1.build_gui()