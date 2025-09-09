        

from crochet import setup
from scrapy.crawler import CrawlerRunner
from docrawl.docrawl_core import DocrawlSpider

#setup()
#crawler = CrawlerRunner()
#crawler.crawl(DocrawlSpider, docrawl_client=None)


from selenium.webdriver import ChromeOptions, FirefoxOptions

from selenium.webdriver.firefox.service import Service

from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager

from selenium import webdriver

options = FirefoxOptions()
options.set_preference("marionette", True)

#options.add_argument("--headless")
# For headless mode different width of window is needed
window_size_x = 1450

try:
    service = Service(GeckoDriverManager().install())
except Exception as e:
    service = None
    print(
        "GeckoDriverManager update was not successful - launching latest Firefox version instead"
        + str(e)
    )

try:
    browser = webdriver.Firefox(
        options=options, service=service
    )
except Exception as e:
    print(f'Error while creating Firefox instance {e}')
    browser = webdriver.Firefox(options=options)


browser.get(f"https://www.forloop.ai/blog")

