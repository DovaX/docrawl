import docrawl.docrawl_core as docrawl_core
from scrapy.crawler import CrawlerRunner
from crochet import setup

def load_website(url):
    docrawl_core.spider_requests={"url":url,"loaded":False}

def run_spider(number):
    setup()
    crawler = CrawlerRunner()
    crawler.crawl(docrawl_core.DocrawlSpider)
