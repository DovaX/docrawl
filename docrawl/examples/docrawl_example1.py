import docrawl.docrawl_client as dc

client=dc.DocrawlClient()
client.run_spider(driver="Chrome",in_browser=False)
client.load_website("www.forloop.ai/blog")
client.extract_xpath("/html/body/div[4]/div/div/div/div/div[1]/a/div/h4", "blabla.txt")
client.close_browser()
