import scrapy


class WebsiteItem(scrapy.Item):
    website = scrapy.Field()
    emails = scrapy.Field()
    phones = scrapy.Field()
    vcard_links = scrapy.Field()
    pdf_links = scrapy.Field()
    image_links = scrapy.Field()
