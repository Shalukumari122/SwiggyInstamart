# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SwiggyinstamartProductItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()




class SwiggyinstamartLocation(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()
