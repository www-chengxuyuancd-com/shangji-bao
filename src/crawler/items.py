import scrapy


class SearchResultItem(scrapy.Item):
    title = scrapy.Field()
    url = scrapy.Field()
    source_type = scrapy.Field()
    region_name = scrapy.Field()
    publish_date = scrapy.Field()
    html = scrapy.Field()
