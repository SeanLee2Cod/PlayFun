# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import logging

import scrapy


class LocationCenterSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    org_id = scrapy.Field()
    extract_org_name = scrapy.Field()
    org_type = scrapy.Field()
    nation = scrapy.Field()
    state = scrapy.Field()
    city = scrapy.Field()
    longtitude = scrapy.Field()
    latitude = scrapy.Field()

    def get_update_org_info_sql(self):
        sql = "update org set extract_org_name = %s, org_type = %s, nation = %s, state = %s, city = %s, longtitude = %s, latitude = %s  where org_id = %s"
        logging.debug("更新sql语句为:%s" % sql)
        return sql