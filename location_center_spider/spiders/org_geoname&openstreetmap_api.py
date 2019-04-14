# -*- coding:utf-8 -*-
'''
@author: SeanLee
@license: (C)Copyright 
'''
import copy
import json
import logging
from collections import OrderedDict

import pymysql
import scrapy
from lxml import etree
from scrapy_redis.spiders import RedisSpider

from location_center_spider.items import LocationCenterSpiderItem


class OrgGeonameXmlAndOpenstreetMapSpider(RedisSpider):
    name = 'org_extract'
    allowed_domains = ['geonames.org']
    start_urls = ['https://www.geonames.org/']
    def __init__(self):
        self.effective_i = 0
        self.total_i = 0
        '''
            建立数据库连接
        '''
        self.conn = pymysql.connect('localhost', 'root', 'password',
                                    'dblp_extract', charset='utf8mb4',
                                    use_unicode=True)
        self.cur = self.conn.cursor()
        query_sql = 'select org_id, org_name from org where extract_org_name is null'
        self.cur.execute(query_sql)

    def parse(self, response):
        '''
        从数据库中取出org_id和org_name,拼接后形成url
        :param response:
        :return:
        '''
        data = self.cur.fetchall()
        for id, name in data:
            name_split = name.split(',')
            if(len(name_split) > 1):
                new_name = name_split[0] + "," + name_split[1]
            else:
                new_name = name_split[0]
            item = LocationCenterSpiderItem()
            item['org_id'] = id
            url = "http://api.geonames.org/search?q="+new_name+"&maxRows=10&style=LONG&lang=es&username=seanlee123"
            yield scrapy.Request(url=url, meta={'item': copy.deepcopy(item), 'name': new_name},
                                 callback=self.parse_xml, dont_filter=True)

    def parse_xml(self, response):
        '''
        从geoname的xml或者是openstreet map中的json文件中提取数据
        :param response:
        :return:
        '''
        total_num = 0
        extract_org_name = None
        org_type =None
        nation = None
        state = None
        longtitude = None
        latitude = None
        item = response.meta['item']
        new_name = response.meta['name']
        xml = etree.XML(response.body)
        try:
            total_num =  int(xml.xpath("totalResultsCount/text()")[0])
        except Exception as e:
            print(e)
        if total_num > 0:
            self.effective_i += 1
            self.total_i += 1
            try:
                extract_org_name = xml.xpath("geoname/toponymName/text()")[0]
            except Exception as e:
                print(e)
            try:
                org_type = xml.xpath("geoname/fcodeName/text()")[0]
            except Exception as e:
                print(e)
            try:
                nation = xml.xpath("geoname/countryName/text()")[0]
            except Exception as e:
                print(e)
            try:
                state = xml.xpath("geoname/adminName1/text()")[0]
            except Exception as e:
                print(e)
            try:
                longtitude = xml.xpath("geoname/lng/text()")[0]
            except Exception as e:
                print(e)
            try:
                latitude = xml.xpath("geoname/lat/text()")[0]
            except Exception as e:
                print(e)
            if extract_org_name != None:
                item['extract_org_name'] = extract_org_name
            else:
                item['extract_org_name'] = "geoname_none"

            if org_type != None:
                item['org_type'] = org_type
            else:
                item['org_type'] = "geoname_none"

            if nation != None:
                item['nation'] = nation
            else:
                item['nation'] = "geoname_none"

            if state != None:
                item['state'] = state
            else:
                item['state'] = "geoname_none"

            if longtitude != None:
                item['longtitude'] = longtitude
            else:
                item['longtitude'] = "geoname_none"

            if latitude != None:
                item['latitude'] = latitude
            else:
                item['latitude'] = "geoname_none"

            #GeoName中没有city相关信息
            item['city'] = 'geoname_none'
            logging.debug("第%s个机构：%s被成功提出"%(self.effective_i, new_name))
            logging.debug("已提取机构中有百分之%s被成功提出" % (self.effective_i/self.total_i))
            yield item
        else:                  #当在GeoName中提取不到数据时，到OpenStreetMap中提取
            url = "https://nominatim.openstreetmap.org/search?q=" + new_name + \
                  "&format=json&addressdetails=1&accept-language=en"

            yield scrapy.Request(url=url, meta={'item': copy.deepcopy(item), 'name': new_name},
                                 callback=self.parse_json, dont_filter=True)

    def parse_json(self, response):
        '''
        从OpenStreetMap中提取数据
        :param response:
        :return:
        '''
        item = response.meta['item']
        new_name = response.meta['name']
        extract_org_name = None
        org_type = None
        nation = None
        state = None
        city = None
        longtitude = None
        latitude = None
        self.total_i += 1
        if response.body != b'[]':
            self.effective_i += 1
            json_str = response.body
            json_str = json.loads(json_str.decode('utf-8'), object_pairs_hook=OrderedDict)
            try:
                latitude = json_str[0].get('lat')
            except Exception as e:
                print(e)
            try:
                longtitude = json_str[0].get('lon')
            except Exception as e:
                print(e)
            try:
                extract_org_name = list(json_str[0].get('address').values())[
                0]  # 取address字典的第一个值为extract_name, 但是key名不确定
            except Exception as e:
                print(e)
            try:
                nation = json_str[0].get('address').get('country')
            except Exception as e:
                print(e)
            try:
                city = json_str[0].get('address').get('city')
            except Exception as e:
                print(e)
            try:
                state = json_str[0].get('address').get('state')
            except Exception as e:
                print(e)

            if extract_org_name != None:
                item['extract_org_name'] = extract_org_name
            else:
                item['extract_org_name'] = "OpenStreetMap_none"

            if org_type != None:
                item['org_type'] = org_type
            else:
                item['org_type'] = "OpenStreetMap_none"

            if nation != None:
                item['nation'] = nation
            else:
                item['nation'] = "OpenStreetMap_none"

            if state != None:
                item['state'] = state
            else:
                item['state'] = "OpenStreetMap_none"

            if city != None:
                item['city'] = city
            else:
                item['city'] = "OpenStreetMap_none"

            if longtitude != None:
                item['longtitude'] = longtitude
            else:
                item['longtitude'] = "OpenStreetMap_none"

            if latitude != None:
                item['latitude'] = latitude
            else:
                item['latitude'] = "OpenStreetMap_none"
            logging.debug("第%s个机构：%s被成功提出" % (self.effective_i, new_name))
            logging.debug("已提取机构中有百分之%s被成功提出" % (self.effective_i / self.total_i))
            yield  item

        else:
            item['extract_org_name'] = "OpenStreetMap_none"
            item['org_type'] = "OpenStreetMap_none"
            item['nation'] = "OpenStreetMap_none"
            item['state'] = "OpenStreetMap_none"
            item['city'] = "OpenStreetMap_none"
            item['longtitude'] = "OpenStreetMap_none"
            item['latitude'] = "OpenStreetMap_none"
            yield item






