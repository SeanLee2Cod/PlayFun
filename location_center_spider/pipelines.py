# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from twisted.enterprise import adbapi


class LocationCenterSpiderPipeline(object):
    def process_item(self, item, spider):
        return item


class MysqlTwistedPipeline(object):
    #网络下载速度和数据库I/O速度不一样,为了避免线程的阻塞,进行异步的插入
    def __init__(self,dbpool):
        self.dbpool = dbpool

    @classmethod
    #cls 指的就是MysqlTwistedPipeline这个类
    def from_settings(cls, settings):
        #先将setting中连接数据库所需内容取出，构造一个地点
        dbparms = dict(
            host = settings["MYSQL_HOST"],
            db = settings["MYSQL_DBNAME"],
            user = settings["MYSQL_USER"],
            passwd = settings["MYSQL_PASSWORD"],
            charset = 'utf8mb4',
            cursorclass =pymysql.cursors.DictCursor,
            use_unicode = True
        )
        # **可变化参数
        dbpool = adbapi.ConnectionPool("pymysql", **dbparms)
        return cls(dbpool)

    def process_item(self, item, spider):
        #使用twisted将mysql插入变成异步执行
        query = self.dbpool.runInteraction(self.do_update,item)
        #出现错误时调用
        query.addErrback(self.handle_error, item, spider)  #处理异常

    def handle_error(self, failure, item, spider):
        #处理异步插入的异常
        print (failure)

    def do_update(self, cursor, item):
        update_sql = item.get_update_org_info_sql()
        cursor.execute(update_sql, (item['extract_org_name'], item['org_type'], item['nation'], item['state'], item['city'], item['longtitude'], item['latitude'], item['org_id']))