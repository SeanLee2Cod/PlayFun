# -*- coding:utf-8 -*-
'''
@author: SeanLee
@license: (C)Copyright 
'''
import pymysql


class Statistic():

    def __init__(self):
        '''
            建立数据库连接
        '''

        self.conn = pymysql.connect('localhost', 'root', 'password',
                                   'dblp_extract', charset='utf8mb4',
                                   use_unicode=True)
        self.cur = self.conn.cursor()
        query_sql = 'select count(author_id) from author_org group by org_name'
        self.cur.execute(query_sql)

    def count(self):
        a = [0 for i in range(200)]
        data = self.cur.fetchall()
        for tumple in data:
            number = int(tumple[0])
            for i in range(1, 200):
                if number == i:
                    a[i] += 1
                    break;
        for j in range(1, len(a)):

            print("机构中的作者数为%s的有%s个占总机构的百分之%s"%(j, a[j], a[j]/31705))


if __name__ =='__main__':
    static = Statistic()
    static.count()