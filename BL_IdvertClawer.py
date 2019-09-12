# -*- coding: utf-8 -*-
import datetime
import json
import os
import requests,re,random,time
from PIL import Image
from selenium import webdriver
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.chrome.options import Options

from  mysql_utils.mysql_db import MysqlDb
from tengxun_OCR import Ocr
from hyper.contrib import HTTP20Adapter
import warnings
warnings.filterwarnings('ignore')



class IdvertClawer():

    def __init__(self):
        self.mysql = MysqlDb()
        pass

    def __request_data__(self, language, pageNo, lastSeenDate):

        headers = {
            "POST": "https://e-com.idvert.com/api_web/ad/list",
            "Origin": "https://e-com.idvert.com",
            "Referer": "https://e-com.idvert.com/advertisings?language=41",
            "HOST": "e-com.idvert.com",
            "Cache-Control": "no-cache",
            "Accept": "application/json,text/plain,*/*",
            # "accept-encoding":"gzip,deflate,br",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "authorization": "0c611b8978d3f7e1f1010d21d6c75ce8.55b35d72d103f2d42cabbc81180c7cd1",
            # "content-length":"28",
            "Content-type": "application/json;charset=UTF-8",
            "platType": "1",
            # "user-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
            "Cookie":"__cfduid=dbf5acb65e32c9ae741e1c18bcb51e7561565239817; _ga=GA1.2.848023312.1565239830; intercom-id-ndcu3xsv=95cb9533-dee7-471b-808d-14040be72f07; _gid=GA1.2.994915302.1565572080; _gat_UA-129333948-3=1; Validate_Code=dYBhlVtoBRVZk5sw9tVhlRo4IJgxY0c4; kjbAuthKey=0c611b8978d3f7e1f1010d21d6c75ce8.55b35d72d103f2d42cabbc81180c7cd1"

        }
        payloadData = {
            "sortField": "default",
            "sortType": "desc",
            "language": language,
            "pageNo": pageNo,
            "lastSeenDate":lastSeenDate
        }

        return headers, payloadData

    def __request__(self, headers, datas):
        try:
            url = 'https://e-com.idvert.com/api_web/ad/list'
            res = requests.post(url, headers=headers, data=json.dumps(datas), timeout=20)
            data = res.json()['resultMap']['data']
        except:
            count = 1
            while count <= 10:
                try:
                    res = requests.post(url, headers=headers, data=json.dumps(datas), timeout=20)
                    data = res.json()['resultMap']['data']
                    break
                except:
                    err_info = '__request__ reloading for %d time' % count if count == 1 else '__request__ reloading for %d times' % count
                    print(err_info)
                    count += 1
            if count > 10:
                print("__request__ job failed!")
        return data

    def __parse__(self, data, language):
        ad_info = {}
        # 广告数据
        json_data = json.dumps(data)
        ad_info['json_data'] = json_data
        # adId
        adId = data['adId']
        ad_info['adId'] = adId
        # 广告链接
        ad_url = 'https://e-com.idvert.com/advertising/' + adId
        ad_info['ad_url'] = ad_url
        # 国家
        country = data['country']
        ad_info['country'] = country
        # 语言
        ad_info['language'] = language
        # ldUrl
        if 'ldUrl' in data.keys():
            ad_info['ldUrl'] = data['ldUrl']
        else:
            ad_info['ldUrl'] = ''
        # 广告信息(列表转换以便存储)
        # adInfoVO = json.dumps(data['adInfoVO'])
        # ad_info['adInfoVO'] = adInfoVO

        # 年龄
        # age = data['age']
        # ad_info['age'] = age
        # 性别
        # gender = data['gender']
        # ad_info['gender'] = gender

        # facebook
        adSourceUrl = data['adSourceUrl']
        ad_info['adSourceUrl'] = adSourceUrl

        ############## 上部信息 ##############
        # 头像url
        picUrl = data['advertiserVO']['picUrl']
        ad_info['picUrl'] = picUrl
        # 名字
        name = data['advertiserVO']['name']
        ad_info['name'] = name
        ############## 中部信息 ##############
        # 信息内容
        message = data['adInfoShowVO']['message']
        ad_info['message'] = message
        # 图片url
        if 'url' in data['adInfoShowVO'].keys():
            url = data['adInfoShowVO']['url']
        else:
            url = ''
        ad_info['url'] = url
        # 标题
        title = data['adInfoShowVO']['title']
        ad_info['title'] = title
        # 描述
        description = data['adInfoShowVO']['description']
        ad_info['description'] = description
        ############## 下部信息 ##############
        # 点赞
        likes = data['likes']
        ad_info['likes'] = likes
        # 评论
        comments = data['comments']
        ad_info['comments'] = comments
        # 分享
        shares = data['shares']
        ad_info['shares'] = shares
        # 持续时间
        days = data['days']
        ad_info['days'] = days
        # launchDays = data['launchDays']
        # 首次时间
        firstSeenDate = data['firstSeenDate'].replace('.', '-')
        firstSeenDate = datetime.datetime.strptime(firstSeenDate, "%Y-%m-%d")  # datetime
        ad_info['firstSeenDate'] = firstSeenDate
        # 最后时间
        lastSeenDate = data['lastSeenDate'].replace('.', '-')
        lastSeenDate = datetime.datetime.strptime(lastSeenDate, "%Y-%m-%d")  # datetime
        ad_info['lastSeenDate'] = lastSeenDate
        ############## 尾部信息 ##############
        domain = data['advertiserVO']['domain']
        if domain:
            do_url = 'https://e-com.idvert.com/advertisings?keyword=' + domain
            ad_info['do_url'] = do_url
        else:
            ad_info['do_url'] = ''

        print('信息已提取：',ad_info['adId'],ad_info['ad_url'])
        return ad_info

    def __query_data__(self, adId):

        sql = 'select id from idvert WHERE ad_id = \'%s\' ' % adId
        res = self.mysql.select(sql)
        if res:
            return True
        else:
            return False

    def __query_lastDate__(self):
        sql = 'SELECT DISTINCT min(lastSeenDate) as lastSeenDate from idvert'
        lastSeenDate = self.mysql.select(sql)[0]['lastSeenDate']
        return lastSeenDate

    def __save_data__(self, ad_info):

        flag = self.__query_data__(ad_info['adId'])
        if not flag:
            sql = '''insert into idvert 
                      (ad_id,ad_url,ld_url,country,language,adSourceUrl,picUrl,name,message,url,title,description,
                      likes,comments,shares,days,firstSeenDate,lastSeenDate,do_url,json_data) 
                      VALUES 
                      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            value = [(ad_info['adId'],ad_info['ad_url'],ad_info['ldUrl'],ad_info['country'],ad_info['language'],ad_info['adSourceUrl'],ad_info['picUrl'],
                      ad_info['name'],ad_info['message'],ad_info['url'],ad_info['title'],ad_info['description'],
                      ad_info['likes'],ad_info['comments'],ad_info['shares'],ad_info['days'],ad_info['firstSeenDate'],
                      ad_info['lastSeenDate'],ad_info['do_url'],ad_info['json_data'])]
            self.mysql.insert(sql,value)
            print('信息已写入：',ad_info['adId'],ad_info['ad_url'])
        else:
            print('信息已存在：', ad_info['adId'], ad_info['ad_url'])

    def __clawer__(self,lastSeenDate):
        lastSeenDate = lastSeenDate.strftime("%Y-%m-%d %H:%M:%S") + ',' + lastSeenDate.strftime("%Y-%m-%d 23:59:59")
        headers, datas = self.__request_data__(language='41',pageNo=1,lastSeenDate=lastSeenDate)
        data = self.__request__(headers, datas)
        totalPages = data['totalPages']
        totalCount = data['totalCount']
        print('lastSeenDate %s ,共有 %s 页，数据 %s 条'%(lastSeenDate, totalPages, totalCount))
        i = 35
        if i > totalPages:
            i = 1
        for pageNo in range(i,totalPages+1):

            print('正在采集：第%s页'% pageNo)
            # 10-30s请求一次
            # time.sleep(random.randint(10,30))
            headers, datas = self.__request_data__(language='41', pageNo=pageNo,lastSeenDate=lastSeenDate)
            print(1,'***********************************')
            data = self.__request__(headers, datas)
            print(2, '***********************************')
            data_list = data['list']
            i = 0
            for data in data_list:
                i += 1
                print('正在采集：第%s页，第%s条'%(pageNo,(pageNo-1)*20+i))
                ad_info = self.__parse__(data,language='41')
                self.__save_data__(ad_info)

    def main(self):

        lastSeenDate = self.__query_lastDate__()
        while lastSeenDate.strftime("%Y-%m-%d %H:%M:%S") >= '2019-01-01':
            self.__clawer__(lastSeenDate)
            lastSeenDate = lastSeenDate - datetime.timedelta(days=1)


if __name__== '__main__':
    IdvertClawer().main()
    # IdvertClawer().main()
