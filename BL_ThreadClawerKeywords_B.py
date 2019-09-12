# -*- coding:utf-8 -*-
import csv
import datetime
import traceback

import threading
from threading import Thread
import requests
import time
from queue import Queue
import json,re,os,sys
from lxml import etree
from PIL import Image
from tengxun_OCR import Ocr
from log_utils.mylog import Mylog
from fake_useragent import UserAgent
from mysql_utils.mysql_db import MysqlDb
import warnings
import random
warnings.filterwarnings('ignore')


class ThreadClawerKeyword(Thread):

    def __init__(self,i, sec_keywords_queue, keyword_info_queue, lock):
        Thread.__init__(self)
        self.s = requests.session()
        self.threadName = '第二层关键词采集线程-' + str(i)
        self.sec_keywords_queue = sec_keywords_queue
        self.keyword_info_queue = keyword_info_queue
        self.lock = lock
        self.ua = UserAgent()

    def __getCokie__(self):
        try:
            # 登录账户
            self.driver.get('http://www.merchantwords.cn')
            time.sleep(3)
            # 账户
            self.driver.find_element_by_xpath('//*[@id="username"]').send_keys('mw58315')
            time.sleep(1)
            # 密码
            self.driver.find_element_by_xpath('//*[@id="password"]').send_keys('7dhfy')
            time.sleep(1)
            # 点击登录
            self.driver.find_element_by_xpath('//*[@class="btn btn-success pull-right"]').click()
            # 等页面加载
            time.sleep(3)
            cookie = "JSESSIONID=" + self.driver.get_cookies()[0]['value']

            # self.driver.close()

            return cookie
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)
            traceback.print_exc()

    def __reqMessage__(self, keyWord, pageNumber):

        if pageNumber == 1:
            referer = "http://217.69.1.187/?username=mw58315"
        else:
            referer = 'http://217.69.1.187/search/us/' + keyWord + '/sort-highest/page-' + str(
                pageNumber - 1) + '/?username=mw58315 '

        url = 'http://217.69.1.187/search/us/' + keyWord + '/sort-highest/page-' + str(
            pageNumber) + '/?username=mw58315'
        headers = {
            "Host": "217.69.1.187",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
            "User-Agent": self.ua.chrome,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "Referer": referer,
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            # "Cookie": '__distillery=adfcfbb_16024f7a-0efd-4e95-ba9c-69a97c9ea604-d8b25cd5f-7880ca3ceb97-e14e; mp_1102b81d424da9270ca275eb2e01a038_mixpanel={"distinct_id": "mwords1906@163.com","$device_id": "16c8f12cc5536e-035d5a838ed7ea-7373e61-1fa400-16c8f12cc56498","$initial_referrer": "http://www.merchantwords.cn/","$initial_referring_domain": "www.merchantwords.cn","__mps": {},"__mpso": {},"__mpus": {},"__mpa": {},"__mpu": {},"__mpr": [],"__mpap": [],"mp_name_tag": "mwords1906@163.com","$user_id": "mwords1906@163.com"}',
        }
        postData = {
            # "username": "mw58315",
        }
        return url, headers, postData
        pass

    def __crawler__(self, url, headers, postData):
        try:
            time.sleep(random.randint(1, 3))
            res = self.s.get(url, headers=headers, data=postData, timeout=60)
            html = res.text
        except:
            mylog.logs().exception(sys.exc_info())
            count = 1
            while count <= 3:
                try:
                    res = self.s.get(url, headers=headers, data=postData, timeout=60)
                    html = res.text
                    break
                except:
                    err_info = '__crawler__ for %d time' % count if count == 1 else '__crawler__ for %d times' % count
                    print(err_info)
                    count += 1
            if count > 3:
                html = ''
                print("__crawler__  job failed!")

        return html

    def __parse__(self, html):
        try:
            html_etree = etree.HTML(html)
            key_words_element = html_etree.xpath('//tr[contains(@data-item,"item-0")]')
            key_words_datas = []
            for element in key_words_element:
                key_word = str(element.xpath('string(./td[@data-title="Amazon Search Terms"]//span)')).strip().replace(' ','').replace('\n\n', ' ')
                volume = int(str(element.xpath('./td[@data-title="Amazon Search Volume"]/text()')[0]).replace('< ', '').replace(',',''))
                depth = int(element.xpath('./td[@data-title="Depth"]/text()')[0])
                # seasonality = str(element.xpath('./td[@data-title="Seasonality"]/text()')[0])
                key_words_datas.append([key_word, volume, depth])
            return key_words_datas
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)
            traceback.print_exc()

    def __getNum__(self, html):
        try:

            html_etree = etree.HTML(html)
            li_list = html_etree.xpath('//ul[@class="pagination"]/li')
            if len(li_list) <= 1:
                num = len(li_list)
            elif len(li_list) <= 10:
                num = len(li_list) - 1
            else:
                num = 500
            return num
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)
            traceback.print_exc()

    def __save_process__(self, n):

        # 更新当前的采集进度（web展示）
        content = {"num":n}
        # file_root = os.getcwd() + '/file/'
        file_root = os.getcwd().replace('utils','') + '/amazon1/amazon/amazon/static/file/'
        if not os.path.exists(file_root):
            os.makedirs(file_root)
        file_path = file_root + 'keyWord_num.json'
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(content, json_file, ensure_ascii=False)
        pass

    def run(self):

        print('启动：', self.threadName)
        while not flag_clawer_sec:
            try:
                keyword_data = self.sec_keywords_queue.get(timeout=2)
                keyword = keyword_data[0]
                mptt_keyword_id = keyword_data[1]
            except:
                time.sleep(1)
                continue
            try:
                url, headers, postData = self.__reqMessage__(keyword, 1)
                html = self.__crawler__(url, headers, postData)
                num = self.__getNum__(html)
                if num:
                    for i in range(1, num + 1):
                        try:
                            url, headers, postData = self.__reqMessage__(keyword, i)
                            html = self.__crawler__(url, headers, postData)
                            if html:
                                key_words_datas = self.__parse__(html)
                                keyword_info = []
                                for key_words_data in key_words_datas:
                                    keyword_info.append([key_words_data[0], key_words_data[1], key_words_data[2], mptt_keyword_id])
                                self.keyword_info_queue.put(keyword_info)
                        except:
                            pass
                self.lock.acquire()
                global n
                n += 1
                # 保存采集进度（当前）
                self.__save_process__(n)
                self.lock.release()

            except Exception as err:
                mylog.logs().exception(sys.exc_info())
                print(err)
                traceback.print_exc()

        print('退出：', self.threadName)

        pass


class ThreadParse(Thread):

    def __init__(self, i, keyword_info_queue):
        Thread.__init__(self)
        self.mysql = MysqlDb()
        self.threadName = '解析线程-' + str(i)
        self.keyword_info_queue = keyword_info_queue

    # 将数据写入数据库
    def save_keyWords(self, key_word_data):
        try:

            # keyWord = key_word_data[0]
            # volume = key_word_data[1]
            # depth = key_word_data[2]
            # mptt_keyword_id = key_word_data[3]

            sql = 'INSERT IGNORE INTO amazonshop_keywords (keyword_name, searches_volume, depth, mptt_keyword_id) VALUES (%s,%s,%s,%s)'
            self.mysql.insert(sql, key_word_data)
            print('写入数据：', key_word_data)

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)
            traceback.print_exc()

    def run(self):

        print('启动：', self.threadName)

        while not flag_parse:
            try:
                key_word_data =  self.keyword_info_queue.get(timeout=2)
            except:
                time.sleep(1)
                continue
            self.save_keyWords(key_word_data)

        print('退出：', self.threadName)
        self.mysql.close()

        pass


class GetSecKeyWords(Thread):

    def __init__(self, i, wait_clawer_queue, sec_keywords_queue, keyword_info_queue):
        Thread.__init__(self)
        self.mysql = MysqlDb()
        self.threadName = '第一层关键词采集线程-' + str(i)
        self.ua = UserAgent()
        self.s = requests.session()
        self.wait_clawer_queue = wait_clawer_queue
        self.sec_keywords_queue = sec_keywords_queue
        self.keyword_info_queue = keyword_info_queue
        pass

    def __reqMessage__(self, keyWord, pageNumber):

        if pageNumber == 1:
            referer = "http://217.69.1.187/?username=mw58315"
        else:
            referer = 'http://217.69.1.187/search/us/' + keyWord +'/sort-highest/page-' + str(pageNumber-1) + '/?username=mw58315 '

        url = 'http://217.69.1.187/search/us/' + keyWord +'/sort-highest/page-' + str(pageNumber) + '/?username=mw58315'
        headers = {
            "Host": "217.69.1.187",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
            "User-Agent": self.ua.chrome ,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "Referer": referer,
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            # "Cookie": '__distillery=adfcfbb_16024f7a-0efd-4e95-ba9c-69a97c9ea604-d8b25cd5f-7880ca3ceb97-e14e; mp_1102b81d424da9270ca275eb2e01a038_mixpanel={"distinct_id": "mwords1906@163.com","$device_id": "16c8f12cc5536e-035d5a838ed7ea-7373e61-1fa400-16c8f12cc56498","$initial_referrer": "http://www.merchantwords.cn/","$initial_referring_domain": "www.merchantwords.cn","__mps": {},"__mpso": {},"__mpus": {},"__mpa": {},"__mpu": {},"__mpr": [],"__mpap": [],"mp_name_tag": "mwords1906@163.com","$user_id": "mwords1906@163.com"}',
        }
        postData = {
            # "username": "mw58315",
        }
        return url,headers,postData
        pass

    def __crawler__(self, url, headers,postData):
        try:
            time.sleep(random.randint(1,3))
            res = self.s.get(url,headers = headers,data=postData,timeout=60)
            html = res.text
        except:
            mylog.logs().exception(sys.exc_info())
            count = 1
            while count <= 3:
                try:
                    res = self.s.get(url, headers=headers, data=postData, timeout=60)
                    html = res.text

                    break
                except:
                    err_info = '__crawler__ for %d time' % count if count == 1 else '__crawler__ for %d times' % count
                    print(err_info)
                    count += 1
            if count > 3:
                html = ''
                print("__crawler__  job failed!")


        return html

    def __parse__(self, html):
        try:
            html_etree = etree.HTML(html)
            key_words_element = html_etree.xpath('//tr[contains(@data-item,"item-0")]')
            key_words_datas = []
            for element in key_words_element:
                key_word = str(element.xpath('string(./td[@data-title="Amazon Search Terms"]//span)')).strip().replace(' ','').replace('\n\n', ' ')
                volume = int(str(element.xpath('./td[@data-title="Amazon Search Volume"]/text()')[0]).replace('< ', '').replace(',',''))
                depth = int(element.xpath('./td[@data-title="Depth"]/text()')[0])
                # seasonality = str(element.xpath('./td[@data-title="Seasonality"]/text()')[0])
                key_words_datas.append([key_word, volume, depth])
            return key_words_datas
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)
            traceback.print_exc()

    def __getNum__(self, html):
        try:

            html_etree = etree.HTML(html)
            li_list = html_etree.xpath('//ul[@class="pagination"]/li')
            if len(li_list) <= 1:
                num = len(li_list)
            elif len(li_list) <= 10:
                num = len(li_list) - 1
            else:
                num = 500
            return num
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)
            traceback.print_exc()

    def query_keyWord(self, keyword, mptt_keyword_id):

        mysql = MysqlDb()
        sql = 'select id from amazonshop_keywords WHERE keyword_name = \"%s\" AND  mptt_keyword_id = \'%s\' ' % (keyword, mptt_keyword_id)
        res = mysql.select(sql)
        mysql.close()
        if res:
            return True
        else:
            return False

    def update_status(self, keyword,mptt_keyword_id,flag):
        sql = 'update amazonshop_waitcrawlkeywords set flag = %s WHERE w_crawl_keyword = %s AND mptt_keyword_id = %s '
        self.mysql.update(sql,[(flag,keyword,mptt_keyword_id)])

    def __save_process__(self, n):

        # 更新当前的采集进度（web展示）
        content = {"sum": n}
        file_root = os.getcwd().replace('utils', '') + '/amazon1/amazon/amazon/static/file/'
        if not os.path.exists(file_root):
            os.makedirs(file_root)
        file_path = file_root + 'keyWord_sum.json'
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(content, json_file, ensure_ascii=False)
        pass

    def run(self):

        print('启动：', self.threadName)
        total_keyWords = 0
        while not flag_clawer_1st:
            try:
                keyword_data = self.wait_clawer_queue.get(timeout=2)
                keyword = keyword_data[0]
                mptt_keyword_id = keyword_data[1]
            except:
                time.sleep(1)
                continue

            try:
                print('正在采集关键词：',(keyword, mptt_keyword_id))
                flag = self.query_keyWord(keyword, mptt_keyword_id)
                if not flag:
                    url, headers, postData = self.__reqMessage__(keyword, 1)
                    html = self.__crawler__(url, headers, postData)
                    # 请求数据成功
                    if html:
                        num = self.__getNum__(html)
                        # 页码数据不为空
                        if num:
                            for i in range(1,num+1):
                                try:
                                    url, headers, postData = self.__reqMessage__(keyword, i)
                                    html = self.__crawler__(url, headers, postData)
                                    if html:
                                        key_words_datas = self.__parse__(html)
                                        total_keyWords += len(key_words_datas)
                                        # 保存采集进度（总量）
                                        self.__save_process__(total_keyWords)
                                        keyword_info = []
                                        for key_words_data in key_words_datas:
                                            self.sec_keywords_queue.put([key_words_data[0],mptt_keyword_id])
                                            keyword_info.append([key_words_data[0], key_words_data[1], key_words_data[2], mptt_keyword_id])
                                        self.keyword_info_queue.put(keyword_info)
                                except:
                                    pass

                        # 更改关键词状态为 已采集
                        self.update_status(keyword, mptt_keyword_id, 1)
                    else:
                        # 更改关键词状态为 采集失败
                        self.update_status(keyword, mptt_keyword_id, 3)
                else:
                    print('关键词已存在：',(keyword, mptt_keyword_id))

            except Exception as err:
                mylog.logs().exception(sys.exc_info())
                print(err)
                traceback.print_exc()

        print('退出：', self.threadName)


# 每五分钟获取一次数据库中待采集和采集失败的关键词
def query_wait_keyWord(wait_clawer_queue):

    sql = 'select w_crawl_keyword,mptt_keyword_id from amazonshop_waitcrawlkeywords  WHERE  flag = 0 OR  flag = 3'
    mysql = MysqlDb()
    res = mysql.select(sql)

    if res:
        # 更改关键词状态为 采集中
        sql = 'update amazonshop_waitcrawlkeywords set flag = 2 WHERE w_crawl_keyword = %s  AND mptt_keyword_id = %s '
        for keyword_data in res:
            keyword = keyword_data['w_crawl_keyword']
            mptt_keyword_id = keyword_data['mptt_keyword_id']
            wait_clawer_queue.put([keyword, mptt_keyword_id])
            mysql.update(sql, [(keyword, mptt_keyword_id)])

    mysql.close()
    t = threading.Timer(300, query_wait_keyWord, [wait_clawer_queue])
    t.start()




flag_clawer_1st = False
flag_clawer_sec = False
# 解析是否完成的标志
flag_parse = False
# 创建日志
mylog = Mylog('clawer_keywords')
# 关键词采集数
n = 0



def read_csv():

    csv_data = csv.reader(open(r'C:\Users\panda\Desktop\us_print shirt_2019071694127.csv', 'r', encoding='gbk'))
    all_keywords = []
    for row in csv_data:
        keyword = row[0]
        all_keywords.append((keyword,5))

    return all_keywords


def main():

    lock = threading.Lock()
    # 待采集关键词队列
    wait_clawer_queue = Queue()
    # 第二层关键词队列
    sec_keywords_queue = Queue()
    # 关键词信息队列
    keyword_info_queue = Queue()

    # 查询待采集的关键词
    query_wait_keyWord(wait_clawer_queue)


    # all_keywords = read_csv()
    # for  keyword_data in all_keywords:
    #     wait_clawer_queue.put((keyword_data[0],keyword_data[1]))

    # wait_clawer_queue.put(('shirts',5))

    while 1:

        print(datetime.datetime.now())
        if not wait_clawer_queue.empty():

            # 存储1个采集第一层关键词的线程列表集合
            threadcrawl_1st = []
            for i in range(1):
                thread = GetSecKeyWords(i, wait_clawer_queue, sec_keywords_queue, keyword_info_queue)
                thread.start()
                threadcrawl_1st.append(thread)

            # 存储3个采集第二层关键词的线程列表集合
            threadcrawl_sec = []
            for i in range(3):
                thread = ThreadClawerKeyword(i, sec_keywords_queue, keyword_info_queue, lock)
                thread.start()
                threadcrawl_sec.append(thread)

            # 存储1个写库的线程
            threadparse = []
            for i in range(1):
                thread = ThreadParse(i, keyword_info_queue)
                thread.start()
                threadparse.append(thread)


            # 等 待采集队列为空，采集完成
            while not wait_clawer_queue.empty():
                pass
            else:
                global flag_clawer_1st
                flag_clawer_1st = True

            for thread in threadcrawl_1st:
                thread.join()


            # 等第二层关键词队列为空，采集完成
            while not sec_keywords_queue.empty():
                pass
            else:
                global flag_clawer_sec
                flag_clawer_sec = True

            for thread in threadcrawl_sec:
                thread.join()


            # 等关键词信息列为空，解析入库完成
            while not keyword_info_queue.empty():
                pass
            else:
                global flag_parse
                flag_parse = True

            for thread in threadparse:
                thread.join()


            # 进入新一轮的关键词采集，标志需全部改为False
            flag_clawer_1st = False
            flag_clawer_sec = False
            flag_parse = False

        else:
            print('没有待采集的关键词！')
            time.sleep(300)



if __name__ == '__main__':

    main()
    print('数据采集完成！')
