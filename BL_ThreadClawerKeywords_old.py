# -*- coding:utf-8 -*-

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
import threading
from threading import Thread
import requests
import time
from queue import Queue
import json,re,os,sys
from PIL import Image
from tengxun_OCR import Ocr
from log_utils.mylog import Mylog
# from fake_useragent import UserAgent
from mysql_utils.mysql_db import MysqlDb
import warnings
import random
warnings.filterwarnings('ignore')



class ThreadClawerKeyword(Thread):

    def __init__(self,i, page_number_queue, keyword_info_queue, lock, keyword):
        Thread.__init__(self)
        self.keyword = keyword
        self.lock = lock
        self.s = requests.session()
        self.threadName = '采集线程' + str(i)
        self.page_number_queue = page_number_queue
        self.keyword_info_queue = keyword_info_queue

        service_args = ['--ssl-protocol=any', '--cookies-file=False']
        dcap = dict(DesiredCapabilities.PHANTOMJS)
        dcap["phantomjs.page.settings.userAgent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"
        )
        self.driver = webdriver.PhantomJS(desired_capabilities=dcap, service_args=service_args)
        # self.driver = webdriver.Chrome()

        pass

    def __getCokie__(self):
        try:
            # 登录账户
            self.driver.get('http://www.zhangjiayuan.com.cn/userLogin.jsp')
            time.sleep(3)
            # 账户
            self.driver.find_element_by_xpath('//*[@id="user"]').send_keys('MW061206')
            time.sleep(1)
            # 密码
            self.driver.find_element_by_xpath('//*[@id="password"]').send_keys('mm2463')
            time.sleep(1)
            # 点击登录
            self.driver.find_element_by_xpath('//*[@id="login_btn"]/span/span').click()
            # 等页面加载
            time.sleep(3)
            cookie = "JSESSIONID=" + self.driver.get_cookies()[0]['value']

            # self.driver.close()

            return cookie
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)

    def __reqMessage__(self,cookie, keyWord, pageNumber):
        url = 'http://www.zhangjiayuan.com.cn/common/merchantwordsAction/doMerchantwordsTask'
        headers = {
            "Host": "www.zhangjiayuan.com.cn",
            "Connection": "keep-alive",
            # "Content-Length": "112",
            # "Cache-Control": "max-age=0",
            "Origin": "http://www.zhangjiayuan.com.cn",
            # "Upgrade-Insecure-Requests": "1",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
            # "User-Agent": self.ua.random ,
            # "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "http://www.zhangjiayuan.com.cn/common/merchantwordsAction/getMerchantwords",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cookie": cookie,
            "X-Requested-With": "XMLHttpRequest"

        }
        postData = {
            "url": "https://www.merchantwords.com/search",
            "country": "us",
            "keyword": keyWord,
            "category": "in-",
            "sortby": "sort-highest",
            "page": pageNumber,
        }
        return url,headers,postData
        pass

    def __crawler__(self, url, headers,postData):
        try:
            res = self.s.post(url,headers = headers,data=postData,timeout=30)
            data_list = json.loads(res.json()['message'])['values']
        except:
            mylog.logs().exception(sys.exc_info())
            count = 1
            while count <= 5:
                try:
                    res = self.s.post(url, headers=headers, data=postData, timeout=30)
                    data_list = json.loads(res.json()['message'])['values']
                    break
                except:
                    err_info = '__crawler__ for %d time' % count if count == 1 else '__crawler__ for %d times' % count
                    print(err_info)
                    count += 1
            if count > 5:
                data_list = []
                print("__crawler__  job failed!")

        return data_list

    def __parse__(self, data_list):
        try:
            key_word_data = []
            key_words = []
            for data in data_list:
                key_word = data['dataKeyword']
                volume = data['monthlySearchVolume']
                depth = data['depth']
                # seasonality = data['seasonality']
                # result_number = self.__getAmazonResultNumber__(key_word)
                key_word_data.append([key_word, volume, depth])
                key_words.append(key_word)
            return key_word_data, key_words
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)

    def run(self):

        print('启动：', self.threadName)
        # 获取登录后的cookie
        cookie = self.__getCokie__()
        # cookie = ''
        while not flag_clawer:
            pageNumber = self.page_number_queue.get()
            pageNumber = "page-" + str(pageNumber)
            print('采集页码：',pageNumber)
            url, headers, postData = self.__reqMessage__(cookie, self.keyword, pageNumber)
            #  爬取数据
            time.sleep(random.random())
            data_list = self.__crawler__(url, headers, postData)
            if data_list:
                # 解析数据
                key_word_data, key_words = self.__parse__(data_list)
                self.keyword_info_queue.put(key_word_data)
                print(key_word_data)
                # 存储关键词（第二层爬取）
                global next_key_words
                self.lock.acquire()
                next_key_words += key_words
                self.lock.release()
            else:
                self.page_number_queue.queue.clear()
                # 数据加载完成，跳出循环
                break
        self.driver.quit()
        print('退出：', self.threadName)

        pass


class ThreadParse(Thread):

    def __init__(self, i, keyword_info_queue, keywords_queue, mptt_keyword_id):
        Thread.__init__(self)
        self.mysql = MysqlDb()
        self.mptt_keyword_id = mptt_keyword_id
        self.threadName = '解析线程' + str(i)
        self.keyword_info_queue = keyword_info_queue
        self.keywords_queue = keywords_queue

    # 将数据写入数据库
    def save_keyWords(self, key_word_data):
        try:
            # datas = []
            for data in key_word_data:
                keyWord = data[0]
                volume = data[1]
                depth = data[2]
                # resultNumber = data[3]
                # datas.append((keyWord, volume, depth))
                print('写入数据：',(keyWord, volume, depth))
                self.keywords_queue.put(keyWord)
                sql = 'INSERT IGNORE INTO amazonshop_keywords (keyword_name, searches_volume, depth, mptt_keyword_id) VALUES (%s,%s,%s,%s)'
                self.mysql.insert(sql, [(keyWord, volume, depth,self.mptt_keyword_id)])
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)

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


# class WriteAmazonResult(Thread):
#
#     def __init__(self, i, keywords_queue):
#         Thread.__init__(self)
#         self.keywords_queue = keywords_queue
#         self.mysql = MysqlDb()
#         self.threadName = '写入线程' + str(i)
#         service_args = ['--ssl-protocol=any', '--cookies-file=False']
#         dcap = dict(DesiredCapabilities.PHANTOMJS)
#         dcap["phantomjs.page.settings.userAgent"] = (
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"
#         )
#         self.driver = webdriver.PhantomJS(desired_capabilities=dcap, service_args=service_args)
#
#     def __getAmazonResultNumber__(self,keyword):
#         url = 'https://www.amazon.com/?language=en_US'
#         self.driver.get(url)
#         # time.sleep(1)
#
#         try:
#             # text_box = WebDriverWait(self.driver, 30, 3).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="twotabsearchtextbox"]')))
#             # text_box.clear()
#             # text_box.send_keys(keyword)
#             # WebDriverWait(self.driver, 30, 3).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="nav-search"]/form/div[2]/div/input'))).click()
#             self.driver.find_element_by_xpath('//*[@id="twotabsearchtextbox"]').clear().send_keys(keyword)
#             self.driver.find_element_by_xpath('//*[@id="nav-search"]/form/div[2]/div/input').click()
#         except :
#             mylog.logs().exception(sys.exc_info())
#             flag = self.get_character_by_ocr('https://www.amazon.com/?language=en_US')
#             if flag:
#                 # WebDriverWait(self.driver, 30, 3).until(
#                 #     EC.visibility_of_element_located((By.XPATH, '//*[@id="twotabsearchtextbox"]'))).clear().send_keys(
#                 #     keyword)
#                 # WebDriverWait(self.driver, 30, 3).until(
#                 #     EC.element_to_be_clickable((By.XPATH, '//*[@id="nav-search"]/form/div[2]/div/input'))).click()
#                 self.driver.find_element_by_xpath('//*[@id="twotabsearchtextbox"]').send_keys(keyword)
#                 self.driver.find_element_by_xpath('//*[@id="nav-search"]/form/div[2]/div/input').click()
#
#         time.sleep(random.random())
#         result_number = self.driver.find_element_by_xpath('//*[@id="search"]/span/h1/div/div[1]/div/div/span[1]').text
#         # result_number = re.search(r'共.*?(\d+,\d+|\d+)',result_number).group(1).replace(',','')
#         result_number = re.search(r'([,\d]+) result',result_number).group(1).replace(',','')
#         self.driver.delete_all_cookies()
#         return result_number
#
#     def __request__(self, keyword):
#         post_data = {
#             "i": "aps",
#             "k": keyword,
#             "ref": "nb_sb_noss",
#             "url": "search-alias=aps",
#         }
#
#         headers = {
#             "Host": "www.amazon.com",
#             # "Connection": "keep-alive",
#             "Connection": "close",
#             "Upgrade-Insecure-Requests": "1",
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
#             "Referer": "www.amazon.com",
#             "Accept-Encoding": "gzip, deflate, br",
#             "Accept-Language": "zh-CN,zh;q=0.9",
#             # "Cookie": cookie ,
#             "Cookie": 'session-id=144-3995449-1647815; session-id-time=2082787201l; i18n-prefs=USD; sp-cdn="L5Z9:CN"; ubid-main=131-4704024-1481156; x-wl-uid=11pI1R8AVx1DhVjYPNqpO1wdTNilckVannUorKn0kTNwC/9W0omYFKqd7poB5OES9mnwKfD4c/Iw=; session-token=lJzTNP+ST89Hf15FrcRq4hMrf2wXixD7gNw8oLVCeTHeombHt+dTEu99kiQl++j3NKWnx2N5h5mldqo2Sdm1WcqVhPZOy2pbIuQdrLFwHhLhDt69DZAyON5JiNxW69WXOP4iPoQFxZyFznxrfYFMtTHPIr8ls2XMY31zsQ1Y/xl2H2qVOmGGuVK+ikpGIp9+MuYqpclWKUkukD53mbY7IHGFVuxrOLPb1+1ME7jl2j5p/lPMhnHPoiEGG5wjjEYO; skin=noskin; lc-main=en_US; csm-hit=tb:FJPRNNZKZNCFYCEB7QS7+s-FJPRNNZKZNCFYCEB7QS7|1563361913768&t:1563361913768&adb:adblk_no'
#
#         }
#
#         url = 'https://www.amazon.com/s?i=aps&ref=nb_sb_noss&url=search-alias%3Daps&k=' + keyword
#         res = requests.post(url, headers=headers, data=post_data)
#         html = res.text
#         result = re.search(r'"totalResultCount":([,\d]+),', html)
#         if result:
#             result = result.group(1)
#         else:
#             result = 0
#         return result
#         pass
#
#     # 识别验证码
#     def get_character_by_ocr(self, product_link):
#             '''
#             :param product_link: 商品链接
#             :return:
#             '''
#
#             try:
#
#                 print('出现验证码，正在验证！')
#
#                 # 最多进行10次验证
#                 for i in range(10):
#                     # 验证码截图
#                     img_path = os.getcwd() + '/verification/verification_code.png'
#                     self.driver.get_screenshot_as_file(img_path)
#                     try:
#                         element = self.driver.find_element_by_xpath(
#                             '/html/body/div/div[1]/div[3]/div/div/form/div[1]/div/div/div[1]')
#                     except:
#
#                         print('验证成功！')
#                         return self.driver.page_source
#
#                     # 计算出元素上、下、左、右 位置
#                     left = element.location['x']
#                     top = element.location['y']
#                     right = element.location['x'] + element.size['width']
#                     bottom = element.location['y'] + element.size['height']
#
#                     im = Image.open(img_path)
#                     im = im.crop((left, top, right, bottom))
#                     im.save(img_path)
#
#                     # 识别验证码
#                     tengxun_ocr = Ocr()
#                     character = tengxun_ocr.recognition_character(img_path)
#                     print('识别验证码：', character)
#
#                     # 验证码输入
#                     self.driver.find_element_by_xpath('//*[@id="captchacharacters"]').send_keys(character)
#                     time.sleep(3)
#                     # 验证
#                     self.driver.find_element_by_xpath(
#                         '/html/body/div/div[1]/div[3]/div/div/form/div[2]/div/span/span/button').click()
#                 else:
#                     return False
#
#             except Exception as err:
#                 mylog.logs().exception(sys.exc_info())
#                 print(err)
#
#     def run(self):
#         try:
#
#             print('启动：', self.threadName)
#
#             while not flag_write:
#                 try:
#                     keyword = self.keywords_queue.get(timeout=2)
#                 except:
#                     time.sleep(1)
#                     continue
#                 sql = 'select id from amazonshop_keywords WHERE keyword_name =\'%s\' and  result_volume IS NULL' % keyword
#                 res = self.mysql.select(sql)
#                 if res:
#                     id = res[0]['id']
#                     try:
#                         result_number = self.__getAmazonResultNumber__(keyword)
#                     except:
#                         count = 1
#                         while count <= 5:
#                             try:
#                                 result_number = self.__getAmazonResultNumber__(keyword)
#                                 break
#                             except:
#                                 err_info = '__getAmazonResultNumber__ reloading for %d time' % count if count == 1 else '__getAmazonResultNumber__ reloading for %d times' % count
#                                 print(err_info)
#                                 count += 1
#                         if count > 5:
#                             print("__getAmazonResultNumber__ job failed!")
#                     # time.sleep(random.random())
#                     # result_number = self.__request__(keyword)
#                     finally:
#                         sql = 'update amazonshop_keywords set result_volume = %s WHERE id = %s '
#                         self.mysql.update(sql,[(result_number, id)])
#                         print('更新关键词：', keyword,result_number)
#             print('退出：', self.threadName)
#             self.driver.quit()
#             self.mysql.close()
#         except Exception as err:
#             print(err)




# 采集是否完成的标志


flag_clawer = False
# 解析是否完成的标志
flag_parse = False
# 写入亚马逊结果
# flag_write = False
# 创建日志
mylog = Mylog('clawer_keywords')
# 存储关键词
next_key_words = []


def query_keyWord(keyword):

    mysql = MysqlDb()
    sql = 'select id from amazonshop_keywords WHERE keyword_name = \'%s\' ' % keyword
    res = mysql.select(sql)
    mysql.close()
    if res:
        return True
    else:
        return False

def main(keyword,mptt_keyword_id):

    flag = query_keyWord(keyword)

    if not flag:

        # 线程锁
        lock = threading.Lock()

        # 关键词的页码队列
        page_number_queue = Queue()
        # 关键词信息队列
        keyword_info_queue = Queue()
        # 关键词队列
        keywords_queue = Queue()

        for i in range(1,500):
            page_number_queue.put(i)

        # 存储3个采集线程的列表集合
        threadcrawl = []
        for i in range(3):
            thread = ThreadClawerKeyword(i, page_number_queue, keyword_info_queue, lock, keyword)
            thread.start()
            threadcrawl.append(thread)

        # 存储1个写库的线程
        threadparse = []
        for i in range(1):
            thread = ThreadParse(i, keyword_info_queue, keywords_queue, mptt_keyword_id)
            thread.start()
            threadparse.append(thread)


        # 等待队列为空，采集完成
        while not page_number_queue.empty():
            pass
        else:
            global flag_clawer
            flag_clawer = True

        for thread in threadcrawl:
            thread.join()

        # 等待队列为空，解析完成
        while not keyword_info_queue.empty():
            pass
        else:
            global flag_parse
            flag_parse = True

        for thread in threadparse:
            thread.join()

        # 进入新一轮的关键词采集，标志需全部改为False
        # 采集是否完成的标志

        flag_clawer = False
        # 解析是否完成的标志
        flag_parse = False
        # # 解析是否完成的标志
        # flag_write = False
    else:
        print('关键词已存在：',keyword)


def run(keyWord,mptt_keyword_id):

    print('正在爬取关键词：', keyWord)
    # 第一层关键词
    main(keyWord,mptt_keyword_id)

    # 第二层关键词
    global next_key_words
    length = len(next_key_words)
    # 第二层采集时，next_key_words会不断加入新的关键词，这些新加入的关键词不再进行采集
    for i in range(length):
        keyword = next_key_words[i]
        print('正在爬取第二层关键词：',keyword,'，上一级关键词：',keyWord)
        main(keyword,mptt_keyword_id)

    next_key_words = []


if __name__ == '__main__':
    all_keywords = [['diy logo hoodie print'], ['diy logo tee shirt print'], ['diy logo mugs print'], ['diy logo hoodies'], ['diy logo tee shirt'], ['diy star hoodies'], ['diy star tee shirt '], ['logo hoodies printed'], ['logo tee shirt printed']]
    for data in all_keywords:
        keyword = data[0]
        run(keyword,1)
    print('数据采集完成！')
    # run('diy logo tee shirt',1)