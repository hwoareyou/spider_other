import datetime
import os
import threading
from threading import Thread
# from queue import Queue
from queue_ import Queue
import requests,re,random,time
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from  mysql_utils.mysql_db import MysqlDb
from tengxun_OCR import Ocr
import warnings
warnings.filterwarnings('ignore')


class QueryAmazonResult(Thread):

    def __init__(self,keyword_queue):
        Thread.__init__(self)
        self.mysql = MysqlDb()
        self.keyword_queue = keyword_queue

        # 创建一个参数对象，用来控制chrome以无界面的方式打开
        chrome_options = Options()
        # 设置浏览器参数
        chrome_options.add_argument('--headless')  # 无界面
        chrome_options.add_argument('--no-sandbox')  # 让Chrome在root权限下跑
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument(
            # 'User-Agent="Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16"'
            'User-Agent=\"%s\"' % self.get_useragent(),
        )
        # chrome_options.binary_location = r'C:\Users\panda\Desktop\ChromePortable\App\Google Chrome\chrome.exe'
        # 创建浏览器对象
        self.driver = webdriver.Chrome(chrome_options=chrome_options)
        pass

    def get_useragent(self):
        useragent_list = [
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
            "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
        ]
        return random.choice(useragent_list)

    def request(self,keyword):
        # post_data = {
        #     "i": "aps",
        #     "k": keyword,
        #     "ref": "nb_sb_noss",
        #     "url": "search-alias=aps",
        # }
        post_data = {
            "field-keywords": keyword,
            "url": "search-alias=aps",
        }
        # url = 'https://www.amazon.com/s?i=aps&language=en_US&ref=nb_sb_noss&url=search-alias%3Daps&k=' + keyword
        url = 'https://www.amazon.com/s/ref=nb_sb_noss_2?url=search-alias%3Daps&field-keywords=' + keyword
        headers = {
            "Host": "www.amazon.com",
            # "Connection": "keep-alive",
            "Connection": "close",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": self.get_useragent(),
            "Referer": "https://www.amazon.com",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cookie": 'sp-cdn="L5Z9:HK"; x-wl-uid=1xBqxYIuOIztBDpPpsu351DxgWBQ42OGx343ju8M8ltVk/59nIK9MoJN1P1dB8DQ6bvfyEVzm1W8=; session-token=8Pd2UYRkblNTGDkESYqRmD+uda4m96mmoTDdbK7JUBP3lXJ/BCIGY4mUuILiiDyApyj9wVSccktWrc/0eHcI/UXbyVVRWT+mi7EsYM8VoQ/42MAPsS/ZVu70GDm+BhHjk9YDLk1euKk5SluVfQpWB/7W3fOKbqZ91+z2F7nEveiyZTWESyD7hqmyQT+jeckO; lc-main=en_US; i18n-prefs=USD; skin=noskin; ubid-main=132-5397715-1604240; session-id-time=2082787201l; session-id=134-2367345-6187209; csm-hit=tb:6F31XF9WC0K3CGKQ95X3+s-6KHM0E3VF36QF8D4J3JV|1566201614805&t:1566201614805&adb:adblk_no' ,
            # "Cookie": 'session-id=144-3995449-1647815; session-id-time=2082787201l; i18n-prefs=USD; sp-cdn="L5Z9:CN"; ubid-main=131-4704024-1481156; x-wl-uid=11pI1R8AVx1DhVjYPNqpO1wdTNilckVannUorKn0kTNwC/9W0omYFKqd7poB5OES9mnwKfD4c/Iw=; session-token=lJzTNP+ST89Hf15FrcRq4hMrf2wXixD7gNw8oLVCeTHeombHt+dTEu99kiQl++j3NKWnx2N5h5mldqo2Sdm1WcqVhPZOy2pbIuQdrLFwHhLhDt69DZAyON5JiNxW69WXOP4iPoQFxZyFznxrfYFMtTHPIr8ls2XMY31zsQ1Y/xl2H2qVOmGGuVK+ikpGIp9+MuYqpclWKUkukD53mbY7IHGFVuxrOLPb1+1ME7jl2j5p/lPMhnHPoiEGG5wjjEYO; skin=noskin; lc-main=en_US; csm-hit=tb:FJPRNNZKZNCFYCEB7QS7+s-FJPRNNZKZNCFYCEB7QS7|1563361913768&t:1563361913768&adb:adblk_no'

        }

        try:
            rest = requests.post(url, headers=headers, data=post_data, timeout=20)
        except:
            count = 1
            while count <= 5:
                try:
                    rest = requests.post(url, headers=headers, data=post_data, timeout=20)
                    break
                except:
                    err_info = 'requests reloading for %d time' % count if count == 1 else 'requests reloading for %d times' % count
                    print(err_info)
                    count += 1
            if count > 5:
                print("requests job failed!")
                result = self.getAmazonResultNumber(url)
                sql = 'update amazonshop_keywords set result_volume = %s WHERE keyword_name = %s '
                self.mysql.update(sql, [(result, keyword)])
                print('keyword:%s,result:%s' % (keyword, result))

        return rest, url

    # 识别验证码
    def get_character_by_ocr(self):
        try:
            print('出现验证码，正在验证！')
            # 最多进行10次验证
            for i in range(10):
                # 验证码截图
                img_path = os.getcwd() + '/verification/verification_code.png'
                self.driver.get_screenshot_as_file(img_path)
                try:
                    element = self.driver.find_element_by_xpath(
                        '/html/body/div/div[1]/div[3]/div/div/form/div[1]/div/div/div[1]')
                except:
                    print('验证成功！')
                    return True

                # 计算出元素上、下、左、右 位置
                left = element.location['x']
                top = element.location['y']
                right = element.location['x'] + element.size['width']
                bottom = element.location['y'] + element.size['height']

                im = Image.open(img_path)
                im = im.crop((left, top, right, bottom))
                im.save(img_path)

                # 识别验证码
                tengxun_ocr = Ocr()
                character = tengxun_ocr.recognition_character(img_path)
                print('识别验证码：', character)

                # 验证码输入
                self.driver.find_element_by_xpath('//*[@id="captchacharacters"]').send_keys(character)
                time.sleep(3)
                # 验证
                self.driver.find_element_by_xpath(
                    '/html/body/div/div[1]/div[3]/div/div/form/div[2]/div/span/span/button').click()
            else:
                return False

        except Exception as err:
            print(err)

    def getAmazonResultNumber(self,url):
        try:
            self.driver.get(url)
        except:
            count = 1
            while count <= 5:
                try:
                    self.driver.get(url)
                    break
                except:
                    err_info = 'driver.get() reloading for %d time' % count if count == 1 else 'driver.get() reloading for %d times' % count
                    print(err_info)
                    count += 1
            if count > 5:
                print("driver.get() job failed!")

            title = self.driver.title
            if title == 'Robot Check':
                flag = self.get_character_by_ocr()
                if flag:
                    self.driver.get(url)
                else:
                    return -99
            time.sleep(random.random())
            try:
                result_number = self.driver.find_element_by_xpath('//*[@id="search"]/span/h1/div/div[1]/div/div/span[1]').text
                result_number = re.search(r'([,\d]+) result', result_number).group(1).replace(',', '')
            except:
                result_number = -99
            # result_number = re.search(r'共.*?(\d+,\d+|\d+)',result_number).group(1).replace(',','')
            # driver.delete_all_cookies()
            return result_number

    def run(self):

        while not flag_clawer:
            try:
                keyword = self.keyword_queue.get(timeout=10)
            except:
                time.sleep(10)
                continue
            try:
                rest , url = self.request(keyword)
            except:
                return
            html = rest.text
            title = re.search(r'<title[ dir="ltr"]*>(.+)</title>',html)
            if title:
                # print('title')
                title = title.group(1)
                if title == 'Robot Check':
                    # print('Robot Check')
                    result = self.getAmazonResultNumber(url)
                else:
                    result = re.search(r'"totalResultCount":([,\d]+),', html)
                    if result:
                        result = result.group(1)
                    else:
                        result = -99
            else:
                # print('no-title')
                result = -99
            sql = 'update amazonshop_keywords set result_volume = %s WHERE keyword_name = %s '
            self.mysql.update(sql,[(result, keyword)])
            print('keyword:%s,result:%s'%(keyword, result))

        self.driver.quit()

flag_clawer = False

# 每5分钟获取一次数据库中待查询的关键词
def query_keyWord(keyword_queue):

    sql = 'select keyword_name from amazonshop_keywords WHERE result_volume IS NULL OR result_volume = -99'
    mysql = MysqlDb()
    res = mysql.select(sql)

    if res:
        for data in res :
            keyword = data['keyword_name']
            keyword_queue.put(keyword,unique=True)

    mysql.close()
    t = threading.Timer(300, query_keyWord,[keyword_queue])
    t.start()


def main():
    keyword_queue = Queue()
    query_keyWord(keyword_queue)


    while 1:

        global flag_clawer
        flag_clawer = False

        print(datetime.datetime.now())
        if not keyword_queue.empty():

            # 存储1个线程
            threadcrawl = []
            for i in range(1):
                thread = QueryAmazonResult(keyword_queue)
                thread.start()
                threadcrawl.append(thread)

            # 等待队列为空，采集完成
            while not keyword_queue.empty():
                pass
            else:
                flag_clawer = True

            for thread in threadcrawl:
                thread.join()

            flag_clawer = False

        else:
            print('没有待查询的关键词！')
            time.sleep(300)

    pass







if __name__ == '__main__':
    main()