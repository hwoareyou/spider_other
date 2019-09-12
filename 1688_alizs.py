# -*- coding: utf-8 -*-
# @Author   : liu
import requests, time ,random
from lxml import etree
from mysql_utils.mysql_db import MysqlDb
import warnings
warnings.filterwarnings('ignore')


# 采集关键词信息（关键词ID）
class ALiIndexClawer():

    def __init__(self):
        self.mysql = MysqlDb("localhost", 3306, "root", "123123", "alzs")
        pass


    def save_data(self,data):
        sql = 'insert ignore into alzs_keyword (key_word_id,key_word_name,level,up_level_id) values (%s,%s,%s,%s) '
        self.mysql.insert(sql,data)
        pass


    def get_level_keyWords(self,level,type):

        keyWords_list = []
        if level.xpath('./@data-key'):
            keyWord_id = str(level.xpath('./@data-key')[0])
        else:
            return False
        keyWord_name = str(level.xpath('string(./p)')).strip()
        if keyWord_id != '-1':
            keyWords_list.append((keyWord_id,keyWord_name,type))
            print('关键词信息：', keyWord_id, keyWord_name, type)
        # keyWords_list.append({"keyWord_id": keyWord_id, "keyWord_name": keyWord_name, "type": type})

        keyWords_datas = level.xpath('./div//a')
        for keyWords_data in keyWords_datas:
            keyWord_id = keyWords_data.attrib['data-key']
            keyWord_name = keyWords_data.attrib['title']
            keyWords_list.append((keyWord_id, keyWord_name, type))
            print('关键词信息：',keyWord_id, keyWord_name, type)
            # keyWords_list.append({"keyWord_id":keyWord_id,"keyWord_name":keyWord_name,"type":type})

        return keyWords_list


    def request(self,url,referer):

        headers = {
            "Host": "index.1688.com",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
            # "User-Agent": get_useragent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "Referer": referer,
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cookie": "ali_ab=119.123.132.57.1567996618960.2; cna=y6j8FYd0fiACAXd7hDkF1Tr5; _alizs_area_info_=1001; ali_beacon_id=119.123.132.57.1567996623475.834131.1; _alizs_user_type_=purchaser; _alizs_base_attr_type_=0; cookie2=14702df760efc7080640c6e6a915917c; t=ac5eb97131e72a51ce30b43ad9fce58c; _tb_token_=e9eae5e335bb; __cn_logon__=false; alicnweb=touch_tb_at%3D1568019013546; _alizs_cate_info_=67; isg=BHFxLC3hiHbtPSRhefi2LAFrgP0LXuXQHtzoaFOGHDhXepPMm69HoIwImg4cqX0I; l=cBO1rYllq1RYCI4kBOCanurza77OSIRYYuPzaNbMi_5LK6T6-W_Okzr-uF96VjWdtbYB4HK4uMv9-etuZvUAfZ37jlBR."
        }

        res = requests.get(url, headers=headers)

        return res.text


    def parse(self,html_source):

        html = etree.HTML(html_source)
        level = html.xpath('//div[@class="cate-selectors fd-clr"]/div')
        level_1 = level[0]
        level_2 = level[1]
        level_3 = level[2]

        return level_1, level_2, level_3


    def main(self):

        # level 1 数据
        url = 'http://index.1688.com/alizs/keyword.htm?userType=purchaser&cat=201128501'
        html = self.request(url,url)
        level_1, level_2, level_3 = self.parse(html)
        data_1 = self.get_level_keyWords(level_1, 1)
        if data_1:
            data_1 = [item + ('',) for item in data_1]
            self.save_data(data_1)
            print('level 1 关键词信息已保存！')

            url_source = 'http://index.1688.com/alizs/keyword.htm?userType=purchaser&cat='

            # level 2 数据
            for data_1_sub in data_1:
                key_word_id = data_1_sub[0]
                url_sub = url_source + key_word_id
                html = self.request(url_sub,url)
                level_1, level_2, level_3 = self.parse(html)
                data_2 = self.get_level_keyWords(level_2, 2)
                if data_2:
                    data_2 = [item+(key_word_id,) for item in data_2]
                    self.save_data(data_2)
                    print('正在保存 level 2 数据...')

                    # level 3 数据
                    for data_2_sub in data_2:
                        key_word_id = data_2_sub[0]
                        url_sub_sub = url_sub + ',' + key_word_id
                        html = self.request(url_sub_sub,url_sub)
                        level_1, level_2, level_3 = self.parse(html)
                        data_3 = self.get_level_keyWords(level_3, 3)
                        if data_3:
                            data_3 = [item + (key_word_id,) for item in data_3]
                            self.save_data(data_3)
                            print('正在保存 level 3 数据...')

            print('关键词信息采集完毕！')
            self.mysql.close()


# 采集关键词信息（4个榜单数据）
class ALiIndexInfoClawer():

    def __init__(self):
        self.mysql = MysqlDb("localhost", 3306, "root", "123123", "alzs")
        pass

    def save_data(self, data, type):

        if type == 'rise':
            sql = 'insert ignore into alzs_rise_month (keyword_name,trend,keyword_index,url,key_word_id) values (%s,%s,%s,%s,%s) '
        elif type == 'hot':
            sql = 'insert ignore into alzs_hot_month (keyword_name,total,keyword_index,url,key_word_id) values (%s,%s,%s,%s,%s) '
        elif type == 'new':
            sql = 'insert ignore into alzs_new_month (keyword_name,total,rate,url,key_word_id) values (%s,%s,%s,%s,%s) '
        elif type == 'word':
            sql = 'insert ignore into alzs_word_month (keyword_name,total,keyword_index,url,key_word_id) values (%s,%s,%s,%s,%s) '

        self.mysql.insert(sql, data)

        pass

    def get_keyword_id(self):

        # sql = 'select key_word_id from alzs_keyword'
        # sql = 'select key_word_id from alzs_keyword WHERE key_word_id not in (SELECT DISTINCT key_word_id FROM alzs_hot_month)'
        sql = 'select key_word_id from alzs_keyword WHERE key_word_id not in (SELECT  key_word_id FROM alzs_keyword_done)'
        res = self.mysql.select(sql)
        if res:
            key_word_id_list = [item['key_word_id'] for item in res]
        else:
            key_word_id_list = []

        return key_word_id_list

    def request(self, url, referer):

        headers = {
            "Host": "index.1688.com",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
            # "User-Agent": get_useragent(),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": referer,
            "X-Requested-With": "X-Requested-With",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cookie": 'ali_ab=119.123.132.57.1567996618960.2; cna=y6j8FYd0fiACAXd7hDkF1Tr5; _alizs_area_info_=1001; ali_beacon_id=119.123.132.57.1567996623475.834131.1; _alizs_user_type_=purchaser; _alizs_base_attr_type_=0; cookie2=1005e60f5db299a668737cf6a82a0412; t=bed109d421bbaff645dfeb1cdcfbf125; _tb_token_=36ee634586ee5; __cn_logon__=false; h_keys="%u6807%u724c#%u94dd%u5408%u91d1%u95e8%u724c#pvc%u5851%u6599%u6807%u724c"; ad_prefer="2019/09/10 09:13:48"; _alizs_cate_info_=67; alicnweb=touch_tb_at%3D1568080253144; l=cBO1rYllq1RYCsTAmOfahurza77TFQAb8sPzaNbMiICP9pfH5mHGWZUuyG8MCnGVLsayR3-p0rS3BVL3xy4Eh1OmRvipjuGN.; isg=BAoK5EwaYzKzyO-cfn2Nid72W_Cs-45VEXFjdZRBm93oR6sBfIqZZFnxV7Obtwbt'
        }

        try:
            res = requests.get(url, headers=headers, proxies=get_proxy())
        except:
            count = 1
            while count <= 5:
                try:
                    res = requests.get(url, headers=headers, proxies=get_proxy())
                    break
                except:
                    err_info = '__request__ reloading for %d time' % count if count == 1 else '__request__ reloading for %d times' % count
                    print(err_info)
                    count += 1
            if count > 5:
                print("__request__ job failed!")
                return

        return res.json()

    def parse(self, data_source, key_word_id, type):

        data_list = []
        try:
            data = data_source['content']
        except:
            return data_list

        if type == 'rise':
            for item in data:
                keyword = item['keyword']
                trend = item['trend']
                index = item['index']
                url = item['url']
                data_list.append((keyword,trend,index,url,key_word_id))
        elif type == 'hot' or type == 'word':
            for item in data:
                keyword = item['keyword']
                total = item['total']
                index = item['index']
                url = item['url']
                data_list.append((keyword,total,index,url,key_word_id))
        elif type == 'new':
            for item in data:
                keyword = item['keyword']
                total = item['total']
                rate = item['rate']
                url = item['url']
                data_list.append((keyword,total,rate,url,key_word_id))

        return data_list

    def query_keyword(self, key_word_id):

        sql = 'select id from alzs_rise_month WHERE key_word_id = \'%s\' ' % key_word_id
        res_rise = self.mysql.select(sql)
        sql = 'select id from alzs_hot_month WHERE key_word_id = \'%s\' ' % key_word_id
        res_hot = self.mysql.select(sql)
        sql = 'select id from alzs_new_month WHERE key_word_id = \'%s\' ' % key_word_id
        res_new = self.mysql.select(sql)
        sql = 'select id from alzs_word_month WHERE key_word_id = \'%s\' ' % key_word_id
        res_word = self.mysql.select(sql)

        if (res_rise or res_hot or res_new or res_word):
            return True
        else:
            return False

    def main(self):
        try:
            sql = 'insert ignore into alzs_keyword_done (key_word_id) values (%s) '
            url_source = 'http://index.1688.com/alizs/word/listRankType.json?cat={}&rankType={}&period=month'
            key_word_id_list = self.get_keyword_id()
            for key_word_id in key_word_id_list:
                try:
                    self.mysql.insert(sql, [(key_word_id)])
                    flag = self.query_keyword(key_word_id)
                    if not flag:
                        print('*'*50)
                        print('正在采集关键词：', key_word_id)
                        url_rise = url_source.format(key_word_id,'rise')
                        url_hot = url_source.format(key_word_id,'hot')
                        url_new = url_source.format(key_word_id,'new')
                        url_word = url_source.format(key_word_id,'word')

                        # time.sleep(random.randint(1,2))
                        data_rise_source = self.request(url_rise,url_rise)
                        data_hot_source = self.request(url_hot,url_hot)
                        data_new_source = self.request(url_new,url_new)
                        data_word_source = self.request(url_word,url_word)

                        data_rise = self.parse(data_rise_source,key_word_id,'rise')
                        data_hot = self.parse(data_hot_source,key_word_id,'hot')
                        data_new = self.parse(data_new_source,key_word_id,'new')
                        data_word = self.parse(data_word_source,key_word_id,'word')
                        print('关键词信息：\n上升榜：%s\n热搜榜：%s\n转化率榜：%s\n新词榜：%s\n'%(data_rise,data_hot,data_new,data_word))

                        self.save_data(data_rise,'rise')
                        self.save_data(data_hot,'hot')
                        self.save_data(data_new,'new')
                        self.save_data(data_word,'word')

                        print('关键词数据已保存：',key_word_id)
                    else:
                        print('关键词信息已存在：',key_word_id)
                except:
                    pass
            print('数据采集完成！')
        except Exception as err:
            print(err)


# 导出
class Export():

    def __init__(self):
        self.mysql = MysqlDb("localhost", 3306, "root", "123123", "alzs")


    def query_data(self,sql):
        res = self.mysql.select(sql)
        return res


    def get_all_keywords(self):
        sql = 'select * from alzs_keyword WHERE key_word_id not in (SELECT  key_word_id FROM alzs_keyword_done) ORDER BY level,up_level_id '
        res = self.mysql.select(sql)
        return res

    def query_level_info(self,key_word_id,level):

        if level == 1:
            sql = 'SELECT * FROM alzs_keyword WHERE key_word_id = %s ' % key_word_id
        elif level == 2:
            sql = '''SELECT * FROM alzs_keyword WHERE key_word_id in (SELECT up_level_id FROM alzs_keyword WHERE key_word_id = %s ) 
                      UNION 
                      SELECT * FROM alzs_keyword WHERE key_word_id = %s ''' % (key_word_id,key_word_id)
        elif level == 3:
            sql = '''SELECT * FROM alzs_keyword WHERE key_word_id in (SELECT up_level_id FROM alzs_keyword WHERE key_word_id in (SELECT up_level_id FROM alzs_keyword WHERE key_word_id = %s))
                     UNION 
                     SELECT * FROM alzs_keyword WHERE key_word_id in (SELECT up_level_id FROM alzs_keyword WHERE key_word_id = %s)
                     UNION
                     SELECT * FROM alzs_keyword WHERE key_word_id = %s''' % (key_word_id,key_word_id,key_word_id)
        res = self.mysql.select(sql)
        key_word_names = [item['key_word_name'] for item in res]
        if len(key_word_names) == 1:
            level_info = ','.join(key_word_names) + ','*2
        if len(key_word_names) == 2:
            level_info = ','.join(key_word_names) + ','*1
        if len(key_word_names) == 3:
            level_info = ','.join(key_word_names) + ','*0

        return level_info


    def main(self):

        sql = 'SELECT * FROM alzs_{}_month WHERE key_word_id = {}'
        sql_insert = 'insert ignore into alzs_keyword_done (key_word_id) values (%s) '
        keywords_info = self.get_all_keywords()
        with open('alzs_.csv', 'a', encoding='utf-8') as f:
            header = '一级分类' + ',' + '二级分类' + ',' + '三级分类' + ',' + '' + ',' \
                     + '排名' + ',' + '' + ',' + '上升榜' + ',' + '' + ',' \
                     + '' + ',' + '' + ',' + '热搜榜' + ',' + '' + ',' \
                     + '' + ',' + '' + ',' + '转化率榜' + ',' + '' + ',' \
                     + '' + ',' + '' + ',' + '新词榜' + ',' + '' + '\n' \
                                                                '' + ',' + '' + ',' + '' + ',' + '' + ',' \
                     + '' + ',' + '关键词' + ',' + '搜索趋势' + ',' + '搜索指数' + ',' \
                     + '' + ',' + '关键词' + ',' + '搜索指数' + ',' + '全站商品数' + ',' \
                     + '' + ',' + '关键词' + ',' + '搜索转化率' + ',' + '全站商品数' + ',' \
                     + '' + ',' + '关键词' + ',' + '搜索指数' + ',' + '全站商品数' + '\n'

            f.writelines(header)

            for keyword_info in keywords_info:
                key_word_id = keyword_info['key_word_id']
                self.mysql.insert(sql_insert, [(key_word_id)])
                data_rise = self.query_data(sql.format('rise',key_word_id))
                data_hot = self.query_data(sql.format('hot',key_word_id))
                data_new = self.query_data(sql.format('new',key_word_id))
                data_word = self.query_data(sql.format('word',key_word_id))
                if data_rise or data_hot or data_new or data_word:
                    level = keyword_info['level']
                    level_info = self.query_level_info(key_word_id,level)
                    row_num = max([len(data_rise),len(data_hot),len(data_new),len(data_word)])


                    for i in range(row_num):
                        try:
                            rise = data_rise[i]
                            line_rise = rise['keyword_name'] + ',' + rise['trend'] + ',' + str(rise['keyword_index'])
                        except:
                            line_rise = '' + ',' + '' + ',' + ''
                        try:
                            hot = data_hot[i]
                            line_hot = hot['keyword_name'] + ',' + str(hot['keyword_index']) + ',' + str(hot['total'])
                        except:
                            line_hot = '' + ',' + '' + ',' + ''
                        try:
                            new = data_new[i]
                            line_new = new['keyword_name'] + ',' + str(new['rate']) + ',' + str(new['total'])
                        except:
                            line_new = '' + ',' + '' + ',' + ''
                        try:
                            word = data_word[i]
                            line_word = word['keyword_name'] + ',' + str(word['keyword_index']) + ',' + str(word['total'])
                        except:
                            line_word = '' + ',' + '' + ',' + ''

                        line = level_info + ',' + '' + ',' + str(i+1) + ','\
                             + line_rise + ','    + '' + ','\
                             + line_hot + ','    + '' + ','\
                             + line_new + ','    + '' + ','\
                             + line_word + '\n'\

                        f.writelines(line)
                        print(line)

        print('写入完成！')


def get_proxy():
    # 代理服务器
    proxyHost = "http-dyn.abuyun.com"
    proxyPort = "9020"

    # 代理隧道验证信息
    proxyUser = "HIL217ZFCDHGJ6FD"
    proxyPass = "1375697BCADCD8BB"

    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": proxyHost,
        "port": proxyPort,
        "user": proxyUser,
        "pass": proxyPass,
    }

    proxies = {
        # "http": proxyMeta,
        # "https": proxyMeta,
    }

    return proxies


if __name__ == '__main__':

    # obj_1 = ALiIndexClawer()
    # obj_1.main()

    # obj_2 = ALiIndexInfoClawer()
    # obj_2.main()

    export = Export()
    export.main()