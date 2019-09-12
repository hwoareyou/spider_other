# -*- coding: utf-8 -*-
# @Author   : liu
# 加入日志
from selenium.webdriver import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
import time
import json,re,os,urllib.request,datetime,random,requests,sys,socket
from lxml import etree
from mysql_utils.mysql_db import MysqlDb
from baidu_OCR import recognition_character
import threading
from threading import Thread
from queue import Queue
from tengxun_OCR import Ocr
from selenium.webdriver.chrome.options import Options
from log_utils.mylog import Mylog
from PIL import Image
import traceback
import warnings
warnings.filterwarnings('ignore')





def request(url):
    print('采集商品：',url)
    headers = {
        "Host": "www.portlong.com",
        "Connection": "keep-alive",
        # "Cache-Control": " max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
        # "User-Agent": get_useragent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Referer": "http://www.wittysafe.com/s/index.php/category?cid=1",
        # "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        # "Cookie": "ali_apache_id=10.103.166.17.1564972496692.314349.2; cna=MJHOFZFqTGQCAXd7hW7RbDW+; aep_common_f=s2S5iSRoLL/xeUxx4lElCqPxKHfC+NE2cmbLRdr/Q/pJRG74xU6jeg==; _ga=GA1.2.86522749.1564975934; _bl_uid=I4j9byz9xa2unv9jIi1jznwuLg8z; _uab_collina=156497664328006922363972; intl_locale=en_US; _gid=GA1.2.1446018449.1565658388; _fbp=fb.1.1565658388480.45105460; _hvn_login=13; aep_usuc_t=ber_l=A0; ali_apache_tracktmp=W_signed=Y; acs_usuc_t=acs_rt=ee7529d5ebb1455499561f995172aa0e&x_csrf=14pvv1zifx5zg; XSRF-TOKEN=b126ff26-c8f3-49b3-af46-833c1953d65a; JSESSIONID=11DABEF03410B74E0794AFDDFA7D5492; aep_history=keywords%5E%0Akeywords%09%0A%0Aproduct_selloffer%5E%0Aproduct_selloffer%0933051278895%0932909510019%0932810934777%0933035526367%0932838346044%0932892901920%0932996370174%0932955810487; _mle_tmp_enc0=Ey%2Fp8LswzxA3J47VsqxI%2B6mLKOTiqa5Lpi%2FMeleRq7d%2FDnMh%2BuxNdHkBA%2FvPee7Uwvd3%2BtwJIxFz83XYf1rXUOyeZSYZG7Zn7S34c%2FoOYeNNRL7NmRCDbYGKLNxVgInG; _m_h5_tk=2ce73230c0370b7114c70ef7a4989517_1565689765542; _m_h5_tk_enc=9081ea4fa22dafd65332d979c8c96db9; xman_us_t=x_lid=cn261343849rwfae&sign=y&x_user=E9UJox29XphkMz2HEDjFlkf+1oJDCte2RtkVXlSkbFM=&ctoken=qt8q29fiv41a&need_popup=y&l_source=aliexpress; xman_f=xoFBv51OtzNJvazeVF88u/4qcPNkY7xaV18nHEnDcZPHhdabyS5869iklKAM+5NJ1t6Y8bmuaryeVhrNhRca9qp3Ov8BBGHdMtfmHQrXwG9EmxJZNbUkP8JQq632/7i6JFmPPPVJusijX/XGve1EGcJo/JjHxQPAveziJI2vUWOe0NGTo2iIGRqBZfJhJobgaITZtk5wsv0I0REE/0A8aOlGNc9O9lDOFoiMcyT1VdO8hjRSroMegOSOlsJq0fjn3aGJNw74tIQK2sa+kmYw5ylmWC4/c+M/yZzrRYfHNbVzvHEWZIdK+glM6r+eeAiaF5CtQ5Tf7vh+r7k8zmgvDGP7UE3gw1ZD02hpeaekyNeU/PwXl5AWKzKwaCrHN+w0SH/bvfDCoNf7StW9++HlQeewXenJt5UlWI3T2UsAopU=; xman_us_f=zero_order=y&x_locale=en_US&x_l=1&last_popup_time=1564975927101&x_user=CN|CN|shopper|ifm|1861063758&no_popup_today=n; aep_usuc_f=site=glo&c_tp=USD&x_alimid=1861063758&isb=y&region=CN&b_locale=en_US; _gat=1; intl_common_forever=rXRSHRjB+kmZcdGqwzcSzbrzJ65rv9Y3OhEG3xK6JWFQSJ5a5GvVyQ==; ali_apache_track=mt=1|ms=|mid=cn261343849rwfae; xman_t=29QUM9dsDjrgXo5QP4S3GETnmYEH4zs+lH1fJJfUu4bYz2qXRJMollWOXCPH4gPj3uJUfYnSYqXyfyoahQVScBngWEx7zZEwgbNUoBtVQaSBVJT4q0VMBN6j7U9cdLBC/RWRAuaAVTlPAGa8HkfoEkamL2HVpubP97AE8uzjWNr84lDUS20H64+Xr2N/0ioXnTLMwAy1cUwZkWvp0PYtWqEx/97eHKIGdK9qFdAy6kuC645iT6GS+7xT7ik3uQlnJCUj/QhhHbxfrApUsH3U9j0c7+IduJ6lJY+rKNg7dM04x3gQXcgPqI3+wEx/KXerAGVzOwhH5rEktu4eipQEV3VO3z5HPqgMw5REYhc3g4aigO8Q4aSaq733z2LfYYQv5jnTlB7diXvdYhpmWxiiBD0PJF4pU5sGPjZ+yzgIos00/yMQY2i9sK4Y+IyDZjltS6okF/1h8zWt7Z6eLO6Upfpwqj8O6ioRBey+Qf1TN1uw5R8NdoYSLhmdcsOnnPSzYtr7QZeAy8Z8nuUCryqRc5IIHSAnReMd9VXmR/0YdYpNMX0vi8kcPq32HvQjK3mavAe7RI9dAAT/AcZ0GN+L1XDplm2mDyswWQXSVYssLjXfPKtd59sIrSdBwAGHo6o40uK3etDe6R1H97cyal/bgA==; isg=BNzcYxzx7b00n5mE26JrxgmtrfoiRYEDyA5DLrbc4kcnAX2L3mbgD9_zYSlcibjX; l=cBxuVc5qqMecYzDSBOfwSQKbLm7TnIOb8sPP2NRG-ICPO_55k6MPWZe2eFYXCnGVLsM2R37uMFCUBWYajyUIhHZY1KFS4-9N."
    }
    res = requests.get(url,headers=headers)
    html = res.text

    return html


def get_comments(sellItemId):


    headers = {
        "Host": "www.portlong.com",
        "Connection": "keep-alive",
        # "Cache-Control": " max-age=0",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
        # "User-Agent": get_useragent(),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "http://www.portlong.com/e/index.php/p/" + sellItemId,
        # "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        # "Cookie": "ali_apache_id=10.103.166.17.1564972496692.314349.2; cna=MJHOFZFqTGQCAXd7hW7RbDW+; aep_common_f=s2S5iSRoLL/xeUxx4lElCqPxKHfC+NE2cmbLRdr/Q/pJRG74xU6jeg==; _ga=GA1.2.86522749.1564975934; _bl_uid=I4j9byz9xa2unv9jIi1jznwuLg8z; _uab_collina=156497664328006922363972; intl_locale=en_US; _gid=GA1.2.1446018449.1565658388; _fbp=fb.1.1565658388480.45105460; _hvn_login=13; aep_usuc_t=ber_l=A0; ali_apache_tracktmp=W_signed=Y; acs_usuc_t=acs_rt=ee7529d5ebb1455499561f995172aa0e&x_csrf=14pvv1zifx5zg; XSRF-TOKEN=b126ff26-c8f3-49b3-af46-833c1953d65a; JSESSIONID=11DABEF03410B74E0794AFDDFA7D5492; aep_history=keywords%5E%0Akeywords%09%0A%0Aproduct_selloffer%5E%0Aproduct_selloffer%0933051278895%0932909510019%0932810934777%0933035526367%0932838346044%0932892901920%0932996370174%0932955810487; _mle_tmp_enc0=Ey%2Fp8LswzxA3J47VsqxI%2B6mLKOTiqa5Lpi%2FMeleRq7d%2FDnMh%2BuxNdHkBA%2FvPee7Uwvd3%2BtwJIxFz83XYf1rXUOyeZSYZG7Zn7S34c%2FoOYeNNRL7NmRCDbYGKLNxVgInG; _m_h5_tk=2ce73230c0370b7114c70ef7a4989517_1565689765542; _m_h5_tk_enc=9081ea4fa22dafd65332d979c8c96db9; xman_us_t=x_lid=cn261343849rwfae&sign=y&x_user=E9UJox29XphkMz2HEDjFlkf+1oJDCte2RtkVXlSkbFM=&ctoken=qt8q29fiv41a&need_popup=y&l_source=aliexpress; xman_f=xoFBv51OtzNJvazeVF88u/4qcPNkY7xaV18nHEnDcZPHhdabyS5869iklKAM+5NJ1t6Y8bmuaryeVhrNhRca9qp3Ov8BBGHdMtfmHQrXwG9EmxJZNbUkP8JQq632/7i6JFmPPPVJusijX/XGve1EGcJo/JjHxQPAveziJI2vUWOe0NGTo2iIGRqBZfJhJobgaITZtk5wsv0I0REE/0A8aOlGNc9O9lDOFoiMcyT1VdO8hjRSroMegOSOlsJq0fjn3aGJNw74tIQK2sa+kmYw5ylmWC4/c+M/yZzrRYfHNbVzvHEWZIdK+glM6r+eeAiaF5CtQ5Tf7vh+r7k8zmgvDGP7UE3gw1ZD02hpeaekyNeU/PwXl5AWKzKwaCrHN+w0SH/bvfDCoNf7StW9++HlQeewXenJt5UlWI3T2UsAopU=; xman_us_f=zero_order=y&x_locale=en_US&x_l=1&last_popup_time=1564975927101&x_user=CN|CN|shopper|ifm|1861063758&no_popup_today=n; aep_usuc_f=site=glo&c_tp=USD&x_alimid=1861063758&isb=y&region=CN&b_locale=en_US; _gat=1; intl_common_forever=rXRSHRjB+kmZcdGqwzcSzbrzJ65rv9Y3OhEG3xK6JWFQSJ5a5GvVyQ==; ali_apache_track=mt=1|ms=|mid=cn261343849rwfae; xman_t=29QUM9dsDjrgXo5QP4S3GETnmYEH4zs+lH1fJJfUu4bYz2qXRJMollWOXCPH4gPj3uJUfYnSYqXyfyoahQVScBngWEx7zZEwgbNUoBtVQaSBVJT4q0VMBN6j7U9cdLBC/RWRAuaAVTlPAGa8HkfoEkamL2HVpubP97AE8uzjWNr84lDUS20H64+Xr2N/0ioXnTLMwAy1cUwZkWvp0PYtWqEx/97eHKIGdK9qFdAy6kuC645iT6GS+7xT7ik3uQlnJCUj/QhhHbxfrApUsH3U9j0c7+IduJ6lJY+rKNg7dM04x3gQXcgPqI3+wEx/KXerAGVzOwhH5rEktu4eipQEV3VO3z5HPqgMw5REYhc3g4aigO8Q4aSaq733z2LfYYQv5jnTlB7diXvdYhpmWxiiBD0PJF4pU5sGPjZ+yzgIos00/yMQY2i9sK4Y+IyDZjltS6okF/1h8zWt7Z6eLO6Upfpwqj8O6ioRBey+Qf1TN1uw5R8NdoYSLhmdcsOnnPSzYtr7QZeAy8Z8nuUCryqRc5IIHSAnReMd9VXmR/0YdYpNMX0vi8kcPq32HvQjK3mavAe7RI9dAAT/AcZ0GN+L1XDplm2mDyswWQXSVYssLjXfPKtd59sIrSdBwAGHo6o40uK3etDe6R1H97cyal/bgA==; isg=BNzcYxzx7b00n5mE26JrxgmtrfoiRYEDyA5DLrbc4kcnAX2L3mbgD9_zYSlcibjX; l=cBxuVc5qqMecYzDSBOfwSQKbLm7TnIOb8sPP2NRG-ICPO_55k6MPWZe2eFYXCnGVLsM2R37uMFCUBWYajyUIhHZY1KFS4-9N."
    }


    # comm_id = item['id']
    # comm_itemId = item['itemId']
    # comm_mobile = item['mobile']
    # comm_userName = item['userName']
    # comm_rate = item['rate']
    # comm_icontent = item['content']
    # comm_time = item['time']

    comm_data = []
    for pageNumber in range(1,1000):
        post_data = {
            "sid": sellItemId,
            "pageNumber": str(pageNumber),
            "pageSize": "20",
        }
        url = 'http://www.portlong.com/e/index.php/item/reviews?sid={0}&pageNumber={1}&pageSize=20'.format(sellItemId,pageNumber)
        res = requests.post(url,headers=headers,data=post_data)
        if len(res.json()['data']) < 20 :
            for item in res.json()['data']:
                try:
                    pic_url = re.search(r'src="(.+?\.[pngj]+)',item['content']).group(1)
                except:
                    pic_url = ''
                item['pic_url'] = pic_url
                comm_data.append(item)
            break
        else:
            for item in res.json()['data']:
                try:
                    pic_url = re.search(r'src="(.+?png)',item['content']).group(1)
                except:
                    pic_url = ''
                item['pic_url'] = pic_url
                comm_data.append(item)

    return comm_data





    pass

def parse(html,sellItemId):

    product_info = {}
    html = etree.HTML(html)

    product_info['sellItemId'] = sellItemId

    title = str(html.xpath('string(//div[@class="reveal_title"])')).strip()
    product_info['title'] = title

    introduction = str(html.xpath('string(//div[@class="vat cl"])')).strip()
    product_info['introduction'] = introduction


    price = str(html.xpath('string(//div[@class="price"])')).strip().replace('NT$ ','') # 促销价
    marketPrice = str(html.xpath('string(//div[@class="marketPrice"]/b)')).strip().replace('NT$ ','') # 原价
    product_info['price'] = float(price)
    product_info['marketPrice'] = float(marketPrice)


    pic = str(html.xpath('//*[@id="page-scroll-content"]/div[1]/div[1]/div/div/img/@src')[0])
    product_info['pic'] = pic

    content = html.xpath('//div[@class="content"]//img')
    content_pics = [dict(item.attrib)['data-original'] for item in content]
    product_info['content_pics'] = content_pics

    comm_data = get_comments(sellItemId)
    product_info['comm_data'] = comm_data

    attrs = get_attr(sellItemId)
    product_info['attrs'] = attrs

    return product_info

def get_attr(sellItemId):

    headers = {
        "Host": "www.portlong.com",
        "Connection": "keep-alive",
        # "Cache-Control": " max-age=0",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
        # "User-Agent": get_useragent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Referer": "http://www.portlong.com/e/index.php/p/" + sellItemId,
        # "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        # "Cookie": "ali_apache_id=10.103.166.17.1564972496692.314349.2; cna=MJHOFZFqTGQCAXd7hW7RbDW+; aep_common_f=s2S5iSRoLL/xeUxx4lElCqPxKHfC+NE2cmbLRdr/Q/pJRG74xU6jeg==; _ga=GA1.2.86522749.1564975934; _bl_uid=I4j9byz9xa2unv9jIi1jznwuLg8z; _uab_collina=156497664328006922363972; intl_locale=en_US; _gid=GA1.2.1446018449.1565658388; _fbp=fb.1.1565658388480.45105460; _hvn_login=13; aep_usuc_t=ber_l=A0; ali_apache_tracktmp=W_signed=Y; acs_usuc_t=acs_rt=ee7529d5ebb1455499561f995172aa0e&x_csrf=14pvv1zifx5zg; XSRF-TOKEN=b126ff26-c8f3-49b3-af46-833c1953d65a; JSESSIONID=11DABEF03410B74E0794AFDDFA7D5492; aep_history=keywords%5E%0Akeywords%09%0A%0Aproduct_selloffer%5E%0Aproduct_selloffer%0933051278895%0932909510019%0932810934777%0933035526367%0932838346044%0932892901920%0932996370174%0932955810487; _mle_tmp_enc0=Ey%2Fp8LswzxA3J47VsqxI%2B6mLKOTiqa5Lpi%2FMeleRq7d%2FDnMh%2BuxNdHkBA%2FvPee7Uwvd3%2BtwJIxFz83XYf1rXUOyeZSYZG7Zn7S34c%2FoOYeNNRL7NmRCDbYGKLNxVgInG; _m_h5_tk=2ce73230c0370b7114c70ef7a4989517_1565689765542; _m_h5_tk_enc=9081ea4fa22dafd65332d979c8c96db9; xman_us_t=x_lid=cn261343849rwfae&sign=y&x_user=E9UJox29XphkMz2HEDjFlkf+1oJDCte2RtkVXlSkbFM=&ctoken=qt8q29fiv41a&need_popup=y&l_source=aliexpress; xman_f=xoFBv51OtzNJvazeVF88u/4qcPNkY7xaV18nHEnDcZPHhdabyS5869iklKAM+5NJ1t6Y8bmuaryeVhrNhRca9qp3Ov8BBGHdMtfmHQrXwG9EmxJZNbUkP8JQq632/7i6JFmPPPVJusijX/XGve1EGcJo/JjHxQPAveziJI2vUWOe0NGTo2iIGRqBZfJhJobgaITZtk5wsv0I0REE/0A8aOlGNc9O9lDOFoiMcyT1VdO8hjRSroMegOSOlsJq0fjn3aGJNw74tIQK2sa+kmYw5ylmWC4/c+M/yZzrRYfHNbVzvHEWZIdK+glM6r+eeAiaF5CtQ5Tf7vh+r7k8zmgvDGP7UE3gw1ZD02hpeaekyNeU/PwXl5AWKzKwaCrHN+w0SH/bvfDCoNf7StW9++HlQeewXenJt5UlWI3T2UsAopU=; xman_us_f=zero_order=y&x_locale=en_US&x_l=1&last_popup_time=1564975927101&x_user=CN|CN|shopper|ifm|1861063758&no_popup_today=n; aep_usuc_f=site=glo&c_tp=USD&x_alimid=1861063758&isb=y&region=CN&b_locale=en_US; _gat=1; intl_common_forever=rXRSHRjB+kmZcdGqwzcSzbrzJ65rv9Y3OhEG3xK6JWFQSJ5a5GvVyQ==; ali_apache_track=mt=1|ms=|mid=cn261343849rwfae; xman_t=29QUM9dsDjrgXo5QP4S3GETnmYEH4zs+lH1fJJfUu4bYz2qXRJMollWOXCPH4gPj3uJUfYnSYqXyfyoahQVScBngWEx7zZEwgbNUoBtVQaSBVJT4q0VMBN6j7U9cdLBC/RWRAuaAVTlPAGa8HkfoEkamL2HVpubP97AE8uzjWNr84lDUS20H64+Xr2N/0ioXnTLMwAy1cUwZkWvp0PYtWqEx/97eHKIGdK9qFdAy6kuC645iT6GS+7xT7ik3uQlnJCUj/QhhHbxfrApUsH3U9j0c7+IduJ6lJY+rKNg7dM04x3gQXcgPqI3+wEx/KXerAGVzOwhH5rEktu4eipQEV3VO3z5HPqgMw5REYhc3g4aigO8Q4aSaq733z2LfYYQv5jnTlB7diXvdYhpmWxiiBD0PJF4pU5sGPjZ+yzgIos00/yMQY2i9sK4Y+IyDZjltS6okF/1h8zWt7Z6eLO6Upfpwqj8O6ioRBey+Qf1TN1uw5R8NdoYSLhmdcsOnnPSzYtr7QZeAy8Z8nuUCryqRc5IIHSAnReMd9VXmR/0YdYpNMX0vi8kcPq32HvQjK3mavAe7RI9dAAT/AcZ0GN+L1XDplm2mDyswWQXSVYssLjXfPKtd59sIrSdBwAGHo6o40uK3etDe6R1H97cyal/bgA==; isg=BNzcYxzx7b00n5mE26JrxgmtrfoiRYEDyA5DLrbc4kcnAX2L3mbgD9_zYSlcibjX; l=cBxuVc5qqMecYzDSBOfwSQKbLm7TnIOb8sPP2NRG-ICPO_55k6MPWZe2eFYXCnGVLsM2R37uMFCUBWYajyUIhHZY1KFS4-9N."
    }
    post_data = {
        "id":sellItemId
    }

    url = 'http://www.portlong.com/e/index.php/order?id=' + sellItemId
    res = requests.post(url,headers=headers,data=post_data)
    html = etree.HTML(res.text)
    # attrs = [dict(item.attrib) for item in html.xpath('//ul[@class="sys_spec_text"]/li')]
    attrs_data = html.xpath('//div[@class="spec-select"]//dd')
    i = 0
    attrs = []
    for item in attrs_data:
        attr_name = str(item.xpath('string(./div)')).strip().replace(':', '')
        if not attr_name:
            i += 1
            attr_name = '属性' + str(i)
        attr_value_list = []
        for li_item in item.xpath('.//li'):
            attr_value = str(li_item.xpath('./a/text()')[0])
            if li_item.xpath('./img/@src'):
                attr_url = str(li_item.xpath('./img/@src')[0])
            else:
                attr_url = ''
            attr_value_list.append({'attr_value':attr_value,'attr_url':attr_url})
        attrs.append({'attr_name':attr_name,'attr_value':attr_value_list})
    return attrs

def get_product_by_category(category_id,pageNumber):

    headers = {
        "Host": "www.portlong.com",
        "Connection": "keep-alive",
        # "Cache-Control": " max-age=0",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
        # "User-Agent": get_useragent(),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "http://www.portlong.com/e/index.php/item/itemlist?cid="+str(category_id),
        # "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        # "Cookie": "ali_apache_id=10.103.166.17.1564972496692.314349.2; cna=MJHOFZFqTGQCAXd7hW7RbDW+; aep_common_f=s2S5iSRoLL/xeUxx4lElCqPxKHfC+NE2cmbLRdr/Q/pJRG74xU6jeg==; _ga=GA1.2.86522749.1564975934; _bl_uid=I4j9byz9xa2unv9jIi1jznwuLg8z; _uab_collina=156497664328006922363972; intl_locale=en_US; _gid=GA1.2.1446018449.1565658388; _fbp=fb.1.1565658388480.45105460; _hvn_login=13; aep_usuc_t=ber_l=A0; ali_apache_tracktmp=W_signed=Y; acs_usuc_t=acs_rt=ee7529d5ebb1455499561f995172aa0e&x_csrf=14pvv1zifx5zg; XSRF-TOKEN=b126ff26-c8f3-49b3-af46-833c1953d65a; JSESSIONID=11DABEF03410B74E0794AFDDFA7D5492; aep_history=keywords%5E%0Akeywords%09%0A%0Aproduct_selloffer%5E%0Aproduct_selloffer%0933051278895%0932909510019%0932810934777%0933035526367%0932838346044%0932892901920%0932996370174%0932955810487; _mle_tmp_enc0=Ey%2Fp8LswzxA3J47VsqxI%2B6mLKOTiqa5Lpi%2FMeleRq7d%2FDnMh%2BuxNdHkBA%2FvPee7Uwvd3%2BtwJIxFz83XYf1rXUOyeZSYZG7Zn7S34c%2FoOYeNNRL7NmRCDbYGKLNxVgInG; _m_h5_tk=2ce73230c0370b7114c70ef7a4989517_1565689765542; _m_h5_tk_enc=9081ea4fa22dafd65332d979c8c96db9; xman_us_t=x_lid=cn261343849rwfae&sign=y&x_user=E9UJox29XphkMz2HEDjFlkf+1oJDCte2RtkVXlSkbFM=&ctoken=qt8q29fiv41a&need_popup=y&l_source=aliexpress; xman_f=xoFBv51OtzNJvazeVF88u/4qcPNkY7xaV18nHEnDcZPHhdabyS5869iklKAM+5NJ1t6Y8bmuaryeVhrNhRca9qp3Ov8BBGHdMtfmHQrXwG9EmxJZNbUkP8JQq632/7i6JFmPPPVJusijX/XGve1EGcJo/JjHxQPAveziJI2vUWOe0NGTo2iIGRqBZfJhJobgaITZtk5wsv0I0REE/0A8aOlGNc9O9lDOFoiMcyT1VdO8hjRSroMegOSOlsJq0fjn3aGJNw74tIQK2sa+kmYw5ylmWC4/c+M/yZzrRYfHNbVzvHEWZIdK+glM6r+eeAiaF5CtQ5Tf7vh+r7k8zmgvDGP7UE3gw1ZD02hpeaekyNeU/PwXl5AWKzKwaCrHN+w0SH/bvfDCoNf7StW9++HlQeewXenJt5UlWI3T2UsAopU=; xman_us_f=zero_order=y&x_locale=en_US&x_l=1&last_popup_time=1564975927101&x_user=CN|CN|shopper|ifm|1861063758&no_popup_today=n; aep_usuc_f=site=glo&c_tp=USD&x_alimid=1861063758&isb=y&region=CN&b_locale=en_US; _gat=1; intl_common_forever=rXRSHRjB+kmZcdGqwzcSzbrzJ65rv9Y3OhEG3xK6JWFQSJ5a5GvVyQ==; ali_apache_track=mt=1|ms=|mid=cn261343849rwfae; xman_t=29QUM9dsDjrgXo5QP4S3GETnmYEH4zs+lH1fJJfUu4bYz2qXRJMollWOXCPH4gPj3uJUfYnSYqXyfyoahQVScBngWEx7zZEwgbNUoBtVQaSBVJT4q0VMBN6j7U9cdLBC/RWRAuaAVTlPAGa8HkfoEkamL2HVpubP97AE8uzjWNr84lDUS20H64+Xr2N/0ioXnTLMwAy1cUwZkWvp0PYtWqEx/97eHKIGdK9qFdAy6kuC645iT6GS+7xT7ik3uQlnJCUj/QhhHbxfrApUsH3U9j0c7+IduJ6lJY+rKNg7dM04x3gQXcgPqI3+wEx/KXerAGVzOwhH5rEktu4eipQEV3VO3z5HPqgMw5REYhc3g4aigO8Q4aSaq733z2LfYYQv5jnTlB7diXvdYhpmWxiiBD0PJF4pU5sGPjZ+yzgIos00/yMQY2i9sK4Y+IyDZjltS6okF/1h8zWt7Z6eLO6Upfpwqj8O6ioRBey+Qf1TN1uw5R8NdoYSLhmdcsOnnPSzYtr7QZeAy8Z8nuUCryqRc5IIHSAnReMd9VXmR/0YdYpNMX0vi8kcPq32HvQjK3mavAe7RI9dAAT/AcZ0GN+L1XDplm2mDyswWQXSVYssLjXfPKtd59sIrSdBwAGHo6o40uK3etDe6R1H97cyal/bgA==; isg=BNzcYxzx7b00n5mE26JrxgmtrfoiRYEDyA5DLrbc4kcnAX2L3mbgD9_zYSlcibjX; l=cBxuVc5qqMecYzDSBOfwSQKbLm7TnIOb8sPP2NRG-ICPO_55k6MPWZe2eFYXCnGVLsM2R37uMFCUBWYajyUIhHZY1KFS4-9N."
    }
    post_data = {
        "cid":str(category_id),
        "pageNumber":str(pageNumber),
        "pageSize":"20",
    }
    url = 'http://www.portlong.com/e/index.php/item/search?cid={}&pageNumber={}&pageSize=20'.format(str(category_id),str(pageNumber))
    res = requests.post(url,headers=headers,data=post_data)
    return res.json()

def save_imgDir(img_list):

    for item in img_list:
        img_dir = 'upload' + item['img_dir'].split('upload')[1]
        pic_name = re.search(r'/\d+/(.+)\.jpg', item['img_dir']).group(1)
        sql = 'insert into sys_album_picture (album_id,pic_name,pic_tag,pic_cover,pic_cover_big,pic_cover_mid,pic_cover_small,pic_cover_micro) ' \
              'select \"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\" from dual WHERE NOT  EXISTS  (SELECT pic_id from sys_album_picture WHERE pic_name = \"%s\" ) ' \
              % (31, pic_name, pic_name, img_dir, img_dir, img_dir, img_dir, img_dir, pic_name)
        cur = mysql.mysql.cursor()
        cur.execute(sql)
        cur.execute('commit')
    cur.close()

    pass

def get_pic_id(sellItemId):
    sql = 'select pic_id from sys_album_picture WHERE pic_name LIKE \"%s\" ' % (sellItemId + '_pic_%')
    pic_id = mysql.select(sql)
    return pic_id

def save_img(product_info,sellItemId):

    dir = 'D:/PHPTutorial/WWW/333.com/upload/goods/20190825/'
    if not os.path.exists(dir):
        os.makedirs(dir)


    img_list = []

    # 主图
    pic = product_info['pic']
    img_dir = dir + sellItemId +'_pic_1.jpg'
    img_list.append({'img_url': pic, 'img_dir': img_dir})

    # 属性图
    attrs = product_info['attrs']
    i = 1
    for item in attrs:
        for attr in item['attr_value']:
            img_url = attr['attr_url']
            if img_url:
                i += 1
                img_dir = dir + sellItemId + '_pic_' + str(i) +'.jpg'
                img_list.append({'img_url': img_url, 'img_dir': img_dir})

    # 保存图片本地路径
    save_imgDir(img_list)


    # 描述图
    content_pics = product_info['content_pics']
    description_list = []
    for i in range(len(content_pics)):
        img_dir = dir + sellItemId +'_description_' + str(i) + '.jpg'
        img_list.append({'img_url': content_pics[i], 'img_dir': img_dir})
        description_list.append({'img_url': content_pics[i], 'img_dir': img_dir})

    product_info['description_list'] = description_list

    # 评论图
    comm_data = product_info['comm_data']
    for item in comm_data:
        if item['pic_url']:
            img_dir = dir + sellItemId + '_comment_' + str(item['id']) + '.jpg'
            save_dir = '/upload' + img_dir.split('upload')[1]
            item['content_sub'] = item['content'].replace(item['pic_url'],save_dir)
            img_list.append({'img_url': item['pic_url'], 'img_dir': img_dir})

    product_info['comm_data'] = comm_data

    for img_data in img_list:
        img_url = img_data['img_url']
        if 'http' not in img_url:
            img_url = 'http:' + img_url
            # continue
        img_dir = img_data['img_dir']

        # urllib.request.urlretrieve(img_url, img_dir)
        try:
            r = requests.get(img_url,timeout=30)
            with open(img_dir, 'wb') as f:
                f.write(r.content)
        except:
            count = 1
            while count <= 5:
                try:
                    r = requests.get(img_url, timeout=30)
                    with open(img_dir, 'wb') as f:
                        f.write(r.content)
                    break
                except:
                    err_info = 'save_img reloading for %d time' % count if count == 1 else 'save_img reloading for %d times' % count
                    print(err_info)
                    count += 1
            if count > 5:
                print("save_img job failed!")

def get_category_id(category_name):

    sql = 'insert into ns_goods_category (category_name,short_name,level,is_visible,keywords) select \"%s\",\"%s\",\"%s\",\"%s\",\"%s\" from dual WHERE NOT  EXISTS  (SELECT category_id from ns_goods_category WHERE category_name = \"%s\" ) ' \
          % (category_name, category_name, 1,1,category_name,category_name)
    cur = mysql.mysql.cursor()
    cur.execute(sql)
    cur.execute('commit')
    cur.close()
    sql = 'select category_id from ns_goods_category WHERE category_name = \"%s\" ' % category_name
    category_id = mysql.select(sql)[0]['category_id']
    return category_id

def get_attr_name_id(attr_name):

    sql = 'insert into ns_goods_spec (spec_name,is_visible,show_type,is_screen) select \"%s\",\"%s\",\"%s\",\"%s\" from dual WHERE NOT  EXISTS  (SELECT spec_id from ns_goods_spec WHERE spec_name = \"%s\" ) ' \
          % (attr_name, 1, 1, 1, attr_name)
    cur = mysql.mysql.cursor()
    cur.execute(sql)
    cur.execute('commit')
    cur.close()
    sql = 'select spec_id from ns_goods_spec WHERE spec_name = \"%s\" ' % attr_name
    attr_name_id = mysql.select(sql)[0]['spec_id']
    return  attr_name_id

def get_attr_value_id(attr_name_id,attr_value):

    sql = 'insert into ns_goods_spec_value (spec_id,spec_value_name,is_visible) select \"%s\",\"%s\",\"%s\" from dual WHERE NOT  EXISTS  (SELECT spec_id from ns_goods_spec_value WHERE spec_id = \"%s\" AND  spec_value_name = \"%s\") ' \
          % (attr_name_id, attr_value, 1, attr_name_id, attr_value)
    cur = mysql.mysql.cursor()
    cur.execute(sql)
    cur.execute('commit')
    cur.close()
    sql = 'select spec_id,spec_value_id from ns_goods_spec_value WHERE spec_id = \"%s\" AND  spec_value_name = \"%s\" ' % (attr_name_id, attr_value)
    goods_spec_id = str(mysql.select(sql)[0]['spec_id']) + ':' + str(mysql.select(sql)[0]['spec_value_id'])

    return goods_spec_id

def save_goods_sku(product_info,goods_id,attr_value_id,attr_value):

    sql = 'insert into ns_goods_sku (goods_id,sku_name,attr_value_items,attr_value_items_format,market_price,price) select \"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"' \
          ' from dual WHERE NOT  EXISTS  (SELECT sku_id from ns_goods_sku WHERE goods_id = \"%s\" AND  sku_name = \"%s\") ' \
          % (goods_id,attr_value,attr_value_id,attr_value_id,product_info['marketPrice'],product_info['price'],goods_id,attr_value)
    cur = mysql.mysql.cursor()
    cur.execute(sql)
    cur.execute('commit')
    # cur.close()

    pass

def save_comment(goods_id,product_info):
    comm_data = product_info['comm_data']
    for item in comm_data:
        member_name = item['userName']
        if 'content_sub' in list(item.keys()):
            content = item['content_sub']
        else:
            content = item['content']
        sql = 'insert into ns_goods_evaluate (goods_id,goods_name,content,member_name) select \"%s\",\"%s\",\'%s\',\"%s\" from dual WHERE NOT  EXISTS  (SELECT id from ns_goods_evaluate WHERE goods_id = \"%s\"  and content = \'%s\' AND member_name = \"%s\" ) ' \
              % (goods_id, product_info['title'], content, member_name, goods_id, content, member_name)
        cur = mysql.mysql.cursor()
        cur.execute(sql)
        cur.execute('commit')

    pass

def save(product_info, sellItemId, category_name):


    sql = 'select goods_id from ns_goods WHERE goods_name = \"%s\" ' % product_info['title']
    res = mysql.select(sql)

    # 商品不存在则写入
    if not res:

        # 保存图片
        save_img(product_info, sellItemId)

        # 保存并获取分类id
        category_id = get_category_id(category_name)

        # 获取图片id
        pic_id = get_pic_id(sellItemId)
        picture = pic_id[0]['pic_id']
        img_id_array = ','.join([str(item['pic_id']) for item in pic_id])

        # 拼接描述html
        description_list = product_info['description_list']
        description = '<p>'
        for item in description_list:
            description += ' <img src="%s" />' % ('/upload' + item['img_dir'].split('upload')[1])
        description += '</p>'

        sql = '''insert into ns_goods (shop_id,goods_name,category_id,goods_type,market_price,price,promotion_price,stock,picture,keywords,introduction,description,
                          is_stock_visible,state,img_id_array,shipping_fee_type,presell_delivery_type)
                          values
                          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        value = [(0,product_info['title'], category_id, 1, product_info['marketPrice'], product_info['price'],
                  product_info['price'], 0, picture, '', product_info['introduction'],
                  description, 1, 1, img_id_array, 3, 1)]
        mysql.insert(sql, value)

        sql = 'select goods_id from ns_goods WHERE  goods_name = \"%s\" ' % product_info['title']
        goods_id = mysql.select(sql)[0]['goods_id']
        #
        # 保存评论
        save_comment(goods_id, product_info)

        # 属性信息
        attrs = product_info['attrs']
        if attrs:
            goods_spec_format = []
            for item in attrs:
                attr_name = item['attr_name']
                attr_name_id = get_attr_name_id(attr_name)

                goods_spec_format_sub = {}
                goods_spec_format_sub_value = []
                goods_spec_id = []
                for attr_value in item['attr_value']:
                    goods_spec_format_sub_value_item = {}
                    attr_value = attr_value['attr_value']
                    attr_value_id= get_attr_value_id(attr_name_id,attr_value)

                    # 保存sku
                    save_goods_sku(product_info, goods_id,attr_value_id,attr_value)

                    goods_spec_id.append(attr_value_id)
                    goods_spec_format_sub_value_item['spec_value_name'] = attr_value
                    goods_spec_format_sub_value_item['spec_name'] = attr_name
                    goods_spec_format_sub_value_item['spec_value_id'] = attr_value_id
                    goods_spec_format_sub_value_item['spec_show_type'] = 1
                    goods_spec_format_sub_value_item['spec_value_data'] = ''
                    goods_spec_format_sub_value.append(goods_spec_format_sub_value_item)
                goods_spec_format_sub['spec_name'] = attr_name
                # goods_spec_format_sub['spec_id'] = attr_name_id
                goods_spec_format_sub['value'] = goods_spec_format_sub_value

                goods_spec_format.append(goods_spec_format_sub)
            sql = 'update ns_goods set goods_spec_format = %s WHERE goods_id = %s'
            mysql.update(sql,[(json.dumps(goods_spec_format).encode('utf-8'),goods_id)])

    pass


def main():

    category_dict = {

        1:"厨房餐饮",
        2:"家具生活",
        3:"宠物用品",
        4:"美容美妆",
        5:"母婴玩具",
        6:"女装女鞋",
        7:"男装男鞋",
        8:"健身保健",
        9:"汽车骑行",
        10:"户外旅行",
        11:"箱包配饰",
        12:"数码电器",

    }
    for category_id in range(1,12):
        category_name = category_dict[category_id]
        for pageNumber in range(1,1000):
            data = get_product_by_category(category_id,pageNumber)
            if data['data']:
                for item in data['data']:
                    sellItemId = item['sellItemId']
                    url = 'http://www.portlong.com/e/index.php/p/{}'.format(sellItemId)
                    html = request(url)
                    product_info = parse(html,sellItemId)
                    # print(product_info)
                    print('商品信息：', category_name,sellItemId)
                    save(product_info,sellItemId, category_name)
            else:
                break

    pass



if __name__ == '__main__':
    mysql = MysqlDb()
    main()
    mysql.close()