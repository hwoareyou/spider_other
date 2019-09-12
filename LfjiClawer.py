# -*- coding: utf-8 -*-
# @Author   : liu
# 加入日志


import json
from selenium.webdriver import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
import time
import re, os, urllib.request, datetime, random, requests, sys, socket, string
from lxml import etree
from mysql_utils.mysql_db import MysqlDb
from baidu_OCR import recognition_character
import threading
from threading import Thread
from queue import Queue
from tengxun_OCR import Ocr
from log_utils.mylog import Mylog
from PIL import Image
import traceback
import warnings

warnings.filterwarnings('ignore')


def get_proxy():
    # 代理服务器
    proxyHost = "http-dyn.abuyun.com"
    proxyPort = "9020"

    # 代理隧道验证信息
    proxyUser = "H19SC127905HL13D"
    proxyPass = "7942D1D850E8D5C5"

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


def request(url):
    print('采集商品：', url)

    headers = {
        "Host": "www.lfji.com.tw",
        "Connection": "keep-alive",
        # "Cache-Control": " max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
        # "User-Agent": get_useragent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        # "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cookie": "XSRF-TOKEN=burPheYxOGLZNabhWpkzv0m%2FtS%2F2tsGru%2Bp1get5gbU8rQOdnIq%2B%2FpWiUUey5dLrznq5ZBhthIBlJEzVCvHq6w%3D%3D; _shop_shopline_session_id_v2=c34bca5b5a8b37b751de94d8e813d94a; _fbp=fb.2.1566552361950.677639931; _ga=GA1.3.860871987.1566552444; _gid=GA1.3.896719120.1566552444"
    }
    res = requests.get(url, headers=headers, proxies=get_proxy(),verify=False)
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
    for pageNumber in range(1, 1000):
        post_data = {
            "sid": sellItemId,
            "pageNumber": str(pageNumber),
            "pageSize": "20",
        }
        url = 'http://www.portlong.com/e/index.php/item/reviews?sid={0}&pageNumber={1}&pageSize=20'.format(sellItemId,
                                                                                                           pageNumber)
        res = requests.post(url, headers=headers, data=post_data, proxies=get_proxy())
        if len(res.json()['data']) < 20:
            for item in res.json()['data']:
                try:
                    pic_url = re.search(r'src="(.+\.png)', item['content']).group(1)
                except:
                    pic_url = ''
                item['pic_url'] = pic_url
                comm_data.append(item)
            break
        else:
            for item in res.json()['data']:
                try:
                    pic_url = re.search(r'src="(.+)', item['content']).group(1)
                except:
                    pic_url = ''
                item['pic_url'] = pic_url
                comm_data.append(item)

    return comm_data

    pass


def parse(html_source, product_link):
    product_info = {}
    html = etree.HTML(html_source)
    data = json.loads(re.search(r"app.value\('product', (.+?)\);", html_source).group(1))

    # sku
    sku = data['sku']
    if sku:
        product_info['sku'] = sku
    else:
        sku_sub = re.search(r'products/(.+)', product_link).group(1)
        product_info['sku'] = sku_sub
    # id
    # id = data['_id']
    # owner_id = data['owner_id']
    # product_info['id'] = id
    # product_info['owner_id'] = owner_id
    # 标题
    title = data['title_translations']['zh-hant']
    product_info['title'] = title
    # 类别
    if html.xpath('//ol[@class="breadcrumb"]/li[2]/a/text()'):
        product_info['category'] = str(html.xpath('//ol[@class="breadcrumb"]/li[2]/a/text()')[0])
    else:
        product_info['category'] = '未分类'
    # 介绍
    introduction = '\n'.join(
        [item.strip() for item in html.xpath('//div[@class="Product-promotions js-promotions"]//text()') if
         item.strip()])
    product_info['introduction'] = introduction
    # 品牌
    brand = data['brand']
    product_info['brand'] = brand
    # 价格单位
    # currency_symbol = data['price']['currency_symbol']
    # product_info['currency_symbol'] = currency_symbol
    # 原价
    price = data['price']['cents']
    product_info['price'] = price
    # 销售价
    price_sale = data['price_sale']['cents']
    product_info['price_sale'] = price_sale
    # 最低团购价
    # lowest_member_price = data['lowest_member_price']['cents']
    # product_info['lowest_member_price'] = lowest_member_price
    # 团购价
    # member_price = data['member_price']['cents']
    # product_info['member_price'] = member_price
    # 关键词
    seo_keywords = data['seo_keywords']
    product_info['seo_keywords'] = seo_keywords
    # 数量（库存）
    quantity = data['quantity']
    product_info['quantity'] = quantity
    # 交易量
    max_order_quantity = data['max_order_quantity']
    product_info['max_order_quantity'] = max_order_quantity
    # 重量
    weight = data['weight']
    product_info['weight'] = weight
    # 性别
    # gender = data['gender']
    # product_info['gender'] = gender
    # 年龄群
    # age_group = data['age_group']
    # product_info['age_group'] = age_group
    # 成人
    # adult = data['adult']
    # product_info['adult'] = adult
    # 创建日期
    # created_at = data['link']['created_at']
    # product_info['created_at'] = created_at
    # # 更新日期
    # updated_at = data['link']['updated_at']
    # product_info['updated_at'] = updated_at
    # 主图
    media = [item['images']['original']['url'] for item in data['media']]
    product_info['media'] = media
    # 描述信息
    if html.xpath('//div[@id="R-BOX"]'):
        content = html.xpath('//div[@id="R-BOX"]')
    elif html.xpath('//div[@class="global-secondary dark-secondary description-container"]'):
        content = html.xpath('//div[@class="global-secondary dark-secondary description-container"]')
    elif html.xpath('//div[@id="product-detail-media"]'):
        content = html.xpath('//div[@id="product-detail-media"]')
    else:
        content = ['']

    description_html = etree.tostring(content[0], encoding="utf-8", pretty_print=True, method="html").decode('utf-8')
    description_html = description_html.replace('☎', '').replace('購買商品詢問:', '').replace(':', '').replace('FB線上客服',
                                                                                                         '').replace(
        '➤不會購買請看', '').replace('【教學】如何一次購買多個顏色尺寸', '').replace('購 物 教 學', '').replace('『平行輸入商品』', '').replace('src=""',
                                                                                                              '')
    description_html = re.sub(r'(<iframe .+</iframe>)?', '', description_html)
    product_info['description_html'] = description_html
    # 描述图片（包括描述信息里推荐商品的图片）
    description = re.findall(r'data-src="(.+?\.[jpgif]+)"', description_html)
    description = [item for item in re.findall(r'data-src="(.+?\.[jpgif]+)"', description_html) if not 'gif' in item]
    product_info['description'] = description
    # 属性（变体）
    attr_names = [item['label'] for item in data['field_titles']]
    attr_values = [item['fields_translations']['zh-hant'] for item in data['variations']]
    attrs = []
    for i in range(len(attr_names)):
        attr_name = attr_names[i]
        attr_value = []
        for j in range(len(attr_values)):
            attr_value.append(attr_values[j][i])
        attrs.append({"attr_name": attr_name, "attr_value": list(set(attr_value))})

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
        "id": sellItemId
    }

    url = 'http://www.portlong.com/e/index.php/order?id=' + sellItemId
    res = requests.post(url, headers=headers, data=post_data, proxies=get_proxy())
    html = etree.HTML(res.text)
    attrs = [dict(item.attrib) for item in html.xpath('//ul[@class="sys_spec_text"]/li')]
    return attrs


def get_product(page):
    if page:
        referer = "https://www.lfji.com.tw/products?page=" + str(page)
    else:
        referer = "https://www.lfji.com.tw/products"
    headers = {
        "Host": "www.lfji.com.tw",
        "Connection": "keep-alive",
        "upgrade-insecure-requests": "1",
        # "Cache-Control": " max-age=0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
        # "User-Agent": get_useragent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": referer,
        # "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "If-None-Match": 'W/"e0d7dc7b7962d97d7e858f5b0ec6257f"',
        "Cookie": "XSRF-TOKEN=dKLg7Ba%2BLoXtfkUKSqTEZygFp4ITg0RlZK2GyfRqCQMm5Sz0bAWoGaHpsqyi2CUzr8Cryf1YAU66Y7%2BdFeJiXQ%3D%3D; _shop_shopline_session_id_v2=c34bca5b5a8b37b751de94d8e813d94a; _fbp=fb.2.1566552361950.677639931; _ga=GA1.3.860871987.1566552444; _gid=GA1.3.896719120.1566552444; _gat=1"
    }
    post_data = {
        "page": str(page)
    }
    url = 'https://www.lfji.com.tw/products?page=' + str(page)
    res = requests.get(url, headers=headers, data=post_data, proxies=get_proxy())
    html = etree.HTML(res.text)
    products_link = ["https://www.lfji.com.tw" + str(item.xpath('./@href')[0]) for item in
                     html.xpath('//ul[@class="boxify-container"]//a')]

    return products_link


def save_imgDir(img_list):
    for item in img_list:
        img_dir = 'upload' + item['img_dir'].split('upload')[1]
        pic_name = re.search(r'/\d+/(.+)\.jpg', item['img_dir']).group(1)
        sql = 'insert into sys_album_picture (album_id,pic_name,pic_tag,pic_cover,pic_cover_big,pic_cover_mid,pic_cover_small,pic_cover_micro) ' \
              'select \"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\" from dual WHERE NOT  EXISTS  (SELECT pic_id from sys_album_picture WHERE pic_name = \"%s\" ) ' \
              % (32, pic_name, pic_name, img_dir, img_dir, img_dir, img_dir, img_dir, pic_name)
        cur = mysql.mysql.cursor()
        cur.execute(sql)
        cur.execute('commit')
    cur.close()

    pass


def get_pic_id(sellItemId):
    sql = 'select pic_id from sys_album_picture WHERE pic_name LIKE \"%s\" ' % (sellItemId + '_pic_%')
    pic_id = mysql.select(sql)
    return pic_id


def save_img(product_info, sellItemId):
    dir = 'D:/PHPTutorial/WWW/333.com/upload/goods/20190826/'
    if not os.path.exists(dir):
        os.makedirs(dir)

    img_list = []

    # 主图
    media = product_info['media']
    i = 0
    for item in media:
        i += 1
        img_dir = dir + sellItemId + '_pic_' + str(i) + '.jpg'
        img_list.append({'img_url': item, 'img_dir': img_dir})

    # # 属性图
    # attrs = product_info['attrs']
    # i = 0
    # for item in attrs:
    #     for attr in item['attr_value']:
    #         img_url = attr['attr_url']
    #         if img_url:
    #             i += 1
    #             img_dir = dir + sellItemId + '_pic_' + str(i) +'.jpg'
    #             img_list.append({'img_url': img_url, 'img_dir': img_dir})

    # 保存图片本地路径
    save_imgDir(img_list)

    # 描述图
    description = product_info['description']
    description_html = product_info['description_html']
    description_list = []
    for i in range(len(description)):
        img_dir = dir + sellItemId + '_description_' + str(i) + '.jpg'
        sub_img_dir = '/upload' + img_dir.split('upload')[1]
        description_html = description_html.replace(description[i], sub_img_dir)
        img_list.append({'img_url': description[i], 'img_dir': img_dir})
        description_list.append({'img_url': description[i], 'img_dir': img_dir})

    product_info['description_html_sub'] = description_html.replace('data-src', 'src').replace('src=""', '')

    for img_data in img_list:
        img_url = img_data['img_url']
        index = img_url.find('//')
        img_url = img_url.replace(img_url[:index], 'https:')
        img_dir = img_data['img_dir']

        # 设置超时时间为30s
        socket.setdefaulttimeout(30)
        try:
            urllib.request.urlretrieve(img_url, img_dir)
            # r = requests.get(img_url,timeout=30)
            # with open(img_dir, 'wb') as f:
            #     f.write(r.content)
        except:
            count = 1
            while count <= 5:
                try:
                    urllib.request.urlretrieve(img_url, img_dir)
                    # r = requests.get(img_url, timeout=30)
                    # with open(img_dir, 'wb') as f:
                    #     f.write(r.content)
                    break
                except:
                    err_info = 'save_img reloading for %d time' % count if count == 1 else 'save_img reloading for %d times' % count
                    print(err_info)
                    count += 1
            if count > 5:
                print("save_img job failed!")
                print(img_url)


def get_category_id(category_name):
    sql = 'insert into ns_goods_category (category_name,short_name,level,is_visible,keywords) select \"%s\",\"%s\",\"%s\",\"%s\",\"%s\" from dual WHERE NOT  EXISTS  (SELECT category_id from ns_goods_category WHERE category_name = \"%s\" ) ' \
          % (category_name, category_name, 1, 1, category_name, category_name)
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
    return attr_name_id


def get_attr_value_id(attr_name_id, attr_value):
    sql = 'insert into ns_goods_spec_value (spec_id,spec_value_name,is_visible) select \"%s\",\"%s\",\"%s\" from dual WHERE NOT  EXISTS  (SELECT spec_id from ns_goods_spec_value WHERE spec_id = \"%s\" AND  spec_value_name = \"%s\") ' \
          % (attr_name_id, attr_value, 1, attr_name_id, attr_value)
    cur = mysql.mysql.cursor()
    cur.execute(sql)
    cur.execute('commit')
    cur.close()
    sql = 'select spec_id,spec_value_id from ns_goods_spec_value WHERE spec_id = \"%s\" AND  spec_value_name = \"%s\" ' % (
    attr_name_id, attr_value)
    goods_spec_id = str(mysql.select(sql)[0]['spec_id']) + ':' + str(mysql.select(sql)[0]['spec_value_id'])

    return goods_spec_id


def save_goods_sku(product_info, goods_id, attr_value_id, attr_value):
    sql = 'insert into ns_goods_sku (goods_id,sku_name,attr_value_items,attr_value_items_format,market_price,price) select \"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"' \
          ' from dual WHERE NOT  EXISTS  (SELECT sku_id from ns_goods_sku WHERE goods_id = \"%s\" AND  sku_name = \"%s\") ' \
          % (goods_id, attr_value, attr_value_id, attr_value_id, product_info['price'], product_info['price_sale'],
             goods_id, attr_value)
    cur = mysql.mysql.cursor()
    cur.execute(sql)
    cur.execute('commit')
    # cur.close()

    pass


def save_comment(goods_id, product_info):
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

        # # 拼接描述html
        # description_list = product_info['description_list']
        # description = '<p>'
        # for item in description_list:
        #     description += ' <img src="%s" />' % ('/upload' + item['img_dir'].split('upload')[1])
        # description += '</p>'

        sql = '''insert into ns_goods (shop_id,goods_name,category_id,goods_type,market_price,price,promotion_price,stock,picture,keywords,introduction,description,
                          is_stock_visible,state,img_id_array,shipping_fee_type,presell_delivery_type) 
                          values 
                          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        value = [(0, product_info['title'], category_id, 1, product_info['price'], product_info['price_sale'],
                  product_info['price_sale'], 0, picture, '', product_info['introduction'],
                  product_info['description_html_sub'], 1, 1, img_id_array, 3, 1)]
        mysql.insert(sql, value)

        sql = 'select goods_id from ns_goods WHERE  goods_name = \"%s\" ' % product_info['title']
        goods_id = mysql.select(sql)[0]['goods_id']

        # # 保存评论
        # save_comment(goods_id, product_info)

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
                    attr_value_id = get_attr_value_id(attr_name_id, attr_value)

                    # 保存sku
                    save_goods_sku(product_info, goods_id, attr_value_id, attr_value)

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
            mysql.update(sql, [(json.dumps(goods_spec_format).encode('utf-8'), goods_id)])

            print('数据已写入:', product_info['category'], product_info['sku'])
    else:

        print('数据已存在:', product_info['category'], product_info['sku'])
    pass


def main():

    for page in range(1, 8):
        products_link = get_product(page)
        for product_link in products_link:
            html = request(product_link)
            product_info = parse(html, product_link)
            print('商品信息：', product_info['category'], product_info['sku'])
            save(product_info, product_info['sku'], product_info['category'])
    pass


if __name__ == '__main__':

    mysql = MysqlDb("localhost",3306,"root","root","niushop_b2c")
    main()
    mysql.close()
