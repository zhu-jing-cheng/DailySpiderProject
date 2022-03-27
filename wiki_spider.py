# -*- coding: utf-8 -*-
# @Author    : 朱景成
# @FileName  : wiki_spider.py
# @ModifyTime: 2021/12/2 20:26
# @Contact   : zhujingcheng@tom.com
# @Software  : PyCharm
# @Version   : 1.0
# @Description: 维基百科数据采集


import sys
import math
import pymysql
import threading
import multiprocessing

sys.path.append("..")
import random
import time
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
import pymongo
from lxml import etree
import uuid
from utools.langconv import *
from utools.DBUtils import RedisUtils
from utools.redis import redis_db_6
from utools.str_md5 import md5Encode

# 全局变量，redis连接
redis_utils = RedisUtils(redis_db_6)


def is_display_none(node):
    """带有display none的tr，是结束标志的一种"""
    return node.attrs.__contains__('style') and node['style'] == 'display:none' and not node.attrs.__contains__('class')


def is_start(node):
    """开始条件"""
    c1 = node.attrs.__contains__('style') and node.attrs['style'].__contains__('background')
    return c1


def is_merged_row(node):
    """需要融合的列"""
    return node.attrs.__contains__('scope')


def is_end(next_node):
    """结束条件"""
    c1 = next_node.attrs.__contains__('style') and next_node.attrs['style'].__contains__('background')
    return c1


def xpath_value(ele):
    """xpath通配td、th采集数据"""
    selector = etree.HTML(str(ele))
    key = ''.join(selector.xpath('//*//text()')).replace('\s+', '').replace('\n', '').replace(u'\xa0', u'').replace('•',
                                                                                                                    '').replace(
        '▲', '').replace('▼', '').strip()
    key = Converter("zh-hans").convert(key)
    return key


def random_str():
    """生成随机字符串作为ID"""
    uln = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    rs = random.sample(uln, 4)  # 生成一个 指定位数的随机字符串
    a = uuid.uuid1()  # 根据时间戳生成uuid , 保证全球唯一
    b = ''.join(rs + str(a).split("-"))  # 生成将随机字符串 与uuid拼接
    return b  # 返回随机字符串


def mongo_link_save():
    """mongodb连接---存"""
    client = pymongo.MongoClient('172.321.202.101', 17017)
    m_db = client.admin
    m_db.authenticate("admin123", "123456", mechanism='SCRAM-SHA-1')
    m_db = client['zhishiku111']
    mycol = m_db['character_wiki_mysql1111']

    return m_db, mycol


def mysql_link():
    """mysql数据库连接---取"""
    # 1.打开数据库连接
    mysql_db = pymysql.connect(
        host="172.321.202.102",
        database='test',
        user='test',
        password='123456!'
    )
    # 2.使用 cursor() 方法创建一个游标对象 cursor
    cursor = mysql_db.cursor()
    return cursor, mysql_db


def get_url():
    """获取mysql中人物链接"""
    cursor = mysql_link()[0]
    sql = "SELECT * FROM dws_rwzz_entitydata WHERE entity_label not LIKE '%组织%'"
    try:
        cursor.execute(sql)  # 执行SQL语句
        results = cursor.fetchall()  # 获取所有记录列表
        request_url_list_tmp = []  # 新建url列表
        for row in results:
            url = row[3]
            if len(url) > 1:
                name = str(url).split("-")[1]
                new_url = "https://zh.wikipedia.org/wiki/" + name
                request_url_list_tmp.append(new_url)
        return request_url_list_tmp
    except Exception as e:
        print(e)
    finally:
        mysql_link()[1].close()  # 关闭数据库连接


def request_page(page_url):
    """请求页面获取html字符串"""
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
    response = requests.get(url=page_url, headers=headers)
    html_str_tmp = response.content.decode('utf-8')
    return html_str_tmp


def cleaning_dic(baseInfoDic):
    """清洗字典中的空键值"""
    for k, v in list(baseInfoDic.items()):
        if k == '' or v == '' or k.__contains__('.'):
            del baseInfoDic[k]
        if isinstance(v, dict):
            for k2, v2 in list(v.items()):
                if k2 == '' or v2 == '' or k2.__contains__('.'):
                    del v[k2]


def task(url_part):
    """多线程任务"""
    count = 0
    list_renwu_no = []
    list_renwu_yes = []
    num1 = 0
    num2 = 0
    url_part = set(url_part)
    m_db, mycol = mongo_link_save()

    addDateTime_now = time.strftime("%Y-%m-%d %H:%M:%S")
    for url in url_part:
        try:
            count += 1
            print('第{}个待检测采集目标......'.format(count))
            html_str = request_page(url)
            selector_one = etree.HTML(html_str)
            soup_one = BeautifulSoup(html_str, "lxml")
            baseInfoDic = {}
            # 1.采集页面实体名
            page_title_name = ''.join(selector_one.xpath('//h1[@id="firstHeading"]//text()'))
            page_title_name = Converter("zh-hans").convert(page_title_name)
            # 2.采集分类标签
            tags = '|'.join(selector_one.xpath('//div[@id="mw-normal-catlinks"]/ul//li//a//text()'))
            tags = Converter("zh-hans").convert(tags)
            # 3.采集简介
            introduction = ''.join(selector_one.xpath('//div[@class="mw-parser-output"]//p//text()'))
            introduction = Converter("zh-hans").convert(introduction)
            # 4.采集左边正文
            left_content = ''.join(selector_one.xpath(
                '//div[@class="mw-parser-output"]//p//text()|//div[@class="mw-parser-output"]//ul//text()|//div[@class="mw-content-ltr"]//p//text()'))
            left_content = Converter("zh-hans").convert(left_content)
            # 判断人物链接是否有侧边栏box
            table_class = {"class": {"infobox geography vcard", "infobox vcard", "infobox geography", "infobox 地理",
                                     "infobox biography vcard", "infobox vcard plainlist"}}
            if not soup_one.find_all("table", table_class):
                # 添加新的标记字段
                baseInfoDic['entity_id'] = random_str()  # 添加id
                baseInfoDic['name'] = page_title_name  # 保留实体名
                baseInfoDic['page_name'] = page_title_name  # 保留页面实体名
                baseInfoDic['domain'] = '人物'  # 国家地区区分标签
                baseInfoDic['html'] = html_str  # 保留原网页字符串
                baseInfoDic['url'] = url  # 保留网页url
                baseInfoDic['addDatetime'] = addDateTime_now  # 添加时间
                baseInfoDic['tags'] = tags  # 添加标签
                baseInfoDic['introduction'] = introduction  # 添加简介
                baseInfoDic['a_content'] = ''  # 添加属性a标签链接
                baseInfoDic['left_content'] = left_content  # 添加左侧正文内容
                baseInfoDic['picture_url'] = ''  # 添加人物图片链接
                # 将字典插入列表中
                list_renwu_no.append(baseInfoDic)
                print(baseInfoDic['url'])
                num1 += 1
                print('进程名{}，第{}个待插入数据采集成功！--->正在添加进列表....当前数据添加进列表成功！'.format(multiprocessing.current_process().name,
                                                                               num1))
                # 将数据插入数据库
                if num1 % 100 == 0:
                    mycol.insert_many(list_renwu_no)
                    print("MongoDB存入成功！")
                    list_renwu_no = []
            # 确定右边有侧边栏之后
            else:
                baseInfo = soup_one.find_all("table", table_class)[0]
                # 进入条件判断
                isChild = False
                tmpKey = ""
                trs = baseInfo.select("tbody>tr")
                child_dict = {}

                # 5.采集图片链接
                if len(trs) > 1:
                    picture_url = []
                    for tr in trs:
                        img = tr.find_all('img')
                        selector_img = etree.HTML(str(img))
                        picture_url_tmp = selector_img.xpath('//img/@src')
                        if len(picture_url_tmp) > 0:
                            picture_url.append(picture_url_tmp)
                else:
                    picture_url = []

                # 6.采集实体属性链接
                shuxing_dic = {}
                for i in range(len(trs)):
                    tr = trs[i]
                    k_v_td = tr.find_all(['th', 'td'])
                    if len(k_v_td) == 2:
                        selector_a = etree.HTML(str(k_v_td[0]))
                        a_title = ''.join(selector_a.xpath('//th//text()'))
                        a_title = Converter("zh-hans").convert(a_title)
                        a_tag = k_v_td[1].find_all(['a'])
                        a_tag = list(map(lambda a: str(a), a_tag))
                        if len(a_tag) > 0:
                            shuxing_dic[a_title] = a_tag

                for i in range(len(trs) - 1):
                    tr = trs[i]
                    next_tr = trs[i + 1]
                    k_v = tr.find_all(['th', 'td'])
                    next_k_v = next_tr.find_all(['th', 'td'])
                    if len(k_v) == 0:
                        key, value = '', ''
                    elif len(next_k_v) == 0:
                        next_key, next_value = '', ''
                    else:
                        key = xpath_value(k_v[0])  # 国家为th
                        value = xpath_value(k_v[0]) if len(k_v) == 1 else xpath_value(k_v[1])
                        next_key = xpath_value(next_k_v[0])  # 国家为th
                        next_value = xpath_value(next_k_v[0]) if len(next_k_v) == 1 else xpath_value(next_k_v[1])
                    if is_start(k_v[0]):
                        isChild = True
                        tmpKey = key
                        child_dict[key] = value
                    elif isChild and is_end(next_k_v[0]):
                        child_dict[key] = value
                        baseInfoDic[tmpKey] = child_dict
                        child_dict = {}
                        isChild = False
                        tmpKey = ""
                    elif isChild:
                        child_dict[key] = value
                    elif i == len(trs) - 1:
                        if isChild:
                            child_dict[key] = value
                            child_dict[next_key] = next_value
                            baseInfoDic[tmpKey] = child_dict
                            child_dict = {}
                            isChild = False
                            tmpKey = ""
                        else:
                            baseInfoDic[key] = value
                            baseInfoDic[next_key] = next_value
                    else:
                        baseInfoDic[key] = value

                    # 清洗空键值
                    cleaning_dic(baseInfoDic)

                    # 添加新的标记字段
                    baseInfoDic['entity_id'] = random_str()  # 添加id
                    baseInfoDic['name'] = page_title_name  # 保留实体名
                    baseInfoDic['page_name'] = page_title_name  # 保留页面实体名
                    baseInfoDic['domain'] = '人物'  # 国家地区区分标签
                    baseInfoDic['html'] = html_str  # 保留原网页字符串
                    baseInfoDic['url'] = url  # 保留网页url
                    baseInfoDic['addDatetime'] = addDateTime_now  # 添加时间
                    baseInfoDic['tags'] = tags  # 添加标签
                    baseInfoDic['introduction'] = introduction  # 添加简介
                    baseInfoDic['a_content'] = shuxing_dic  # 添加属性a标签链接
                    baseInfoDic['left_content'] = left_content  # 添加左侧正文内容
                    baseInfoDic['picture_url'] = picture_url  # 添加人物图片链接
                # 将字典插入列表中
                list_renwu_yes.append(baseInfoDic)
                print(baseInfoDic['url'])
                num2 += 1
                print('进程名{}，第{}个待插入数据采集成功！--->正在添加进列表....当前数据添加进列表成功！'.format(multiprocessing.current_process().name,num2))
                # 将数据插入数据库
                if num2 % 100 == 0:
                    mycol.insert_many(list_renwu_yes)
                    print("MongoDB存入成功！")
                    list_renwu_yes = []
        except requests.exceptions.ConnectionError:
            print("Connection refused")
            # todo: dump entity_name into file
            with open('连接错误.txt', 'a+', encoding='utf-8') as f:
                f.write("连接错误")
        except Exception as e:
            # todo: dump entity_name into file
            with open('其他异常.txt', 'a+', encoding='utf-8') as f:
                print(e)
                f.write("其他异常")
    # 将最后一次循环中不满100的列表插入数据库中
    if len(list_renwu_yes) > 0:
        mycol.insert_many(list_renwu_yes)
        print("检测完成！")
    if len(list_renwu_no) > 0:
        mycol.insert_many(list_renwu_no)
        print("检测完成！")

    m_db.close()  # 关闭MongoDB链接


if __name__ == '__main__':
    p_num = 16
    p = Pool(p_num)
    print("稍等片刻~~~正在从数据库中取出url")
    request_url_list = list(set(get_url()))
    print("url添加列表完毕，请求url列表长度为{}".format(len(request_url_list)))
    length = len(request_url_list)
    print(length)
    # 将列表切片
    for i in range(p_num):
        p.apply_async(task,args=(request_url_list[i * math.floor(length / p_num):(i + 1) * math.floor(length / p_num)],))
    p.close()
    p.join()
