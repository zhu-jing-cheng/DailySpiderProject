# -*- coding=utf-8 -*-
import pymysql.cursors
import uuid
import random


def random_str():
    uln = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    rs = random.sample(uln, 4)  # 生成一个 指定位数的随机字符串
    a = uuid.uuid1()  # 根据时间戳生成uuid , 保证全球唯一
    b = ''.join(rs + str(a).split("-"))  # 生成将随机字符串 与uuid拼接
    return b  # 返回随机字符串


# a = random_str()
# print(a)
# for i in range(10):
#     a = random_str()
#     num = ''.join(str(a).split("-"))
#     print(num)
