# -*- coding: utf-8 -*-
# @Author    : 朱景成
# @FileName  : test.py
# @ModifyTime: 2021/12/3 9:17
# @Contact   : zhujingcheng@tom.com
# @Software  : PyCharm
# @Version   : 1.0
# @Description: None
import pandas as pd


def read_txt(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        entity_list = f.readlines()
        print(entity_list)


if __name__ == '__main__':
    # read_txt('D:\\CodeProject\\DailySpiderProject\\resource_file\\wiki_entity_url.txt'
    f = lambda x, y: x * y
    result = f(156, 2)
    print(result)
