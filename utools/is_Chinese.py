# -*- coding: utf-8 -*-
# Author    : 朱景成
# FileName  : is_Chinese.py
# WriteTime : 2020/11/19 13:20
# Software  : PyCharm


def is_Chinese(word):
    for ch in word:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False