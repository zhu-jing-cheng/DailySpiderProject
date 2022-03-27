# -*- coding: utf-8 -*-
# @Author    : 朱景成
# @FileName  : 文件名关键词剔除.py.py
# @ModifyTime: 2022/2/10 17:49
# @Contact   : zhujingcheng@tom.com
# @Software  : PyCharm
# @Version   : 1.0
# @Description: 文件名关键词剔除
import os
import os.path


def rename(file, keyword):
    """
    file: 文件路径
    keyWord: 需要修改的文件中所包含的关键字
    """
    os.chdir(file)
    items = os.listdir(file)
    print(os.getcwd())
    for name in items:
        print(name)
        # 遍历所有文件
        if not os.path.isdir(name):
            if keyword in name:
                new_name = name.replace(keyword, '')
                os.renames(name, new_name)
        else:
            rename(file + '\\' + name, keyword)
            os.chdir('...')
    print('-----------------------分界线------------------------')
    items = os.listdir(file)
    for name in items:
        print(name)


if __name__ == '__main__':
    rename('E:\\学习资料\\Python\\从零起步 系统入门Python爬虫工程师-399元\\', '~1【更多资源访问：  666java.com】')
