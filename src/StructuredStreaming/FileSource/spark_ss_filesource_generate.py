# python3
# -*- coding:utf-8 -*-
# @Time: 9/6/19 11:49 AM
# @Author: Damon
# @Software: PyCharm

'''
随机生成JSON格式的1000个文件，文件内容是不超过100行的随机JSON行
'''

import os
import shutil
import random
import time

#存储临时数据目录
#TEST_DATA_TEMP_DIR="file:///home/hadoop/PycharmProjects/WordCount/structured_streaming_data/tmp/"
#TEST_DATA_DIR="file:///home/hadoop/PycharmProjects/WordCount/structured_streaming_data/tmp/testdata/"
TEST_DATA_TEMP_DIR="/tmp/"
TEST_DATA_DIR="/tmp/testdata/"


ACTION_DEF=['login','logout','purchase']
DISTRICT_DEF=['fijian','beijing','shanghai','gaungzhou']
JSON_LINE_PATTERN='{{"eventTime":{},"action":"{}","district":"{}"}}\n'

#测试的环境搭建，判断文件夹是否存在，如果存在则删除旧数据，病建立文件夹
def test_setUp():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR,ignore_errors=True)
        print("log1")
    os.mkdir(TEST_DATA_DIR)

#测试环境的恢复，对文件夹进行清理
def test_tearDown():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR,ignore_errors=True)


#生成测试文件
def write_and_move(filename,data):
    with open(TEST_DATA_TEMP_DIR+filename,"wt",encoding="utf-8") as f:
        f.write(data)

    shutil.move(TEST_DATA_TEMP_DIR+filename,TEST_DATA_DIR+filename)

if __name__=="__main__":
    test_setUp()
    for i in range(100):
        filename='e-mail-{}.json'.format(i)

        content=""
        #100行数据
        rndcount=list(range(100))
        #打乱
        random.shuffle(rndcount)
        for _ in rndcount:
            content+=JSON_LINE_PATTERN.format(str(int(time.time())),random.choice(ACTION_DEF),random.choice(DISTRICT_DEF))
        #将随机生成的数据写入json文件
        write_and_move(filename,content)
        time.sleep(1)
    #清理掉测试环境下文件
    #test_tearDown()


