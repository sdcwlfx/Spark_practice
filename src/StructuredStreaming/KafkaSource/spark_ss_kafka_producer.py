# python3
# -*- coding:utf-8 -*-
# @Time: 9/6/19 4:07 PM
# @Author: Damon
# @Software: PyCharm

'''
生产者,生成数据病扔给Kafka的某个主题
'''

import string
import random
import time
from kafka import KafkaProducer


if __name__=="__main__":
    #生产者对象
    producer=KafkaProducer(bootstrap_servers=['localhost:9092'])

    while True:
        #生成两个小写英文字母
        s2=(random.choice(string.ascii_lowercase) for _ in range(2))
        #将两个小写英文字母组成一个
        word=''.join("aa")
        value=bytearray(word,'utf-8')
        #发送数据到指定topic，超时时间10秒
        producer.send('wordcount-topic',value=value).get(timeout=10)
        #每0.1秒发送由两个英文字母组成的字符串
        time.sleep(1)
