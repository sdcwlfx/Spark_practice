# python3
# -*- coding:utf-8 -*-
# @Time: 9/4/19 4:25 PM
# @Author: Damon
# @Software: PyCharm

'''
DStream是微小规模的批处理，每个批处理块相当于一个RDD
'''

from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext


conf=SparkConf().setAppName("DStream").setMaster("local[*]")
sc=SparkContext(conf=conf)
#设置打印日志级别
sc.setLogLevel("ERROR")
#流计算的主入口，每10秒启动一次流计算
ssc=StreamingContext(sc,10)
#数据源
lines=ssc.textFileStream("file:///home/hadoop/PycharmProjects/WordCount/DStream_data")
#实时计算（转换）
words=lines.flatMap(lambda line:line.split(' '))
wordCounts=words.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
#s实时查询（打印）,必须传入参数
wordCounts.pprint(10)
#k开启流计算，会监控数据源，每10s进行一次流计算
ssc.start()
ssc.awaitTermination()
