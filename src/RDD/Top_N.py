# python3
# -*- coding:utf-8 -*-
# @Time: 9/2/19 7:49 PM
# @Author: Damon
# @Software: PyCharm
'''
计算目录下文件某个字段的Top-N值
'''

from pyspark import SparkContext,SparkConf

conf=SparkConf.setAppName("Top-N").setMaster("local")
sc=SparkContext(conf)

#读取rdd_data文件下下所有文件生成一个RDD对象
rdd=sc.textFile("file:////home/hadoop/PycharmProjects/WordCount/rdd_data/")
#筛选每行只有四个元素的记录
result1=rdd.filter(lambda line:len(line.strip())>0 and len(line.strip(","))==4)
result2=result1.map(lambda line:line.split(",")[2])

#键值对形式
result3=result2.map(lambda x:(x,""))
#降序排列
result4=result3.sortByKey(False)
#取键值对的键
result5=result4.map(lambda x:x[0])
#取前五
result6=result5.take(5)