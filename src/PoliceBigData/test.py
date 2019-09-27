# python3
# -*- coding:utf-8 -*-
# @Time: 9/2/19 12:03 PM
# @Author: Damon
# @Software: PyCharm

'''
筛选时间戳、经纬度一致的分组，并输出分组的经纬度
'''


from __future__ import print_function
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

#conf=SparkConf().setAppName("TestAPP").setMaster("local")
#sc=SparkContext(conf=conf)
spark=SparkSession.builder.appName("TestApp").config("local*").getOrCreate()
#sc.setLogLevel("ERROR")
inputFile="file:///home/hadoop/PycharmProjects/WordCount/data/FACE_TRACE.csv"
#rdd=sc.textFile(inputFile)
df=spark.read.csv(inputFile,header=True)
df1=df.select(df.FACE_CARD_ID,df.TIMESTAMP,df.LNG,df.LAT).groupby(df.TIMESTAMP,df.LNG,df.LAT).count()
#df2=df1.groupBy(df.TIMESTAMP,df.LNG,df.LAT).count()
df1.printSchema()

#创建临时表
#df1.createOrReplaceTempView("test")
df2=df1.filter("count>=2")
#print(df1.filter("count>=2").show())

print(df2.select(df2.LNG,df2.LAT).show())
#print(rdd.map(lambda x:x.split(',')[1]).collect())

