# python3
# -*- coding:utf-8 -*-
# @Time: 9/3/19 11:00 AM
# @Author: Damon
# @Software: PyCharm

'''

'''

from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession,SQLContext




#Spark2.x默认不支持笛卡尔积操作，打开笛卡尔积
spark=SparkSession.builder.appName("TestApp").master("local").config("spark.sql.crossJoin.enabled","true").getOrCreate()


inputFile="file:///home/hadoop/PycharmProjects/WordCount/data/FACE_TRACE.csv"
inputFileTwo="file:///home/hadoop/PycharmProjects/WordCount/data/MAC_TRACE.csv"
df=spark.read.csv(inputFile,header=True).toDF("FACE_CARD_ID_1","TIMESTAMP_1","LNG","LAT","STAYTIME","ING_GRID","LAT_GRID")
df1=df.select(df.FACE_CARD_ID_1,df.TIMESTAMP_1,df.LNG,df.LAT)

df=df.toDF("FACE_CARD_ID_2","TIMESTAMP_2","LNG","LAT","STAYTIME","ING_GRID","LAT_GRID")
df2=df.select(df.FACE_CARD_ID_2,df.TIMESTAMP_2,df.LNG,df.LAT)
print(df1.distinct().show())

#全连接
print(df1.join(df2).show())

#行合并df对象，并去除重复的记录
print(df1.unionAll(df2).distinct().show())

#按照字段“LNG"、”LAT“进行链接，只保留”LNG“、”LAT“字段相同的记录
print(df1.join(df2,["LNG","LAT"]).show())


df3=df1.join(df2,["LNG","LAT"])
print(df3.show())

#过滤掉自身匹配的记录，及时间戳相差60s的记录
df4=df3.filter(df3["FACE_CARD_ID_1"]!=df3["FACE_CARD_ID_2"]).filter((df3["TIMESTAMP_1"]-df3["TIMESTAMP_2"]>=0) | (df3["TIMESTAMP_2"]-df3["TIMESTAMP_1"]>0))
print(df4.show())

df5=df4.groupby(df4.LNG,df4.LAT,df4.FACE_CARD_ID_1).count()
print(df5.show())

df6=df5.groupby(df5.LNG,df5.LAT).count()
print(df6.show())


#RDD=df3.rdd
#RDD.filter(lambda line:line.)

#按经纬度分组，并统计每组中FACE_CARD_ID_1的不同值的个数即为人数
#print(df4.groupby(["LNG","LAT"])["FACE_CARD_ID_1"].sum().show())



