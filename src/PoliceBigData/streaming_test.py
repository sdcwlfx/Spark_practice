# python3
# -*- coding:utf-8 -*-
# @Time: 9/5/19 3:13 PM
# @Author: Damon
# @Software: PyCharm

'''
从kafka中指定Topic获取JSON数据
'''

from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession




if __name__=="__main__":
    #获取入口对象
    spark=SparkSession.builder.appName("BigData").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")






