# python3
# -*- coding:utf-8 -*-
# @Time: 9/6/19 1:02 PM
# @Author: Damon
# @Software: PyCharm

import os
import shutil
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import TimestampType,StringType

#定义JSON文件路径变量
TEST_DATA_DIR_SPARK="file:///tmp/testdata"

if __name__=="__main__":

    #定义模式为时间戳类型的eventTime、字符串类型的操作和 省份组成
    schema=StructType([StructField("eventTime",TimestampType(),True),
                       StructField("action",StringType(),True),
                       StructField("district",StringType(),True)])

    spark=SparkSession.builder.appName("StructuredEMailPurchaseCount").getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    #获取DataFrame对象，Option表示每次最多读取100个Json文件
    lines=spark.readStream.format("json").schema(schema=schema).option("maxFilesPerTrigger",100).load(TEST_DATA_DIR_SPARK)

    #定义窗口
    windowDuration='1 minutes'

    #windowedCounts=lines.filter("action='purchase'").groupBy('district',window('eventTime',windowDuration))\
    #.count().sort(asc('window'))
    windowedCounts = lines.filter("action='purchase'").groupBy('district', window('eventTime', windowDuration)).count().sort("window")

    windowedCounts.printSchema()


    query=windowedCounts.writeStream.outputMode("complete").format("console").option("truncate","false").trigger(processingTime="10 seconds").start()
    query.awaitTermination()

