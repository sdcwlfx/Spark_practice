# python3
# -*- coding:utf-8 -*-
# @Time: 9/9/19 11:34 AM
# @Author: Damon
# @Software: PyCharm


'''
https://blog.csdn.net/asd136912/article/details/82913264
http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=pyspark%20sql%20functions%20from_json#pyspark.sql.functions.from_json
'''

from __future__ import print_function
from kafka import KafkaConsumer
from pyspark.sql import SparkSession,functions

import json

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType,DoubleType

if __name__=="__main__":
    #时间差=60s
    time_difference=60
    #获取入口对象
    spark=SparkSession.builder.appName("TestApp").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    #列
    schema = StructType([StructField("mac", StringType(), True),
                         StructField("brand", StringType(), True),
                         StructField("cache_ssid", StringType(), True),
                         StructField("capture_time", TimestampType(), True),
                         StructField("terminal_fieldstrenth", IntegerType(), True),
                         StructField("identification_type", StringType(), True),
                         StructField("certificate_code", StringType(), True),
                         StructField("ssid_position", StringType(), True),
                         StructField("access_ap_mac", StringType(), True),
                         StructField("access_ap_channel", StringType(), True),
                         StructField("access_ap_encryption_type", StringType(), True),
                         StructField("x_coordinate", DoubleType(), True),
                         StructField("y_coordinate", DoubleType(), True),
                         StructField("netbar_wacode", StringType(), True),
                         StructField("collection_equipmentid", StringType(), True),
                         StructField("collection_equipment_longitude", StringType(), True),
                         StructField("collection_equipment_latitude", StringType(), True),
                         StructField("lng_lat", StringType(), True)])

    #从kafka主题big_data中加载Json数据
    lines=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")\
        .option("subscribe","big_data_11") \
        .option("startingOffsets", "latest") \
        .load()\
        .selectExpr("CAST(value AS STRING)")

    #将存储Josn的value字段转成MapType，选取mac、capture_time、lng_lat，组成新的DataFrame对象
    df=lines.select(functions.from_json(functions.col("value").cast("string"),schema).alias("parse_value"))\
        .select("parse_value.mac","parse_value.capture_time","parse_value.lng_lat")
    df.printSchema()
    #将capture_time时间戳字段作为eventtime，有效时间点为（当前最大时间戳-60）
    df_tmp1=df.withWatermark("capture_time","60 seconds")

    #df_cp=df.toDF("mac_cp","capture_time_cp","lng_lat")
    #df_tmp_result=df_cp.groupby("lng_lat").count()
    #保留lng_lat相同的记录
    #df_tmp1=df.join(df_cp,["lng_lat"])
    #过滤掉自身匹配的记录，及时间戳相差60s的记录
    #df_tmp2=df_tmp1.filter(df_tmp1["mac"]!=df_tmp1["mac_cp"]).filter((int(df_tmp1["capture_time"])-int(df_tmp1["capture_time_cp"])>time_difference) | (int(df_tmp1["capture_time_cp"])-int(df_tmp1["capture_time"])>time_difference))
    #df_tmp2 = df_tmp1.filter(df_tmp1["mac"] != df_tmp1["mac_cp"])
    #df_tmp3=df_tmp2.groupby(df_tmp2.lng_lat,df_tmp2.mac).count()
    #df_tmp_result=df_tmp1.groupby(df_tmp1.lng_lat).count()

    #窗口步长为60s（第三参数），依据window、lng_lat、mac进行分组
    df_tmp2=df_tmp1.groupby(functions.window(df_tmp1.capture_time,"60 seconds","60 seconds"),df_tmp1.lng_lat,df_tmp1.mac).count()
    df_tmp2.printSchema()

    #df_tmp_result=df_tmp3.groupBy(df_tmp3["mac"],df_tmp3["lng_lat"]).count()

    #输出到kafka的big_data_result_11主题中，每8s进行一次流计算
    query=df_tmp2.selectExpr("CAST(lng_lat AS STRING) as key","CAST(window AS STRING) as value")\
        .writeStream.outputMode("append").format("kafka")\
        .option("kafka.bootstrap.servers","localhost:9092")\
        .option("topic","big_data_result_11")\
        .option("checkpointLocation","file:///tmp/kafka-big-data-11-cp") \
        .start()
        #.trigger(processingTime="8 seconds")\

    query.awaitTermination()


