# python3
# -*- coding:utf-8 -*-
# @Time: 9/17/19 5:17 PM
# @Author: Damon
# @Software: PyCharm

'''
从kafka中取出第一次聚合后的JSON数据(包含字段：window、lng_lat、mac、count)，进行第二次聚合操作，并给出报警坐标及mac
{"window":{"start":"2019-09-17T16:57:00.000+08:00",,"end":"2019-09-17T16:58:00.000+08:00"},
  "lng_lat":"124.41::41.77","mac":"44-C3-49-5A-E5-93","count":12}
'''
from IPython.utils.ipstruct import Struct
from pyspark.sql import SparkSession, functions, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, ArrayType, \
    DataType

if __name__=="__main__":

    #获取入口对象
    spark=SparkSession.builder.appName("consumer_result").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    #列
    schema = StructType([StructField("window",StructType([StructField("start",TimestampType(),True),StructField("end",TimestampType(),True)]),True),
                         StructField("lng_lat", StringType(), True),
                         StructField("mac", StringType(), True),
                         StructField("count",IntegerType(),True)])



    #从kafka主题big_data_tmp中加载Json数据
    lines=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")\
        .option("subscribe","big_data_tmp")\
        .option("startingOffsets","latest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    #将存储Josn的value字段转成MapType，选取window、lng_lat、mac，组成新的DataFrame对象
    df=lines.select(functions.from_json(functions.col("value").cast("string"),schema).alias("parse_value"))\
        .select("parse_value.window","parse_value.lng_lat","parse_value.mac")
    df.printSchema()

    #流式去重
    df_tmp1 = df.withWatermark("window","300 seconds").dropDuplicates(["window","mac","lng_lat"])

    df_tmp2=df_tmp1.groupby(df_tmp1.window,df_tmp1.lng_lat).count()

    df_tmp2.printSchema()

    if type(df_tmp2) is DataFrame:
        print("Yes consumer_result DataFrame")
    else:
        print("not consumer_result DataFrame")

    #输出到kafka的big_data_result主题中，每8s进行一次流计算
    #query=df_tmp2.selectExpr("CAST(window AS STRING) as key","CONCAT(CAST(window AS STRING),':',CAST(count AS STRING)) as value")\
        #.writeStream.outputMode("update").format("kafka")\
        #.option("kafka.bootstrap.servers","localhost:9092")\
        #.option("topic","big_data_result_yc") \
        #.option("checkpointLocation", "file:///tmp/kafka-big-data-yc-cp") \
        #.start()

    #输出到shell进行debug
    query = df_tmp2\
        .writeStream.outputMode("update").format("console") \
        .start()
    query.awaitTermination()
