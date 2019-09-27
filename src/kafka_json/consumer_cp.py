# python3
# -*- coding:utf-8 -*-
# @Time: 9/9/19 11:34 AM
# @Author: Damon
# @Software: PyCharm


'''
从kafka中读取原始JSON数据，进行第一次聚合操作,将聚合后的DataFrame(包含字段：window、mac、lng_lat、count)对象转为JSON写入kafka中
https://blog.csdn.net/asd136912/article/details/82913264
http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=pyspark%20sql%20functions%20from_json#pyspark.sql.functions.from_json
'''

from pyspark.sql import SparkSession, functions, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType,DoubleType

if __name__=="__main__":

    #获取入口对象
    spark=SparkSession.builder.appName("consumer_cp").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    #列
    schema = StructType([StructField("id", IntegerType(), True),
                         StructField("mac", StringType(), True),
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
        .option("subscribe","big_data_kj")\
        .option("startingOffsets","latest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    #将存储Josn的value字段转成MapType，选取mac、capture_time、lng_lat，组成新的DataFrame对象
    df=lines.select(functions.from_json(functions.col("value").cast("string"),schema).alias("parse_value"))\
        .select("parse_value.id","parse_value.mac","parse_value.capture_time","parse_value.lng_lat")
    df.printSchema()
    #df3=df.groupby("id").count()

    #流式去重,按照获取的capture_time字段去重
    #df_tmp1=df.withWatermark("capture_time","60 seconds").dropDuplicates(["lng_lat"])
    #df_tmp1 = df.withWatermark("capture_time", "60 seconds").dropDuplicates(["lng_lat","mac","capture_time"])
    df_tmp1 = df.withWatermark("capture_time", "60 seconds").dropDuplicates(["lng_lat","mac","capture_time"])

    df_tmp2=df_tmp1.groupby(functions.window(df_tmp1.capture_time,"60 seconds","60 seconds"),df_tmp1.lng_lat,df_tmp1.mac).count()

    df_tmp2.printSchema()


    if type(df_tmp2) is DataFrame:
        print("Yes consumer_cp DataFrame")
    else:
        print("not consumer_cp DataFrame")

    #输出到kafka的big_data_result主题中，每8s进行一次流计算
    #query=df_tmp2.selectExpr("CAST(window AS STRING) as key","CONCAT(CAST(window AS STRING),':',CAST(count AS STRING)) as value")\
        #.writeStream.outputMode("update").format("kafka")\
        #.option("kafka.bootstrap.servers","localhost:9092")\
        #.option("topic","big_data_result_yc") \
        #.option("checkpointLocation", "file:///tmp/kafka-big-data-yc-cp") \
        #.start()


    #输出到shell进行debug
    #query = df_tmp3\
     #   .writeStream.outputMode("append").format("console") \
      #  .start()

    #DataFrame对象转为JSON作为value传入kafka主题big_data_result_10
    query = df_tmp2.selectExpr("to_json(struct(*)) AS value")\
        .writeStream.outputMode("update").format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "big_data_tmp") \
        .option("checkpointLocation", "file:///tmp/kafka-big-data-10-cp") \
        .start()


    query.awaitTermination()
        #.trigger(processingTime="8 seconds") \

    #query=df_tmp2.writeStream.outputMode("complete").format("memory")\
        #.queryName("tableName")\
        #.start()


    #ark.sql("select * from tableName").show()
    #query=df_tmp2.writeStream\
        #.format("parquet")\
        #.option("checkpointLocation", "file:///tmp/kafka-big-data-kj-cp")\
        #.option("path","file:///tmp/kafka-big-data-kj-dir")\
        #.start()







