# python3
# -*- coding:utf-8 -*-
# @Time: 9/6/19 4:23 PM
# @Author: Damon
# @Software: PyCharm

'''
消费者，实现wordcount功能
'''

from pyspark.sql import SparkSession

if __name__=="__main__":
    #获取入口对象
    spark=SparkSession.builder.appName("StructuredKafkaWordCount").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    #DataFrame包含一个列，列名叫value
    lines=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")\
        .option("subscribe","wordcount-topic").load().selectExpr("CAST(value AS STRING)")

    #此时DataFrame对象有两列，value、count
    wordCounts=lines.groupBy("value").count()

    #窗口
    windowDuration='1 minutes'


    #将列value重命名为key，连接value和count组成列value(value:count),每8s进行一次流计算
    query=wordCounts.selectExpr("CAST(value AS STRING) as key","CONCAT(CAST(value AS STRING),':',CAST(count AS STRING)) as value")\
        .writeStream.outputMode("update").format("kafka")\
        .option("kafka.bootstrap.servers","localhost:9092")\
        .option("topic","wordcount-result-topic")\
        .option("checkpointLocation","file:///tmp/kafka-sink-cp")\
        .trigger(processingTime="8 seconds")\
        .start()

    query.awaitTermination()


