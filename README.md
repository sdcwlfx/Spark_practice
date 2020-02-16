
# 测试运行环境
- ubuntu14+Spark2.3.3 单机模式+scala2.11+python3.6+hadoop2.7+kafka_2.11-2.3.0+jdk1.8+pycharm


# 目录说明
- 根目录下wordcount.py文件以spark本地模式运行，统计word.txt中单词的数量：运行方法：进入根目录，执行命令 
  /usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.3 wordcount.py

- 目录data、DStream_data、file_sort_data、rdd_data存放的数据文件

## src
- kafka_json:pyspark实现structured streaming从kafka读取并解析JSON数据，经一系列转换、聚合后将结果输出到控制台。其中producer.py文件为生产者，生产JSON数据写入kafka主题；consumer_cp.py使用structured streaming实时从kafka中拉取、解析JSON数据，经分组聚合操作写入kafka另外主题中(structured streaming不支持连续聚合操作，故进行第一次分组，存入kafka中，之后再从kafka中读取进行第二次聚合)；consumer_result.py从kafka中读取JSON数据进行第二次聚合操作，并将结果在控制台输出。
#### kafka_json目录运行方式(spark单机模式)
- 启动hadoop:执行命令->sbin/start-dfs.sh
- 启动kafka:
              
              cd /usr/local/kafka/
              
              ./bin/zookeeper-server-start.sh config/zookeeper.properties
              
              ./bin/kafka-server-start.sh config/server.properties
- 监控输入输出终端：
              
              /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic big_data_kj
              
              /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic big_data_tmp
              
- 运行producer.py:直接在pycharm中右击运行
- 运行consumer_cp.py:/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.3 consumer_cp.py
- 运行consumer_result.py:/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.3 consumer_result.py

- RDD目录包含三个对spark RDD的实现，分别为文件排序、二次排序、Top-N,
  运行方式：/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.3 文件名.py
- SparkStreaming目录是使用spark streaming进行流计算的实现
- StructuredStreaming目录是使用structured streaming进行流计算的实现 
