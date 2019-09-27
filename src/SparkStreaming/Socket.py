# python3
# -*- coding:utf-8 -*-
# @Time: 9/5/19 10:25 AM
# @Author: Damon
# @Software: PyCharm

'''

1.此程序为socket客户端，向服务端发送连接请求(服务端事先监听某个端口)，源源不断的从服务端获取数据，使用套接字流作为数据源
2.构建socket服务端：在shell交互式环境中输入命令启动nc程序：nc -lk 9999   监听9999端口,或者自定义服务器段数据源
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__=="__main__":
    if len(sys.argv)!=3:
        print("Usage:Socket.py <hostname> <port>",file=sys.stderr)
        exit(-1)
    sc=SparkContext(appName="PythonStreamingSocket")
    sc.setLogLevel("ERROR")
    ssc=StreamingContext(sc,1)
    #1.流计算数据源
    lines=ssc.socketTextStream(sys.argv[1],int(sys.argv[2]))
    #2.流计算计算
    counts=lines.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
    counts.pprint(50)
    #3.启动流计算
    ssc.start()
    #4.终止流计算
    ssc.awaitTermination()

