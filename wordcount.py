# python3
# -*- coding:utf-8 -*-
# @Time: 8/23/19 5:08 PM
# @Author: Damon
# @Software: PyCharm

from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName("WordCount").setMaster("local")
sc=SparkContext(conf=conf)
inputFile="file:///home/hadoop/PycharmProjects/WordCount/word.txt"
textFile=sc.textFile(inputFile)
wordCount=textFile.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b)
wordCount.foreach(print)