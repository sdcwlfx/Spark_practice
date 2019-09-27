# python3
# -*- coding:utf-8 -*-
# @Time: 9/4/19 10:41 AM
# @Author: Damon
# @Software: PyCharm

'''
多个文件中存有数字，将所有文件数字排序，并生成序号键值对(序号,值)写入新文件
'''

from pyspark import SparkContext,SparkConf

index=0

def getIndex():
    global index
    index+=1
    return index

conf=SparkConf.setMaster("local").setAppName("File_Sort")
sc=SparkContext(conf)

#读取文件下以file开头的所有文件内容，生成一个RDD对象
lines=sc.textFile("file:////home/hadoop/PycharmProjects/WordCount/file_sort_data/file*.txt")
result1=lines.filter(lambda line:len(line.strip())>0)
#键值对形式
result2=result1.map(lambda x:(int(x.strip()),""))
#分进一个分区，确保全局有序
result3=result2.repartition(1)
#键升序排序
result4=result3.sortByKey(True)
result5=result4.map(lambda x:x[0])
result6=result5.map(lambda x:(getIndex(),x))
result6.saveAsTextFile("file:////home/hadoop/PycharmProjects/WordCount/file_sort_data/sorted_result")

