# python3
# -*- coding:utf-8 -*-
# @Time: 9/4/19 11:17 AM
# @Author: Damon
# @Software: PyCharm

'''
二次排序，用于排序的Kay必须是可比较对象，写在文本文件中的是字符串形式
生成临时可比较key用于二次排序,绑定的字符串也随之形成二次排序效果
1.按照Ordered和Serializable接口实现自定义排序的key
2.将要进行二次排序的文件加载进来生成<key,value>类型RDD
3.使用sortByKey基于自定义的key进行二次排序
4.去除掉排序的key只保留排序的结果
'''

from pyspark import SparkContext,SparkConf
from operator import gt

#自定义键值对排序类
class SecondarySortKey():
    def __init__(self,k):
        self.column1=k[0]
        self.column2=k[1]

    def __gt__(self, other):
        if other.column1==self.column1:
            return (gt(self.column2,other.column2))
        else:
            return gt(self.column1,other.column1)

def main():
    conf=SparkConf.setMaster("local").setAppName("Secondary_Sort")
    sc=SparkContext(conf)

    inputFile="file:////home/hadoop/PycharmProjects/WordCount/secondary_sort_data/file*.txt"
    rdd=sc.textFile(inputFile)
    #过滤空字符串
    rdd1=rdd.filter(lambda x:len(x.strip())>0)
    rdd2=rdd1.map(lambda x:((x.split(" ")[0],x.split(" ")[1],x)))
    #使用自定义排序
    rdd3=rdd2.map(lambda x:(SecondarySortKey(x[0]),x[1]))
    rdd4=rdd3.sortByKey(False)
    #仅保留value
    rdd5=rdd4.map(lambda x:x[1])
    rdd5.saveAsTextFile("file:////home/hadoop/PycharmProjects/WordCount/secondary_sort_data/sorted_result")
