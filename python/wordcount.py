#!/usr/bin/env python2
# -*- coding: utf-8 -*-


from pyspark import SparkConf, SparkContext
import re


SparkMaster = "spark://host29:7077"
SparkAppName = "PySparkApp wordcount demo"
HDFS_ROOT = "hdfs://ict24:8020"


if __name__ == "__main__":

    conf = SparkConf().setMaster(SparkMaster).setAppName(SparkAppName)
    sc = SparkContext(conf = conf)

    lines = sc.textFile(HDFS_ROOT+"/dataset/bible_shakes.txt")
    words = lines.flatMap(lambda x: re.split('\W+', x)).filter(lambda x: len(x) > 1)

    result = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortBy(keyfunc = lambda x: x[1], ascending=False)

    print result.count()
    print result.take(50)
