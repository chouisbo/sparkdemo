
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("spark://host29:7077").setAppName("My SparkApp Python")
sc = SparkContext(conf = conf)

