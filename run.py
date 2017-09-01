from pyspark import SparkConf,SparkContext
from properties import *

conf = SparkConf().setAppName(appname).setMaster(mode)
sc = SparkContext(conf=conf)

