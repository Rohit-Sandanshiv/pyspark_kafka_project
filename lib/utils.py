from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser


def getConfig(env):
    spark_conf = SparkConf()
    conf = configparser.ConfigParser()
    conf.read("conf/spark.conf")
    for k, v in conf.items(env):
        spark_conf.set(k, v)
    return spark_conf


def getSpark_Session(env):
    conf = getConfig(env)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


def printSbdlConfigLoader(env):
    d = {}
    conf = configparser.ConfigParser()
    conf.read("conf/sbdl.conf")
    for k, v in conf.items(env):
        d[k] = v
    return d


def printSparkConfigLoader(env):
    d = {}
    conf = configparser.ConfigParser()
    conf.read("conf/spark.conf")
    for k, v in conf.items(env):
        d[k] = v
    return d
