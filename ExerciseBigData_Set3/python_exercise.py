"""
Student Name :  Charlene LAGADEC
Student ID   :  014962406
"""
from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, countDistinct
from pyspark.mllib.stat import Statistics
from data import Data
from object import Object
from word import Word
from select import select
from operator import add

import time

class ExerciseSet3(object):
    """
    Big Data Frameworks Exercises
    https://courses.helsinki.fi/en/data14001/124845011
    # https://www.cs.helsinki.fi/courses/582740/2017/k/k/1
    """

    def __init__(self):
        """
        Initializing Spark Conf and Spark Context here
        Some Global variables can also be initialized
        """
        self.conf = (SparkConf().setMaster("local[2]").
                     setAppName("exercise_set_3").
                     set("spark.executor.memory", "2g"))
        self.spark_context = SparkContext(conf=self.conf)
        
        self.sparkSession = SparkSession.builder.config(conf = self.conf).appName("spark session").getOrCreate()
        
        #self.file = "carat-context-factors-percom.csv"
        self.file = "carat_data.csv"
        self.filewords = "stopwords.txt"
        self.filedata = "onlytxt"

    def exercise_1(self):
        
        numPartitions = 10
        
        data = self.spark_context.textFile(self.file, numPartitions)
        
        values = data.map(lambda line: line.split(";"))
        objects = values.map(lambda entry: Data(entry[0], entry[1], float(entry[2]), entry[3], float(entry[4]), float(entry[5]), entry[6], 
                                                entry[7], entry[8], entry[9], entry[10], float(entry[11]), entry[12], float(entry[13])))
        
        objects = objects.filter(lambda x : x.cpuUsage >= 0 and x.cpuUsage <= 1)
        objects = objects.filter(lambda x : x.distanceTraveled >= 0)
        objects = objects.filter(lambda x : x.screenBrightness >= 0 and x.screenBrightness <= 255)
        objects = objects.filter(lambda x : x.wifiSignalStrength >= -100 and x.wifiSignalStrength <= 0)
        objects = objects.filter(lambda x : x.batteryTemperature >= 0)
        
        #print(objects.take(2))
        
        #Pearson correlation : energyRate, cpuUsage 
        start = time.time()
        x1 = objects.map(lambda x : x.energyRate)
        x2 = objects.map(lambda x : x.cpuUsage)
        corr_mat = Statistics.corr(x1, x2, method="pearson")
        print(corr_mat)
        end = time.time()
        print(end - start)
        
        #Pearson correlation : energyRate, screenBrightness 
        start = time.time()
        x2 = objects.map(lambda x : x.screenBrightness)
        corr_mat = Statistics.corr(x1, x2, method="pearson")
        print(corr_mat)
        end = time.time()
        print(end - start)
        
        #Pearson correlation : energyRate, wifiLinkSpeed 
        start = time.time()
        x2 = objects.map(lambda x : x.wifiLinkSpeed)
        corr_mat = Statistics.corr(x1, x2, method="pearson")
        print(corr_mat)
        end = time.time()
        print(end - start)

        #Pearson correlation : energyRate, wifiSignalStrength 
        start = time.time()
        x2 = objects.map(lambda x : x.wifiSignalStrength)
        corr_mat = Statistics.corr(x1, x2, method="pearson")
        print(corr_mat)
        end = time.time()
        print(end - start)

        return None

    def exercise_2(self):
        
        caratDF = self.sparkSession.read.format("csv").option("header", "false").option("delimiter",";").load(self.file)
        caratDF = caratDF.toDF("energyRate","batteryHealth","batteryTemperature","batteryVoltage","cpuUsage",
                               "distanceTraveled","mobileDataActivity","mobileDataStatus","mobileNetworkType",
                               "networkType","roamingEnabled","screenBrightness","wifiLinkSpeed","wifiSignalStrength")
        
        caratDF.printSchema()
        caratDF.show()
        
        caratDF = caratDF.withColumn("batteryTemperature", caratDF["batteryTemperature"].cast(DoubleType()))
        caratDF = caratDF.withColumn("batteryVoltage", caratDF["batteryVoltage"].cast(DoubleType()))
        
        caratDF.printSchema()
        
        caratDF.agg(countDistinct(col("batteryTemperature")).alias("count unique batteryTemperature")).show()
        caratDF.agg(countDistinct(col("batteryVoltage")).alias("count unique batteryVoltage")).show()
        
        outlierVoltage = caratDF.where((col("batteryVoltage") <= 2) | (col("batteryVoltage") >= 4.35))
        outlierVoltage.agg(countDistinct(col("batteryVoltage")).alias("count outlier batteryVoltage")).show()
        
        outlierTemperature = caratDF.where((col("batteryTemperature") <= 0) | (col("batteryTemperature") >= 50))
        outlierTemperature.agg(countDistinct(col("batteryTemperature")).alias("count outlier batteryTemperature")).show()
        
        
        carat = caratDF.select(col("energyRate"), col("cpuUsage"), col("screenBrightness"), col("wifiSignalStrength"),
                               col("wifiLinkSpeed"))
        carat = carat.where((col("cpuUsage") >= 0) & (col("cpuUsage") <= 1) & (col("screenBrightness") >= 0)
                            & (col("screenBrightness") <= 255) & (col("wifiSignalStrength") >= -100) & 
                            (col("wifiSignalStrength") <= 0))
        
        carat.printSchema()
        carat.show()
        
        '''Pearson correlation : energyRate, cpuUsage '''
        start = time.time()
        data = carat.select(col('energyRate').cast("float"), col('cpuUsage').cast("float"))
        corr_mat = data.corr(col1='energyRate', col2='cpuUsage', method="pearson")
        print(corr_mat)
        end = time.time()
        print(end - start)
        
        '''Pearson correlation : energyRate, screenBrightness '''
        start = time.time()
        data = carat.select(col('energyRate').cast("float"), col('screenBrightness').cast("float"))
        corr_mat = data.corr(col1='energyRate', col2='screenBrightness', method="pearson")
        print(corr_mat)
        end = time.time()
        print(end - start)
        
        '''Pearson correlation : energyRate, wifiLinkSpeed '''
        start = time.time()
        data = carat.select(col('energyRate').cast("float"), col('wifiLinkSpeed').cast("float"))
        corr_mat = data.corr(col1='energyRate', col2='wifiLinkSpeed', method="pearson")
        print(corr_mat)
        end = time.time()
        print(end - start)
        
        '''Pearson correlation : energyRate, wifiSignalStrength '''
        start = time.time()
        data = carat.select(col('energyRate').cast("float"), col('wifiSignalStrength').cast("float"))
        corr_mat = data.corr(col1='energyRate', col2='wifiSignalStrength', method="pearson")
        print(corr_mat)
        end = time.time()
        print(end - start)
        
        return None

    def exercise_3(self):
        
        numPartitions = 10
        
        data = self.spark_context.textFile(self.file, numPartitions)
        
        values = data.map(lambda line: line.split(";"))
        objects = values.map(lambda entry: Data(float(entry[0]), entry[1], float(entry[2]), entry[3], float(entry[4]), float(entry[5]), entry[6], 
                                                entry[7], entry[8], entry[9], entry[10], float(entry[11]), entry[12], float(entry[13])))
        
        objects = objects.filter(lambda x : x.screenBrightness > -1 and x.screenBrightness <= 255)
        
        obj = objects.map(lambda x : (x.screenBrightness, x)).groupByKey()
        print("Number of element for each brightness level")
        print(obj.mapValues(len).collect())
        
        print("Total energy rate for each brightness level")        
        objsum = objects.map(lambda x : (x.screenBrightness, x.energyRate))
        print(objsum.groupByKey().mapValues(sum).collect())
        
        result = obj.reduceByKey(add) 
        print(result.collect())
        
        return None

    def exercise_4(self):
        
        numPartitions = 10
        
        data = self.spark_context.textFile(self.filewords, numPartitions)
        
        values = data.map(lambda line: line.split('\n'))
        objects = values.map(lambda entry: Word(entry[0]))
        words = objects.map(lambda x : x.word)
        
        print(words.collect())
        
        data_bd = self.spark_context.broadcast(words.collect())
        
        print(data_bd.value)
        
        
        rdd = self.spark_context.wholeTextFiles(self.filedata)
        #print(rdd.collect())
        
        rdd = rdd.map(lambda x : x not in data_bd.value)
        
        return None

    def exercise_5(self):
        
        
        return None

if __name__ == "__main__":
    EXERCISESET3 = ExerciseSet3()
    #EXERCISESET3.exercise_1()
    #EXERCISESET3.exercise_2()
    #EXERCISESET3.exercise_3()
    EXERCISESET3.exercise_4()
    EXERCISESET3.exercise_5()