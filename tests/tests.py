'''
Created on 25 de jul de 2016

@author: m140365
'''
import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import Row
from pyspark.sql.types import *
from pyspark.sql.types import DoubleType
from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

import logging
import atexit
from numpy import array
import numpy as np
import datetime as dt
from pyspark.mllib.linalg import Vectors
from pylab import plot,show,hist,figure,title
from pyspark.sql.window import Window
import unittest2
import application
import os, inspect


import findspark
findspark.init()


class Testsitapl02(unittest2.TestCase):
    
    def setUp(self):
        self.sc = pyspark.SparkContext('local[1]')
        self.sqlContext = pyspark.SQLContext(self.sc)
        self.quiet_logs(self.sc)
        
    def tearDown(self):
        self.sc.stop()
    
    def test_such_as_original_code(self):
    
        rows = [Row('$1$BXSWV24w$n3Fk0n9UZMDrMy80gXALP.-760-3-3', 1.0),
                Row('$1$6XfmeOOM$TgISMVWjJ4HlAScJhtFyq/-2911-3-3', 1.0), 
                Row('$1$6XfmeOOM$TgISMVWjJ4HlAScJhtFyq/-2530-3-3', 1.0)]
        file_train = "../resources/sitapl02_saldo_conta_train.csv"
        self.call_modelST2(rows, file_train)        
        
    def test_model_validation(self):
        #somente positivos        
        rows = [Row('$1$ryh3v7.g$IlBuGfBDrcOCU2bBXmv751-1298-3-3', 1.0),
                Row('$1$o9kaQ5ac$13Tpv7/qtOMa2ldDqKSKM/-5302-3-3', 0.87)]
        file_test = "../resources/sitapl02_saldo_conta_test.csv"
        self.call_modelST2(rows, file_test)
        
    def call_modelST2( self, rows, file_data ):
        rdd = self.sc.parallelize(rows)
        schema = StructType([StructField('account_num', StringType(), True), StructField('percentil', DoubleType(), True)])
        df_resultExpected = self.sqlContext.createDataFrame(self.sqlContext.createDataFrame(rdd).take(10), schema)
        df_dataSet = self.sqlContext.read.format('com.databricks.spark.csv').options(header='true', delimiter=',').load(file_data)
        
        df_result = application.modelST2(df_dataSet, 10, 0).toDF()
        df_resultExpected.sort(df_resultExpected.account_num.asc())
        df_result.sort(df_result.account_num.asc())
        
    def quiet_logs( self, sc ):
      logger = sc._jvm.org.apache.log4j
      logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
      logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )    
            
if __name__ == '__main__':
    unittest2.main()