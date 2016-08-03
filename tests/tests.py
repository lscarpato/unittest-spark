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
    
             
        

        
    def quiet_logs( self, sc ):
      logger = sc._jvm.org.apache.log4j
      logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
      logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )    
            
if __name__ == '__main__':
    unittest2.main()
