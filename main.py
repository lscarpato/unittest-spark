'''
Created on 19 de jul de 2016

@author: M146545
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext
from sitapl01.application import modelST1
from sitapl02.application import modelST2
import os
from pyspark.conf import SparkConf
from pyspark.sql.types import Row


if __name__ == '__main__':
    
    #Function that convert the Money 00.00 to Cents 0000
    toDecimal = lambda x: int(x*100)
    
    #Function that give you back the Cents 0000 to Money 00.00
    toFloat = lambda x: float(x)/100
    
    os.environ["PYSPARK_PYTHON"]="/grid/0/emc/anaconda2/bin/python"

    sc = SparkContext(appName='MAIN_SITAPP')
    sqlContext = SQLContext(sc)
    
    filePath = '/ingestao/amostra_cem_mil/extrato.txt'    
    
    df_extrato = sqlContext.read.format('com.databricks.spark.csv') \
                           .options(header='true') \
                           .options(delimiter='|') \
                           .load(filePath)
    df_extrato_train = df_extrato.filter(df_extrato.event_start_dt < '2015-11-01')
    
    df_extrato_test = df_extrato.filter(df_extrato.event_start_dt >= '2015-11-01')
    '''     
    df_extrato = df_extrato.where(df_extrato.account_num.isin({'$1$YFipXtXe$973ugzUty5opPKUkLzJZ91',
                                                        '$1$zIYnZyi3$C3Jp8xt.cXoaXBrwkCLBL1',
                                                        '$1$kTnjFjUl$m73ng0PzucWUGenhXcG3z/',
                                                        '$1$PFpQrNA0$.jFyVEXouVgD2DjNMzkUu0',
                                                        '$1$E1u4/Mw2$ncMHiLu3AaHHgFIYxTuBP/',
                                                        '$1$VVRHMhIT$.kbtI/P8XxfGLWTqqVavH.',
                                                        '$1$CfqbZ1Fb$ewmLbDt3B32ynMH4Cqap61',
                                                        '$1$TTqfpFuy$MrSwxBihcmx/x16uNAB0A/',
                                                        '$1$YDg1Vev7$yUbGUoy3wZWKM4kFcOylv.',
                                                        '$1$dX5tx0/m$RU9.YThk45Z.BTbbWDATp1',
                                                        '$1$SydnVh2W$Vrxhb60Nz1Wlo8FD3xeRx0',
                                                        '$1$1BvbqeUu$WP/kLXWrElHaRYIWHGgd0.',
                                                        '$1$E1u4/Mw2$ncMHiLu3AaHHgFIYxTuBP/',
                                                        '$1$CVvoa2er$P/JtTqJ9hwGreh8QogqLX1',
                                                        '$1$8YWIA0aT$WbmV4nbMX/NqCV1maN5BP1',
                                                        '$1$lLagfsgs$gY/muloScKx7XSdd7eqsv0',
                                                        '$1$ZbDqpH0B$Ff5u8zIw0XMZCD30ihQaZ.',
                                                        '$1$wSGFBj9K$/7QIwk/SGKOTsMJ.PHh3.0',
                                                        '$1$ejG4POqv$y3xFZ14p7YUYZ3vpNHq5c1',
                                                        '$1$TfDYzJzq$mbgoiq51SMrSvv9.7CaL9.'}))
    '''
    
    
    model1, model2 = modelST1(df_extrato, "2015-04-06", 6, 12, '201503', '2015-04-06', df_test=df_extrato)
    
    
    dfModel1 = model1.toDF()
    #dfModel1.show(25)
    
    dfModel2 = model2.toDF()
    #dfModel2.show(5)
    
    fileName_sld_in = '/ingestao/amostra_cem_mil/cem_saldo_conta.txt'
    
    
    
    df_saldo_conta = sqlContext.read.format('com.databricks.spark.csv')\
                           .options(header='true', delimiter='|',\
                                    charset='UTF-8', \
                                    nullValue = 'NA')\
                           .load(fileName_sld_in)
    
    df_saldo_conta_train = df_saldo_conta.filter(df_saldo_conta.dmovto < '2015-12-01')
                       
    df_saldo_conta_test = df_saldo_conta.filter(df_saldo_conta.dmovto >= '2015-12-01')
    
    model4 = modelST2(df_saldo_conta_train, 10, 1000)
    dfModel3 = model4.toDF()
    #dfModel3.show(5)
    
    
    
    
      
    bigVector = dfModel1.join(dfModel2, 'account_num', 'outer') \
                        .join(dfModel3, 'account_num', 'outer') \
                        .dropDuplicates()
    
    
                        
    bigVector = bigVector.rdd.map(lambda x: Row(account_num=x.account_num, \
                                                sa1_output_flag= 1 if x.flag_sa1_m1 > 0 or x.valor_sa1_m2 > 0 else 0,\
                                                sa1_output_valor=toFloat(toDecimal(x.valor_sa1_m1 + x.valor_sa1_m2)),\
                                                sa2_output_flag= 1 if x.percentil_sitApp2_mod1 > 0.0 else 0,\
                                                sa2_output_valor=toFloat(toDecimal(x.percentil_sitApp2_mod1)),\
                                                cota_unica= toFloat(toDecimal(x.valor_sa1_m1 + x.valor_sa1_m2 + x.percentil_sitApp2_mod1)),\
                                                cota_unica_flag= 1 if x.valor_sa1_m1 + x.valor_sa1_m2 + x.percentil_sitApp2_mod1 > 0.0 else 0)).toDF()
                                               
    dfModel1.show(25)
    dfModel2.show(25)
    dfModel3.show(25)                                            
    bigVector.show(25)
    #print bigVector.count()
        
    
    
    
    
    
    
    
    