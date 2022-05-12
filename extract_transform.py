"""The Extract/Transform portion of the pipeline.
The main function contains sub-functions to optimize the code and 
reduce the memory usage"""
from boto3.session import Session
import boto3
import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, DataFrame,SparkSession, SQLContext, Row
import configparser
from pyspark.sql.functions import udf, col, explode, avg, count, max, min, collect_list
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import time
import operator

#set up data parameters
bucket_input='sample_bucket'
category='category.txt'
bucket_category='test_bucket'
#set up AWS session
sessionM=Session() 
s3=sessionM.resource('s3')
client=boto3.client('s3')


def get_data(sc):
    """Extract data"""
    #distribute data to rdds
    rdd=sc.textFile('s3a://'+bucket_input+'/*') 
    #filtering text to remove unnecessary "long" portions
    rdd=rdd.filter(lambda x: len(x)<25)
    rdd=rdd.map(lambda x: x.encode("utf-8", "ignore"))
    #record the time when the processign started
    print("RDDs are complete. Time: " + str(time.time() -start_time)) 
    return(rdd)

def get_category(sc):
    """Get RDD categoryID"""
    rdd=get_data(sc);
    #Define RDD of category
    rdd=rdd.filter(lambda x: x.startswith('categoryId'))    
    rdd=rdd.map(lambda x: int((''.join(x)).split(':')[1]))
    return(rdd) 

def DF_category(sc):
    #Generate DataFrame "CategoryID"
    rdd=get_category(sc)
    rddDF = sqlContext.createDataFrame(rdd,IntegerType() )
    rddDF=rddDF.toDF("CategoryID")
    rddDF=rddDF.withColumn("id",row_number().over(Window.orderBy(lit(1))))
    #QC/View DF "Category"
    rddDF.show()  
    return(rddDF)

def get_year(sc):
    """Get RDD year"""
    rdd=get_data(sc);
    #Define RDD date
    rdd=rdd.filter(lambda x: x.startswith('publishedAt'))
    rdd2=rdd.map(lambda x: (''.join(x)).split(':')[1])
    #Define RDD year 
    rdd=rdd2.map(lambda x: int((''.join(x)).split('-')[0]))
    #QC/View some rrds
    print rdd.take(5)
    return(rdd);

def DF_year(sc):    
    #Generate DataFrame "Year"
    rdd=get_year(sc)
    rddDF= sqlContext.createDataFrame(rdd,IntegerType())
    rddDF=rddDF.toDF("Year")
    rddDF=rddDF.withColumn("id",row_number().over(Window.orderBy(lit(1))))
    return(rddDF)

def DF_cat_year(sc):
    """Transform the extracted DFs of category and year into a dataframe (DF)""" 
    rddDF=DF_category(sc);
    rddDF2=DF_year(sc);
    
    rddDF=rddDF.join(rddDF2, rddDF.id==rddDF2.id).drop('id')
    #set up timer to check how long the job runs
    DF_time=time.time() 
    print("DF is done . Time: " + str(DF_time -start_time))
    #QC/view portion of df
    rddDF.show()
    return(rddDF);

def output_data(sc):    
    #Preparing to write output
    joinDF=DF_cat_year();
    db_properties = {}
    config = configparser.ConfigParser()
    config.read("db_properties.ini")
    db_properties['username'] = 'xxx'
    db_properties['password'] = 'xxx'
    #specify the location of the postgres database (EC2)
    db_properties['url'] = 'jdbc:postgresql://xxx.compute-1.amazonaws.com/videos'
    db_properties['driver'] = 'org.postgresql.Driver'

    #Write to table
    jointDF3.write.format("jdbc").options(
    url=db_properties['url'],
    dbtable='main_table',
    user='xxx',
    password='xxx',
    stringtype="unspecified"
    ).mode('append').save()
    

if __name__ == '__main__':

    start_time=time.time()
    sc = SparkContext(conf=SparkConf().setAppName("videos"))
    spark_session = SparkSession.builder.appName("videos").getOrCreate()
    sqlContext = SQLContext(sc)
    #DF_cat_year(sc)
    #get_year(sc)
    #get_year(sc)
    output_data(sc)
