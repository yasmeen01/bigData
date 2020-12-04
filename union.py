from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import *
import os,re
import matplotlib.pyplot as plt


df1 = spark.read.format('csv').options(header=True, inferSchema=True).load("../data/données/data1.csv1.csv") #loader les données
df2 = spark.read.format('csv').options(header=True, inferSchema=True).load("../state.csv") #loader les données
l=df2.select(['nom'])
print(l)

test=df1.withColumn("name",l)
test.write.csv("../test")  #enregistrer les fichiers résultant
