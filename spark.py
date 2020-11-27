# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import *
import os,re
import time
import matplotlib.pyplot as plt 

path = "./data/données/"
r=".csv$"
for f in os.listdir(path):
    pathname = os.path.join(path, f)
    s=pathname.split('/')
    name=s[len(s)-1]
    print(name)
    if (re.search(r,pathname)):
        df = spark.read.format('csv').options(header=True, inferSchema=True).load(pathname)
        # #calcul age moyen
        df.agg({"age" : "mean"}).show()
        #comparaison en pourcentage des jeunes vs vieux
        df.describe(['age']).show()
        #calculer le pourcentage des employée de plus de 30ans
        nb=df.filter("age > 30").select(['age']).count()
        cpt=df.count()
        pour=nb/cpt
        pour*100

        #comparaison nb H vs F
        res=df.groupBy('sex').count()
        # res.repartition(1).write.csv("./data/result/sex"+name)

        #la semaines ou y a eu plus de poste abolished
        df.createOrReplaceTempView("data_table")
        # abolished=spark.sql("SELECT count(*) as jobEstablished FROM data_table where joblost='position_abolished'")
        # abolished.repartition(1).write.csv("./data/result/abolished_"+name)
