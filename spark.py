# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import *
import os,re
import time
import matplotlib.pyplot as plt


#les fonctions faites:
# récupérer les fichiers un par un automatiquement
# déplacer les fichiers traiter dans un dossier nomé finished
# enregistrer les fichiers résultant dans un dossier nomé résult
# le squelette des dataviz

path = "./data/données/" #le chemin au dossier où se trouves mes fichers .csv
r=".csv$"
for f in os.listdir(path): #marche comme un ls, ici le f représerente mes fichiers .csv
    pathname = os.path.join(path, f) #récupérer les fichiers csv un par un en fesons un join entre mon path "./data/données/" et le nom de mon fichier
    s=pathname.split('/')
    name=s[len(s)-1]
    print(name)
    if (re.search(r,pathname)): #on vérifie que c'est bien un fichier .csv que g récupérer
        df = spark.read.format('csv').options(header=True, inferSchema=True).load(pathname) #loader les données
        os.system("cp "+pathname+" ./data/finished/"+name) #copier les fichiers qui ont était traité dans un dossier finished
        #calcul age moyen
        df.agg({"age" : "mean"}).show()
        #comparaison en pourcentage des jeunes vs vieux
        # df.describe(['age']).show()
        #calculer le pourcentage des employée de plus de 30ans
        # nb=df.filter("age > 30").select(['age']).count()
        # cpt=df.count()
        # pour=nb/cpt
        # pour*100
        
        
        #################### ELhadj ###################################

        #Taux de chommage par etat en pourcentage 
        taux_Chomage_par_Etat = df.groupBy("state").agg({"stateur" : "max"})
        taux_Chomage_par_Etat.collect()
        
        #################### ELhadj ###################################

        #comparaison nb H vs F
        # res.repartition(1).write.csv("./data/result/sex"+name)

        #la semaines ou y a eu plus de poste abolished
        df.createOrReplaceTempView("data_table")
        # abolished=spark.sql("SELECT count(*) as jobEstablished FROM data_table where joblost='position_abolished'")
        # abolished=df.filter("joblost='position_abolished'").count()
        # abolished.write.csv("./data/result/abolished_"+name)  #enregistrer les fichiers résultant





        # df.groupBy('sex').count().collect() #pour pouvoir iterer faut collecter d'abord
        df_pandas = df.toPandas()
        colors = ['lightcoral','blue', 'lightskyblue','green','red']
        labels=df_pandas.age
        # print(labels)
        # res = df.groupBy('age').count().toPandas().plot(x="age",y="count",kind="pie",colors=colors,shadow=True,labels=labels)#pie pour graph en comembert
        res = df.groupBy('age').count().toPandas().plot(x="age",y="count",kind="bar")#pour iterer faut d'abord collecter
        plt.show() 


# visualisation :
# avoir plusieurs bar/lines/pie graphs pour pouvoir comparer entre les semaines:
# plus on rajoute des plt.bar plus le bar en lui meme sera découpé 
dfplot=df.groupBy('sex').count()

x=dfplot.toPandas()['sex'].values.tolist() #faut tjrs mettre en liste car avec le spark le format du résultat n'est pas iterable
y=dfplot.toPandas()['count'].values.tolist()
x1=['semaine1','semaine2']
y1=[20,40]
plt.bar(x1,y,color="green",label="Femme")
plt.bar(x1,y1,color="blue",label="Hommes")
plt.xlabel("mon X")
plt.ylabel("mon Y")
plt.title("comparaison en semaines")
plt.legend(facecolor="grey")
plt.show() 






# dfplot=df.groupBy('sex').count()

# x=dfplot.toPandas()['sex'].values.tolist() #faut tjrs mettre en liste car avec le spark le format du résultat n'est pas iterable
# y=dfplot.toPandas()['count'].values.tolist()
# plt.bar(x,y,color="green",label="Femme")
# plt.xlabel("mon X")
# plt.ylabel("mon Y")
# plt.title("comparaison en semaines")
# plt.legend(facecolor="grey")
# plt.show() 

