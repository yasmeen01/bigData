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
sup=[]
inf=[]
H=[]
F=[]
abolshd=[]
nbwhyO=[]
nbwhyN=[]
nbchildN=[]
nbchildO=[]

def per(x,y):
	z=x+y
	return x*100/z



for f in os.listdir(path): #marche comme un ls, ici le f représerente mes fichiers .csv
    pathname = os.path.join(path, f) #récupérer les fichiers csv un par un en fesons un join entre mon path "./data/données/" et le nom de mon fichier
    s=pathname.split('/')
    name=s[len(s)-1]
    print(name)
    if (re.search(r,pathname)): #on vérifie que c'est bien un fichier .csv que g récupérer
        df = spark.read.format('csv').options(header=True, inferSchema=True).load(pathname) #loader les données
        os.system("cp "+pathname+" ./data/finished/"+name) #copier les fichiers qui ont était traité dans un dossier finished

        #calculer le pourcentage des employée de plus de 30ans
        nb=df.filter("age > 30").select(['age']).count()
        cpt=df.count()
        sup30=nb*100/cpt
        #calculer le pourcentage des employée de moins de 30ans
        nb=df.filter("age < 30").select(['age']).count()
        cpt=df.count()
        inf30=nb*100/cpt
        sup.append(sup30)
        inf.append(inf30)


        #l'année ou y a eu plus de poste abolished (en %)
        abolished=df.filter("joblost='position_abolished'").count()
        abolshd.append(nb*100/cpt)


        dfplot=df.groupBy('sex').count()
        y=dfplot.toPandas()['count'].values.tolist()
        f1=per(y[0],y[1])
        h1=100-f1
        F.append(f1)
        H.append(h1)


        #ceux qui on +12, pourquoi ils se vont virer le plus? (filtre school 12 yes)
        reason=df.filter("school12='yes'").groupby('joblost').count()
        nbreason=reason.toPandas()['count']
        reason=reason.toPandas()['joblost'].values.tolist()

        #why school12 slack at work: comapre btw married and non married
        whyO=df.filter((df.school12=='yes') & (df.joblost=="slack_work") & (df.married=='yes')).count()
        whyN=df.filter((df.school12=='yes') & (df.joblost=="slack_work") & (df.married=='no')).count()
        whyTot=df.filter((df.school12=='yes') & (df.joblost=="slack_work")).count()
        perO=whyO*100/whyTot
        perN=whyN*100/whyTot
        nbwhyN.append(whyN)
        nbwhyO.append(whyO)

        #the reason of the slack_word is it the woman or he child
        childO=df.filter((df.school12=='yes') & (df.joblost=="slack_work") & (df.married=='yes') & (df.dkids=='yes')).count()
        childN=df.filter((df.school12=='yes') & (df.joblost=="slack_work") & (df.married=='yes') & (df.dkids=='no')).count()
        childT=df.filter((df.school12=='yes') & (df.joblost=="slack_work") & (df.married=='yes')).count()
        pO=childO*100/childT
        pN=childN*100/childT
        nbchildN.append(childN)
        nbchildO.append(childO)




        #################### ELhadj ###################################

        #Taux de chommage par etat en pourcentage 
        taux_Chomage_par_Etat = df.groupBy("state").agg({"stateur" : "max"})
        #taux_Chomage_par_Etat.collect()
        
        #################### ELhadj ###################################





##############Stockage résultat dans des fichiers.csv (dans dossier result)#############################
        # res.repartition(1).write.csv("./data/result/sex"+name)
        # abolished.write.csv("./data/result/abolished_"+name)  #enregistrer les fichiers résultant







######################## VISUALISATION #########################################

x1=['année_1','année_2','année_3','année_4']

#Etude age : qui d'entre moins de 30ans ou plus d e30ans se font le plus viré
plt.bar(x1,sup,label=">30",width=.5)
plt.bar(x1,inf,label="Femme",width=.4)
plt.xlabel("Les mois")
plt.ylabel("Nb H/F")
plt.title("comparaison en mois entre le nombre (en %) de F et H")
plt.legend(facecolor="grey")
plt.show() 


#Etude sex : qui des femmes ou hommes se font le plus viré
plt.bar(x1,H,label="Hommes",width=.5)
plt.bar(x1,F,label="Femme",width=.4)
plt.xlabel("Les mois")
plt.ylabel("Nb H/F")
plt.title("comparaison en mois entre le nombre (en %) de F et H")
plt.legend(facecolor="grey")
plt.show() 


#Postes abolis (en %) par an
plt.pie(abolshd, labels=x1, autopct='%1.1f%%',shadow=True, startangle=90)
plt.title("Postes abolis (en %) par an")
plt.show()

###### School12 case ##############
#why does a school12 get fired 
plt.pie(nbreason, labels=reason, autopct='%1.1f%%',shadow=True, startangle=90)
plt.title("reasons why school12 get fired")
plt.show()
#slack_work is the answer! then why do they slack at work?

plt.barh(x1,whyO,label="Married")
plt.barh(x1,whyN,label="Not_Married")
plt.xlabel("Pourcentages")
plt.title("Comparaison entre les bac+12 marrié et non marrié")
plt.legend(facecolor="grey")
plt.show() 
#because they're married is the anwser. then is the wife the reason or the children?


plt.barh(x1,childO,label="has_Kids")
plt.barh(x1,childN,label="Hasn't_Kids")
plt.xlabel("Pourcentages")
plt.title("Comparaison entre les bac+12 avec et sans enfants")
plt.legend(facecolor="grey")
plt.show() 

########end case #############""










# dfplot=df.groupBy('sex').count()

# x=dfplot.toPandas()['sex'].values.tolist() #faut tjrs mettre en liste car avec le spark le format du résultat n'est pas iterable
# y=dfplot.toPandas()['count'].values.tolist()
# plt.bar(x,y,color="green",label="Femme")
# plt.xlabel("mon X")
# plt.ylabel("mon Y")
# plt.title("comparaison en semaines")
# plt.legend(facecolor="grey")
# plt.show() 

