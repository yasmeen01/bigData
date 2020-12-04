# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import *
import os,re
import matplotlib.pyplot as plt

#les fonctions faites:
# récupérer les fichiers un par un automatiquement  
# déplacer les fichiers traiter dans un dossier nomé finished
# enregistrer les fichiers résultant dans un dossier nomé résult
# le squelette des dataviz
print("inserer le chemin où se trouvent les fichiers")
path=input()
print(path)
# path = "./data/données/" #le chemin au dossier où se trouves mes fichers .csv
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
nbreason3=[]
nbreason1=[]
nbreason2=[]
nbreason4=[]
listeState=[]
nbTaux=[]
states=[]
sttes=[]
def per(x,y):
    z=x+y
    return x*100/z



for f in os.listdir(path): #marche comme un ls, ici le f représerente mes fichiers .csv
    pathname = os.path.join(path, f) #récupérer les fichiers csv un par un en fesons un join entre mon path "./data/données/" et le nom de mon fichier
    s=pathname.split('/')
    name=s[len(s)-1]
    print(name)
    if len(s[len(s)-1]) != 0:
        if (re.search(r,pathname)): #on vérifie que c'est bien un fichier .csv que g récupérer
            df = spark.read.format('csv').options(header=True, inferSchema=True).load(pathname) #loader les données
            os.system("cp "+pathname+" ../data/finished/"+name) #copier les fichiers qui ont était traité dans un dossier finished
                #calculer le pourcentage des employée de plus de 30ans
            nb=df.filter("age < 30").select(['age']).count()
            cpt=df.count()
            sup30=nb*100/cpt
            #calculer le pourcentage des employée de moins de 30ans
            nb=df.filter("age > 30").select(['age']).count()
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
            labl=reason.toPandas()['joblost'].values.tolist()
            # nbreason=reason.toPandas()['count']
            # reason=reason.toPandas()['joblost'].values.tolist()
            if 'data2' in name:
                nbreason2=reason.toPandas()['count'].values.tolist()
            elif 'data1' in name:
                nbreason1=reason.toPandas()['count'].values.tolist()
            elif 'data3' in name:
                nbreason3=reason.toPandas()['count'].values.tolist()
            else:
                nbreason4=reason.toPandas()['count'].values.tolist()

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
            #dans le fichiers les states sont représenter en code postale
            #je voudrais afficher dans le graphe les noms des states et non les codes postaux
            df2 = spark.read.format('csv').options(header=True, inferSchema=True).load("../state.csv")
            dic=[]
            dic= df2.toPandas().set_index('state').T.to_dict('list')
            cmpt=0
            #Taux de chommage par etat en pourcentage 
            listeState=[]
            sttes=[]
            top10CodeP=[]
            taux_Chomage_par_Etat = df.groupBy("state").mean('stateur')
            nbTaux=taux_Chomage_par_Etat.toPandas()['avg(stateur)'].values.tolist()
            states=taux_Chomage_par_Etat.toPandas()['state'].values.tolist()
            while cmpt<10:
                listeState.append(round(max(nbTaux),2))
                maxindex=nbTaux.index(max(nbTaux))
                # top10CodeP.append(states[maxindex])
                sttes.append(dic[states[maxindex]])
                nbTaux.remove(max(nbTaux))
                cmpt+=1
            





    ##############Stockage résultat dans des fichiers.csv (dans dossier result)#############################
            # res.repartition(1).write.csv("./data/result/sex"+name)
            # reason.write.csv("../data/result/reason_chomage_"+name)  #enregistrer les fichiers résultant







######################## VISUALISATION #########################################

if __name__ == '__main__':
    x1=['mois_1','mois_2','mois_3','mois_4']

    #Etude sex : qui des femmes ou hommes se font le plus viré
    plt.bar(x1,H,label="Hommes",width=.5)
    plt.bar(x1,F,label="Femme",width=.4)
    plt.xlabel("Les mois")
    plt.ylabel("Nb H/F")
    plt.title("Taux de chomage(en %) entre les femmes et Hommes")
    plt.legend(facecolor="grey")
    plt.show() 


    #Etude age : qui d'entre moins de 30ans ou plus d e30ans se font le plus viré
    plt.bar(x1,sup,label="age > 30",color=('blue'),width=.5)
    plt.bar(x1,inf,label="age < 30",color=('grey'),width=.4)
    plt.xlabel("Les mois")
    plt.ylabel("Nb H/F")
    plt.title("Taux de chomage (en %) par age")
    plt.legend(facecolor="grey")
    plt.show() 


    #taux chomage par state (en %) par an
    sttes=['Louisiana','Montana','Vermont','Utah','Illinois','Maryland','Texas','Missouri','Warren','Kansas']
    plt.pie(listeState, labels=sttes, autopct='%1.1f%%',shadow=True, startangle=90)
    plt.title("Top 10 des states avec un taux de chomage le plus élevé (en %) ")
    plt.show()

    #Postes abolis (en %) par an
    plt.pie(abolshd, labels=x1, autopct='%1.1f%%',shadow=True, startangle=90)
    plt.title("Postes abolis (en %) par mois")
    plt.show()

    ###### School12 case ##############
    #why does a school12 get fired 

    plt.figure(0)
    plt.pie(nbreason1, labels=labl, autopct='%1.1f%%',shadow=True, startangle=90)
    plt.title("raisons pour les quels des school12 sont en chomage __mois_1__") #12ans d'école
    plt.figure(1)
    plt.pie(nbreason2, labels=labl, autopct='%1.1f%%',shadow=True, startangle=90)
    plt.title("raisons pour les quels des school12 sont en chomage __mois_2__") #12"ans d'école
    plt.figure(2)
    plt.pie(nbreason3, labels=labl, autopct='%1.1f%%',shadow=True, startangle=90)
    plt.title("raisons pour les quels des school12 sont en chomage __mois_3__") #12"ans d'école
    plt.figure(3)
    plt.pie(nbreason4, labels=labl, autopct='%1.1f%%',shadow=True, startangle=90)
    plt.title("raisons pour les quels des school12 sont en chomage __mois_4__") #12"ans d'école
    plt.show()
    #slack_work is the answer! then why do they slack at work?
    plt.barh(x1,nbwhyO,label="Married")
    plt.barh(x1,nbwhyN,label="Not_Married")
    plt.xlabel("Pourcentages")
    plt.title("Comparaison entre les school12 marrié et non marrié")
    plt.legend(facecolor="grey")
    plt.show() 
    #because they're married is the anwser. then is the wife the reason or the children?


    plt.barh(x1,nbchildO,color=('grey'),label="has_Kids")
    plt.barh(x1,nbchildN,color=('magenta'),label="Hasn't_Kids")
    plt.xlabel("Pourcentages")
    plt.title("Comparaison entre les school12 avec et sans enfants")
    plt.legend(facecolor="grey")
    plt.show() 



    plt.pie(listeState, labels=sttes, autopct='%1.1f%%',shadow=True, startangle=90)
    plt.title("Top 10 des state qui ont le taux de chomage le plus élevé ")
    plt.show()

#     ########end case #############""



# #rr = replacement rate, un pourcentage de la retraite.si tu as un salaide de 2000euros et que t'as rr à èà% alors la retraite sera plus de 1500euro