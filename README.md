# ***Membres de l'equipe***

#ELhadj Mamadou BAH
#Yasmine SMAILI
#Tiana

# ***Introduction***

Nous avons decide de mener une etude sur un fichier que nous avons scincer en 4 data sets. 

En effet, Ce fichier contient un nombre de 4877 ligne au total et 21 attributs(caracteristiques).
Il designe un ensemble d'individu carterise par des variables( statemb, Age, School12...), comme ceci est explique en details ci-dessous.

Ces personnes se trouvent actuellement au chomage  pour divers raisons( Joblost), appartenant a differents Etats(State) des Etats Unis.

Nous avons donc orienter notre etude sur certaine colone specifique comme ceci est indique dans les fonctions detaillees dans le  script<spark.py>.

Chaque data set (.csv) representera a lui seul un nombre de personnes ayant perdu leur emploi par Mois, et donc de maniere respective,les data sets data1, dat2, data3, data4 sont les premier, deuxieme, troisieme, et quatrieme Mois.
exemple: data1.csv represente la liste des personnes ayant perdu leur emploi dans le premier Mois.


# Details des noms et signification des attributs des Data Sets

1	Statemb: state maximum benefit level      --> indiquer le niveau maximal des prestations 

2	State  :state of residence code          -->  code de l'État de résidence

3	Age    :age in years                    --> âge en années

4	Tenure : years of tenure in job lost    --> années d'ancienneté dans l'emploi perdues

5	Joblost: a factor with levels (slack\_work, position\_abolished,seasonal\_job\_ended,other) 

6	Nwhite : non-white -->  non blanc?

7	school12: more than 12 years of school --> plus de 12 ans d'école ?

8	sex: a factor with levels (male,female)  --> un facteur avec des niveaux (masculin, féminin)

9	bluecol : blue collar worker ?  --> col bleu ?

10	smsa    : lives is smsa ?  --> vit c'est smsa ?

11	married :married ?       --> marié?   

12	dkids   : has kids ?                        --> a des enfants?

13	dykids  : has young kids (0-5 yrs) ?       --> a de jeunes enfants (0-5 ans)? 
 
14	yrdispl : year of job displacement (1982=1,..., 1991=10) --> année de suppression d'emploi (1982 = 1, ..., 1991 = 10)

15	rr      : replacement rate               -->     taux de remplacement 

16	head    : is head of household ?       -->     est le chef de famille?

17	ui      : applied for (and received) UI benefits ?  --> demandé (et reçu) des prestations d'assurance-chômage?

18	portfolio :has a portfolio of securities --> possède un portefeuille de titres

19	derivatives: has derivatives on portfolio (put and call) --> a des dérivés en portefeuille (put et call)

20	life insurance : has a life insurance --> a une assurance-vie

21	Sharpe: the sharpe ratio of the best portfolio --> le ratio sharpe du meilleur portefeuille

# Fonctions réalisées:
I- Etudes des corélations des columns
II-Etudes approfondie sur les ouvriers en chomage
III-Etudes comparatives des states

# Outils utilisés:
1-PySpark notebook
2-Pour la visualisation: Pandas, matplotlib

