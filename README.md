# BDA_ProjGrF

## Datas
Chaque ligne d'un fichier contient des informations sur une course de taxi à New York. 
Pour chaque course, on possède un hash du médaillon du taxi et un hash du numéro de licence 
du chauffeur. Sur la course elle-même, on connait des informations sur la temporalité :
un datetime pour le début de la course et des informations sur la localisation : la latitude
et la longitude du début et de la fin de la course. D'autres informations sont fournies comme
le nombre de personnes transportée, le temps de trajet et la distance parcourus.


#### Header d'un fichier
medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude


#### Fichiers
Le dataset et composé de 12 fichiers contenant 14 millions de lignes chacun. La totalité du dataset pèse 30gb
http://www.andresmh.com/nyctaxitrips/

## Features utilisées et pre-processing
Les localisations de latitude et de longitude sont transformées en quartier de New York. La distance est transformée en kilomètre.


## Analyse de départ :
Analyse du temps entre les course selon le quartier de New-York.


## Questions supplémentaires :

1. Vitesse moyenne sur le trajet selon le quartier
2. Les heures où il y a le plus de course par quartier
3. Le nombre moyen de personne déplacé par un chauffeur en une journée

## Résolution
1. Vitesse moyenne sur le trajet selon le quartier
Pour trouver la vitesse nous avons ajouté à notre classe trip la distance ainsi que le temps de la course. Ensuite nous avons gardé uniquement les courses avec l'arrrivé et le départ dans le même quartier. Pour finir avec une fonction udf, on calcul la vitesse en convertissant la distance en kilomètre et en divisant par le temps en heure.

2. Les heures où il y a le plus de course par quartier
Nous avons dû récupérer le quartier de la position de départ. en supprimant les positions incorrects. Après nous avons ajouté la variable hourTime à notre case class trip elle contient l'heure de la journée. Pour avoir cette nous avons un parser qui garde uniquement l'heure des timestamps. On regroupe les données par heure et par quartier en comptant le nombre d'enregistrement. pour finir nous classons les quartiers par nombre d'enregistrement. Pour garder seulement le plus grand on utilise dropDuplicate qui garde uniquement la première instance de chaque quartier et donc en triant avant on garde uniquement les maximums.

3. Le nombre moyen de personne déplacé par un chauffeur en une journée
Nous avons d'abords essayer en regroupant les donnée par taxi(license) et par date mais nous n'arivions pas faire une somme lors du regroupement par taxi puis une moyenne lors du regroupant par date.

Nous avons donc essayer avec une autre manière en utilisant un foreach sur la liste des taxis qui retournait la moyenne pour le taxi mais le temps de traittement étais très long.

## Optimisations
### Utilisation de session
Il est possible avec spark sql de créer des partitions. Nous avons essayé pour la dernière question de créer des partitions avec les différentes licences. Cependant, nous avons rencontrer des problèmes pour utilier les partitions.

## Résultats
1. Vitesse moyenne en Km/h dans les différents quartiers de New York 

| Borough | AvgSpeed |
| -------- | -------- | 
| Queens    | 33.09861308585534     | 
| Brooklyn    | 23.6755087731464     | 
| Bronx    | 22.726006951631792    | 
| Staten Islan    |20.432806529231634     | 
| Manhattan    | 20.01197981484557     | 

![](https://i.imgur.com/7OQQav5.png)

On peut remarquer que dans certain quartier il est plus facile de circuler en général. À Manhattan, où il y beaucoup de circulation, la vitesse moyenne est plus basse qu'ailleure.


2. L'heure par quartier avec le plus de course de taxi

|Hour of the day|      Borough|count|
|--------|-------------|-----|
|      17|       Queens| 3145|
|       1|     Brooklyn| 1913|
|       8|Staten Island|   11|
|      18|    Manhattan|55521|
|       7|        Bronx|   81|

On remarque que le soir il vaut mieux être à Manhattan ou le Queens pour avoir plus de chance d'avoir des clients. Staten Island et le Bronx sont plutôt des quartiers matinales. Avec ces indications les chauffeurs de taxis peuvent savoir où à quelle heure il est préférable de ce placer dans un quartier pour avoir des clients.

3. le nombre de voyageur moyen par taxi par jour

Max : 270

Moyenne : 27,36

Mediane : 17.33

Min : 1

## Améliorations possibles
En utilisant une carte et les points geojson nous pourrions produire une heatmap des départ ainsi que des arrivés.

Le dataset de base a beaucoup de row mais manque un peu de features, il serait intéréssant d'avoir les points qui constitue le trajet effectué par le taxi. Avec cela nous pourrions les déssiné sur une carte et trouvé les routes les plus emprunter.






