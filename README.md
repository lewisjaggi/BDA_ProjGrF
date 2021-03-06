# BDA_ProjGrF

## Datas
Chaque ligne d'un fichier contient des informations sur une course de taxi à New York. 
Pour chaque course, on possède un hash du médaillon du taxi et un hash du numéro de licence 
du chauffeur. Sur la course elle-même, on connait des informations sur la temporalité :
un datetime pour le début de la course et des informations sur la localisation : la latitude
et la longitude du début et de la fin de la course. D'autres informations sont fournies comme
le nombre de personnes transportées, le temps de trajet et la distance parcourue.


#### Header d'un fichier
medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude


#### Fichiers
Le dataset et composé de 12 fichiers contenant 14 millions de lignes chacun. La totalité du dataset pèse 30GB
http://www.andresmh.com/nyctaxitrips/

## Features utilisées et pre-processing
Les localisations de latitude et de longitude sont transformées en quartier de New York. La distance est transformée en kilomètre.


## Analyse de départ :
Le but de départ du projet et de connaître le quartier où il est plus facile de retrouver une course. Pour trouver cette information, plusieurs opérations sont effectuées. Les enregistrements invalides sont supprimées. Par exemple s'il y a des valeurs null, des champs vides, si le drop off datetime est avant le pick off ou si le temps de course est trop long. Ensuite le temps de course est calculé et les valeurs en dessous de 0 sont supprimées. Les postions (latitude, longitude) sont transformées en quartier. Les coordonnées sont transformées en croodonnées 2d (WKID) est grâce à une librairie de géométrie 2d et un fichier geojson elles sont converties en quartier. Ensuite grâce au système de session, toutes les courses d'un taxi sont regroupées et triées par le pickup time et on calcul le temps entre les course en les prennant deux à deux. Pour finir on calcul la standard déviation et la moyenne par quartier.


## Questions supplémentaires :

1. Vitesse moyenne sur le trajet selon le quartier
2. Les heures où il y a le plus de course par quartier
3. Le nombre moyen de personnes déplacé par un chauffeur en une journée

## Résolution
1. Vitesse moyenne sur le trajet selon le quartier
Pour trouver la vitesse nous avons ajouté à notre classe trip la distance ainsi que le temps de la course. Ensuite nous avons gardé uniquement les courses avec l'arrivée et le départ dans le même quartier. Pour finir avec une fonction udf, on calcule la vitesse en convertissant la distance en kilomètre et en divisant par le temps en heure.

2. Les heures où il y a le plus de course par quartier
Nous avons dû récupérer le quartier de la position de départ. en supprimant les positions incorrects. Après nous avons ajouté la variable hourTime à notre case class trip elle contient l'heure de la journée. Pour avoir cette nous avons un parser qui garde uniquement l'heure des timestamps. On regroupe les données par heure et par quartier en comptant le nombre d'enregistrements. Pour finir, nous classons les quartiers par nombre d'enregistrement. Pour garder seulement le plus grand on utilise dropDuplicate qui garde uniquement la première instance de chaque quartier et donc en triant avant on garde uniquement les maximums.

3. Le nombre moyen de personnes déplacé par un chauffeur en une journée
Nous avons d'abord essayé en regroupant les données par taxi(license) et par date, mais nous n'arrivions pas faire une somme lors du regroupement par taxi puis une moyenne lors du regroupant par date.

Nous avons donc essayé avec une autre manière en utilisant un foreach sur la liste des taxis qui retournait la moyenne pour le taxi, mais le temps de traitement était très long.

## Optimisations
### Utilisation de session
Il est possible avec spark sql de créer des partitions. Nous avons essayé pour la dernière question de créer des partitions avec les différentes licences. Cependant, nous avons rencontré des problèmes pour utiliser les partitions.

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

On peut remarquer que dans certains quartiers il est plus facile de circuler en général. À Manhattan, où il y a beaucoup de circulation, la vitesse moyenne est plus basse qu'ailleurs.


2. L'heure par quartier avec le plus de course de taxi

|Hour of the day|      Borough|count|
|--------|-------------|-----|
|      17|       Queens| 3145|
|       1|     Brooklyn| 1913|
|       8|Staten Island|   11|
|      18|    Manhattan|55521|
|       7|        Bronx|   81|

On remarque que le soir il vaut mieux être à Manhattan ou le Queens pour avoir plus de chance d'avoir des clients. Staten Island et le Bronx sont plutôt des quartiers matinaux. Avec ces indications les chauffeurs de taxi peuvent savoir où à quelle heure il est préférable de se placer dans un quartier pour avoir des clients.

3. le nombre de voyageurs moyens par taxi par jour

Max : 270

Moyenne : 27,36

Mediane : 17.33

Min : 1

## Améliorations possibles
En utilisant une carte et les points geojson nous pourrions produire une heatmap des départs ainsi que des arrivés.

Le dataset de base a beaucoup de row mais manque un peu de features, il serait intéressant d'avoir les points qui constituent le trajet effectué par le taxi. Avec cela nous pourrions les dessiner sur une carte et trouver les routes les plus empruntées.






