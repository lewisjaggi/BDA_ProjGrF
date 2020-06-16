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


## Features utilisées et pre-processing
Les localisations de latitude et de longitude sont transformées en quartier de New York. La distance est transformée en kilomètre.


## Analyse de départ :
Analyse du temps entre les course selon le quartier de New-York.


## Questions supplémentaires :

- Vitesse moyenne sur le trajet selon le quartier
- Les heures où il y a le plus de course par quartier
- Le nombre moyen de personne déplacé par un chauffeur en une journée

## Optimisations

## Approche de test et d'évaluation

## Résultats

## Améliorations possibles


