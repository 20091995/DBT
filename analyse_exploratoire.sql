------------- Analyse exploratoire de certaines variables 
-- .read './analyses/analyse_exploratoire.sql'

-- Nombre de Vendeur ID par enregistrement 

    SELECT VendorID, COUNT(*) AS Nombre, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS Pourcentage
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet' 
    GROUP BY VendorID;

-- Répartition des codes tarifaires

    SELECT RatecodeID, COUNT(*) AS Nombre, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS Pourcentage
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet' 
    GROUP BY RatecodeID;

-- Enregsitrement des trajets 

    SELECT store_and_fwd_flag, COUNT(*) AS Nombre, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS Pourcentage
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet' 
    GROUP BY store_and_fwd_flag;

-- Répartition des types de paiement.

    SELECT payment_type, COUNT(*) AS Nombre, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS Pourcentage
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet' 
    GROUP BY payment_type;

-- Répartition des PULocation : Activation trajet.

    SELECT PULocationID, COUNT(*) AS Nombre, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS Pourcentage
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet' 
    GROUP BY PULocationID;

-- Répartition des PULocation : Désactivation trajet.

    SELECT DOLocationID, COUNT(*) AS Nombre, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS Pourcentage
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet' 
    GROUP BY DOLocationID;

--Analyse des valeurs aberrantes dans les dates de prise et de dépôt 
--Vérification de la contrainte temporelle : 
-- la date de dépôt doit toujours être strictement postérieure à la date de prise

    SELECT COUNT(*) AS Nombre_erreurs
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    WHERE tpep_pickup_datetime > tpep_dropoff_datetime;

    SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, payment_type
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    WHERE tpep_pickup_datetime > tpep_dropoff_datetime
    LIMIT 10;

-- Analyse des distances négatives : la distance mesurée doit toujours être strictement positive ; 
-- toute valeur négative est considérée comme une anomalie.

    SELECT COUNT(*) AS trip_distance
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    WHERE trip_distance <=0;

    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    WHERE trip_distance <=0
    LIMIT 10;

-- Détection et analyse des trajets présentant une facturation égale à zéro ou inférieure à zéro.

    SELECT COUNT(*) AS total_amount
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    WHERE total_amount <=0;

    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, total_amount
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    WHERE total_amount <= 0
    LIMIT 10;

    -- Conclusion : pour garantir la fiabilité des analyses, il est préférable d’éliminer ces valeurs aberrantes.