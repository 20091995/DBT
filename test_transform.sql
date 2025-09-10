WITH source_data AS (
    SELECT * EXCLUDE(VendorID, RatecodeID) 
    FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
),

filtered_data AS 
            (SELECT *
            FROM source_data
            WHERE tpep_pickup_datetime < tpep_dropoff_datetime 
                AND passenger_count >0 
                AND trip_distance > 0  
                AND total_amount >0   
                AND payment_type IN (1,2)
                ),

transformed_data AS (
    SELECT 
        CAST(passenger_count AS BIGINT) AS Nombre_Passager,
        CASE
            WHEN payment_type = 1 THEN 'Credit card' 
            WHEN payment_type = 2 THEN 'Cash'        
            END AS Method_Paiement,
        DATE_DIFF('minute',tpep_pickup_datetime,tpep_dropoff_datetime) AS trip_duration_mns,
        * EXCLUDE (passenger_count, payment_type) 
        FROM filtered_data
)

SELECT * 
FROM transformed_data
LIMIT 10;
