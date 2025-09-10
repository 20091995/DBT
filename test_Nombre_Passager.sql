SELECT *
FROM {{ ref('transform') }}
WHERE Nombre_Passager <= 0