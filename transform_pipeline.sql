WITH unioned AS (

    {% for month in range(1, 13) %}
        SELECT *,
            '2024-{{ "%02d"|format(month) }}' AS ym
        FROM read_parquet(
            'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{{ "%02d"|format(month) }}.parquet'
        )
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT * 
FROM unioned
LIMIT 10;