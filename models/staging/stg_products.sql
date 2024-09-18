WITH source AS (
    SELECT 
        _airbyte_data AS data
    FROM 
        {{ source('airbyte_internal', 'default_raw__stream_products') }}
),

final AS (
    SELECT
        JSONExtractFloat(data, 'id') AS product_id,
        JSONExtractFloat(data, 'year') AS year,
        JSONExtractFloat(data, 'price') AS price,
        JSONExtractString(data, 'model') AS model,
        JSONExtractString(data, 'make') AS make,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'created_at')) AS created_at,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'updated_at')) AS updated_at,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, '_airbyte_extracted_at')) AS _airbyte_extracted_at
    FROM source
)

SELECT * FROM final
