WITH source AS (
    SELECT 
        _airbyte_data AS data
    FROM 
        {{ source('airbyte_internal', 'default_raw__stream_purchases') }}
),

final AS (
    SELECT
        JSONExtractFloat(data, 'id') AS purchase_id,
        JSONExtractFloat(data, 'user_id') AS user_id,
        JSONExtractFloat(data, 'product_id') AS product_id,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'purchased_at')) AS purchased_at,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'returned_at')) AS returned_at,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'added_to_cart_at')) AS added_to_cart_at,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'created_at')) AS created_at,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'updated_at')) AS updated_at,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, '_airbyte_extracted_at')) AS _airbyte_extracted_at
    FROM source
)

SELECT * FROM final
