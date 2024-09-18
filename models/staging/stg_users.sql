WITH source AS (
    SELECT 
        _airbyte_data AS data
    FROM 
        {{ source('airbyte_internal', 'default_raw__stream_users') }}
),

final AS (
    SELECT
        JSONExtractFloat(data, 'id') AS user_id,
        JSONExtractString(data, 'gender') AS gender,
        JSONExtractString(data, 'academic_degree') AS academic_degree,
        JSONExtractString(data, 'title') AS title,
        JSONExtractString(data, 'nationality') AS nationality,
        JSONExtractInt(data, 'age') AS age,
        JSONExtractString(data, 'name') AS name,
        JSONExtractString(data, 'email') AS email,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'created_at')) AS created_at,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'updated_at')) AS updated_at,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, '_airbyte_extracted_at')) AS _airbyte_extracted_at
    FROM source
)

SELECT * FROM final
