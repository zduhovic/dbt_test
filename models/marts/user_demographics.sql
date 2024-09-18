WITH users AS (
  SELECT 
    user_id,
    gender,
    academic_degree,
    nationality,
    age
  FROM {{ ref('stg_users') }}
),

final AS (
SELECT 
  gender,
  academic_degree,
  nationality,
  AVG(age) AS average_age,
  COUNT(user_id) AS user_count
FROM users
GROUP BY gender, academic_degree, nationality
)

select * from final
