WITH final AS (
    SELECT 
      user_id,
      product_id,
      purchased_at,
      added_to_cart_at,
      date_diff('second', purchased_at, added_to_cart_at) AS time_to_purchase_seconds,
      returned_at
    FROM {{ ref('stg_purchases') }}
)

select * from final