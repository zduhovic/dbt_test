WITH purchases AS (
  SELECT 
    product_id,
    COUNT(purchase_id) AS purchase_count
  FROM {{ ref('stg_purchases') }}
  GROUP BY product_id
),

products AS (
    SELECT 
      product_id,
      make,
      model
    FROM {{ ref('stg_products') }} 
),

final AS (
    SELECT 
      p.product_id,
      p.make,
      p.model,
      pc.purchase_count
    FROM products p
    LEFT JOIN purchases pc ON p.product_id = pc.product_id
    ORDER BY pc.purchase_count DESC
)

select * from final