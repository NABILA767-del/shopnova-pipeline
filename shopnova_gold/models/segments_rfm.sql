SELECT
  customer_id,
  date_diff('day', max(created_at), current_date) AS recency,
  count(order_id) AS frequency,
  sum(total_amount) AS monetary,
  CASE
    WHEN count(order_id) > 5 AND sum(total_amount) > 3000 THEN 'VIP'
    WHEN count(order_id) > 2 THEN 'Fidèle'
    ELSE 'Occasionnel'
  END AS segment
FROM {{ ref('fct_orders') }}
GROUP BY 1