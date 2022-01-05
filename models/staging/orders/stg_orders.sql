WITH orders_made as (
  SELECT * FROM {{ source('RDS', 'ORDERS') }}
), order_details as (
    SELECT * FROM {{ source('RDS', 'ORDER_DETAILS')}}
)
SELECT CONCAT('rds-', orders_made.order_id) AS order_id, order_date, CONCAT('rds-', employee_id) AS employee_id, CONCAT('rds-', customer_id) AS customer_id, 
CONCAT('rds-', product_id) AS product_id, quantity, discount, unit_price
FROM orders_made
LEFT JOIN order_details ON order_details.order_id = orders_made.order_id
ORDER BY order_date DESC