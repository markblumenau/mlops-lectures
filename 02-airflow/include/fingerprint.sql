-- loaded_at намеренно исключён: сравниваем бизнес-данные одного фиксированного дня.
SELECT count(*) AS rows, sum(amount) AS revenue,
       md5(string_agg(row(order_id,order_date,created_at,city,channel,items,amount)::text,
                      E'\n' ORDER BY order_id)) AS business_hash
FROM raw.orders WHERE order_date = DATE '2026-09-01';
