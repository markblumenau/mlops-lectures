WITH expected AS (
 SELECT order_date, city, count(*) AS orders_cnt, sum(amount) AS revenue,
        round(avg(amount),2) AS avg_check
 FROM raw.orders WHERE amount IS NOT NULL GROUP BY 1,2
)
SELECT coalesce(r.order_date,m.order_date) AS day, coalesce(r.city,m.city) AS city,
       r.revenue AS expected_revenue, m.revenue AS actual_revenue
FROM expected r FULL OUTER JOIN marts.daily_sales m USING(order_date,city)
WHERE (r.orders_cnt,r.revenue,r.avg_check) IS DISTINCT FROM
      (m.orders_cnt,m.revenue,m.avg_check);
