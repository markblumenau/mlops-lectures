CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id    text PRIMARY KEY,
    order_date  date        NOT NULL,
    created_at  timestamp   NOT NULL,
    city        text        NOT NULL,
    channel     text        NOT NULL,
    items       integer     NOT NULL,
    amount      numeric(12, 2),
    loaded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_order_date_idx ON raw.orders (order_date);

CREATE TABLE IF NOT EXISTS marts.daily_sales (
    order_date  date    NOT NULL,
    city        text    NOT NULL,
    orders_cnt  integer NOT NULL,
    revenue     numeric(14, 2) NOT NULL,
    avg_check   numeric(12, 2) NOT NULL,
    PRIMARY KEY (order_date, city)
);
