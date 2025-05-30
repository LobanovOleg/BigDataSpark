CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- Витрина: Топ-10 самых продаваемых продуктов
CREATE TABLE IF NOT EXISTS top_10_products (
    product_id UInt64,
    product_name String,
    total_quantity UInt64,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY total_quantity DESC;

-- Витрина: Общая выручка по категориям продуктов
CREATE TABLE IF NOT EXISTS revenue_by_category (
    product_category String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY total_revenue DESC;

-- Витрина: Средний рейтинг и количество отзывов для каждого продукта
CREATE TABLE IF NOT EXISTS rating_reviews_by_product (
    product_id UInt64,
    product_name String,
    average_rating Float32,
    review_count UInt32
) ENGINE = MergeTree()
ORDER BY average_rating DESC;

-- Витрина: Топ-10 клиентов по покупкам
CREATE TABLE IF NOT EXISTS top_10_customers (
    customer_id String,
    customer_name String,
    total_spent Float64
) ENGINE = MergeTree()
ORDER BY total_spent DESC;

-- Витрина: Распределение клиентов по странам
CREATE TABLE IF NOT EXISTS customers_by_country (
    customer_country String,
    customer_count UInt32
) ENGINE = MergeTree()
ORDER BY customer_count DESC;

-- Витрина: Средний чек для каждого клиента
CREATE TABLE IF NOT EXISTS avg_receipt_per_customer (
    customer_id String,
    customer_name String,
    avg_receipt Float64
) ENGINE = MergeTree()
ORDER BY avg_receipt DESC;

-- Витрина: Месячные и годовые тренды продаж
CREATE TABLE IF NOT EXISTS monthly_sales_trends (
    year UInt16,
    month UInt8,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY (year, month);

-- Витрина: Средний размер заказа по месяцам
CREATE TABLE IF NOT EXISTS avg_order_by_month (
    year UInt16,
    month UInt8,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (year, month);

-- Витрина: Топ-5 магазинов с наибольшей выручкой
CREATE TABLE IF NOT EXISTS top_5_stores (
    store_id String,
    store_name String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY total_revenue DESC;

-- Витрина: Продажи по городам и странам
CREATE TABLE IF NOT EXISTS sales_by_geo (
    store_country String,
    store_city String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY total_revenue DESC;

-- Витрина: Средний чек по магазинам
CREATE TABLE IF NOT EXISTS avg_receipt_per_store (
    store_id String,
    store_name String,
    avg_receipt Float64
) ENGINE = MergeTree()
ORDER BY avg_receipt DESC;

-- Витрина: Топ-5 поставщиков по выручке
CREATE TABLE IF NOT EXISTS top_5_suppliers (
    supplier_id String,
    supplier_name String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY total_revenue DESC;

-- Витрина: Средняя цена товаров от поставщика
CREATE TABLE IF NOT EXISTS avg_price_by_supplier (
    supplier_id String,
    supplier_name String,
    avg_price Float64
) ENGINE = MergeTree()
ORDER BY avg_price DESC;

-- Витрина: Продажи по странам поставщиков
CREATE TABLE IF NOT EXISTS sales_by_supplier_country (
    supplier_country String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY total_revenue DESC;

-- Витрина: Продукты с наивысшим и наименьшим рейтингом
CREATE TABLE IF NOT EXISTS best_rated_products (
    product_id UInt64,
    product_name String,
    rating Float32
) ENGINE = MergeTree()
ORDER BY rating DESC;

CREATE TABLE IF NOT EXISTS worst_rated_products (
    product_id UInt64,
    product_name String,
    rating Float32
) ENGINE = MergeTree()
ORDER BY rating ASC;

-- Витрина: Корреляция между рейтингом и продажами
CREATE TABLE IF NOT EXISTS rating_sales_correlation (
    product_id UInt64,
    product_name String,
    rating Float32,
    total_sales UInt64
) ENGINE = MergeTree()
ORDER BY rating DESC;

-- Витрина: Продукты с наибольшим количеством отзывов
CREATE TABLE IF NOT EXISTS most_reviewed_products (
    product_id UInt64,
    product_name String,
    review_count UInt32
) ENGINE = MergeTree()
ORDER BY review_count DESC;

CREATE TABLE yearly_sales_trends (
    year UInt16,
    total_revenue Float64
) ENGINE = MergeTree ORDER BY year;