CREATE DATABASE IF NOT EXISTS sales_star;
\c sales_star;

-- Таблица: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    age INT,
    email VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    pet_type VARCHAR,
    pet_name VARCHAR,
    pet_breed VARCHAR
);

-- Таблица: Sellers
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    country VARCHAR,
    postal_code VARCHAR
);

-- Таблица: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    category VARCHAR,
    weight FLOAT,
    color VARCHAR,
    size VARCHAR,
    brand VARCHAR,
    material VARCHAR,
    description TEXT,
    rating FLOAT,
    reviews INT,
    release_date DATE,
    expiry_date DATE
);

-- Таблица: Stores
CREATE TABLE IF NOT EXISTS dim_stores (
    store_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    phone VARCHAR,
    email VARCHAR
);

-- Таблица: Suppliers
CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    contact VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address TEXT,
    city VARCHAR,
    country VARCHAR
);

-- Таблица: Время
CREATE TABLE IF NOT EXISTS dim_date (
    date DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT
);

-- Факт таблица продаж
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE REFERENCES dim_date(date),
    customer_id VARCHAR REFERENCES dim_customers(customer_id),
    seller_id VARCHAR REFERENCES dim_sellers(seller_id),
    product_id VARCHAR REFERENCES dim_products(product_id),
    store_id VARCHAR REFERENCES dim_stores(store_id),
    supplier_id VARCHAR REFERENCES dim_suppliers(supplier_id),
    quantity INT,
    total_price FLOAT
);