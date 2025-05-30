from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, corr

spark = SparkSession.builder \
    .appName("ETL to ClickHouse Reports") \
    .config("spark.jars", "/opt/spark/jars/*") \
    .getOrCreate()

postgres_url = "jdbc:postgresql://postgres:5432/lab2"
postgres_props = {
    "user": "lowban",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/analytics"
clickhouse_props = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

fact_sales = spark.read.jdbc(postgres_url, "fact_sales", properties=postgres_props)
dim_products = spark.read.jdbc(postgres_url, "dim_products", properties=postgres_props)
dim_customers = spark.read.jdbc(postgres_url, "dim_customers", properties=postgres_props)
dim_time = spark.read.jdbc(postgres_url, "dim_date", properties=postgres_props)
dim_stores = spark.read.jdbc(postgres_url, "dim_stores", properties=postgres_props)
dim_suppliers = spark.read.jdbc(postgres_url, "dim_suppliers", properties=postgres_props)

sales_with_products = fact_sales.join(dim_products, "product_id")
sales_with_customers = fact_sales.join(dim_customers, "customer_id")
sales_with_time = fact_sales.join(dim_time, fact_sales.sale_date == dim_time.date)
sales_with_stores = fact_sales.join(dim_stores, "store_id")
sales_with_suppliers = fact_sales.join(dim_suppliers, "supplier_id")

# --- 1. Витрина продаж по продуктам ---
# Топ-10 продаваемых продуктов
top_10_products = sales_with_products.groupBy("product_id", "product_name") \
    .agg(
        sum("quantity").alias("total_quantity"),
        sum("total_price").alias("total_revenue")
    ) \
    .orderBy(col("total_quantity").desc()) \
    .limit(10)

# Общая выручка по категориям
revenue_by_category = sales_with_products.groupBy("product_category") \
    .agg(sum("total_price").alias("total_revenue"))

# Средний рейтинг и количество отзывов
rating_reviews = dim_products.groupBy("product_id", "product_name") \
    .agg(
        avg("rating").alias("average_rating"),
        count("reviews").alias("review_count")
    )

# --- 2. Витрина продаж по клиентам ---
# Топ-10 клиентов по сумме покупок
top_10_customers = sales_with_customers.groupBy("customer_id", "first_name", "last_name") \
    .agg(sum("total_price").alias("total_spent")) \
    .orderBy(col("total_spent").desc()) \
    .limit(10) \
    .withColumn("customer_name", col("first_name") + " " + col("last_name"))

# Распределение клиентов по странам
customers_by_country = dim_customers.groupBy("country") \
    .agg(count("customer_id").alias("customer_count"))

# Средний чек для каждого клиента
avg_receipt_per_customer = sales_with_customers.groupBy("customer_id") \
    .agg((sum("total_price")/sum("quantity")).alias("avg_receipt"))

# --- 3. Витрина продаж по времени ---
monthly_trends = sales_with_time.groupBy("year", "month") \
    .agg(sum("total_price").alias("total_revenue"))

yearly_trends = sales_with_time.groupBy("year") \
    .agg(sum("total_price").alias("total_revenue"))

avg_order_by_month = sales_with_time.groupBy("year", "month") \
    .agg((sum("total_price")/count("* ")).alias("avg_order_value"))

# --- 4. Витрина продаж по магазинам ---
# Топ-5 магазинов
top_5_stores = sales_with_stores.groupBy("store_id", "store_name") \
    .agg(sum("total_price").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5)

# Продажи по городам и странам
sales_by_geo = sales_with_stores.groupBy("store_country", "store_city") \
    .agg(sum("total_price").alias("total_revenue"))

# Средний чек по магазинам
avg_receipt_per_store = sales_with_stores.groupBy("store_id") \
    .agg((sum("total_price")/sum("quantity")).alias("avg_receipt"))

# --- 5. Витрина продаж по поставщикам ---
# Топ-5 поставщиков
top_5_suppliers = sales_with_suppliers.groupBy("supplier_id", "supplier_name") \
    .agg(sum("total_price").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5)

# Средняя цена товаров от поставщика
avg_price_by_supplier = sales_with_suppliers.groupBy("supplier_id", "supplier_name") \
    .agg(avg("product_price").alias("avg_price"))

# Продажи по странам поставщиков
sales_by_supplier_country = sales_with_suppliers.groupBy("supplier_country") \
    .agg(sum("total_price").alias("total_revenue"))

# --- 6. Витрина качества продукции ---
# Продукты с наивысшим/наименьшим рейтингом
best_rated = dim_products.orderBy(col("rating").desc()).limit(10)
worst_rated = dim_products.orderBy(col("rating").asc()).limit(10)

# Корреляция "рейтинг" vs "объём продаж"
rating_sales = sales_with_products.groupBy("product_id", "product_name") \
    .agg(
        avg("rating").alias("avg_rating"),
        sum("quantity").alias("total_sales"),
        corr("rating", "quantity").alias("correlation")
    )

# Продукты с наибольшим количеством отзывов
most_reviewed = dim_products.orderBy(col("reviews").desc()).limit(10)

def save(df, table):
    df.write \
      .format("jdbc") \
      .option("url", clickhouse_url) \
      .option("dbtable", table) \
      .option("driver", clickhouse_props["driver"]) \
      .mode("append") \
      .save()

save(top_10_products, "top_10_products")
save(revenue_by_category, "revenue_by_category")
save(rating_reviews, "rating_reviews_by_product")

save(top_10_customers, "top_10_customers")
save(customers_by_country, "customers_by_country")
save(avg_receipt_per_customer, "avg_receipt_per_customer")

save(monthly_trends, "monthly_sales_trends")
save(yearly_trends, "yearly_sales_trends")  # переиспользуем таблицу для годовых
save(avg_order_by_month, "avg_order_by_month")

save(top_5_stores, "top_5_stores")
save(sales_by_geo, "sales_by_geo")
save(avg_receipt_per_store, "avg_receipt_per_store")

save(top_5_suppliers, "top_5_suppliers")
save(avg_price_by_supplier, "avg_price_by_supplier")
save(sales_by_supplier_country, "sales_by_supplier_country")

save(best_rated, "extreme_rated_products")
save(worst_rated, "extreme_rated_products")  # обе группы в одну таблицу
save(rating_sales, "rating_sales_correlation")
save(most_reviewed, "most_reviewed_products")

print("✅ Все витрины записаны в ClickHouse")

spark.stop()