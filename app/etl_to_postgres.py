from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_date
import os

spark = SparkSession.builder \
    .appName("ETL to PostgreSQL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql_2.12:3.4.1") \
    .getOrCreate()

data_path = "/app/data/"

csv_files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith(".csv")]

print("CSV-файлы в директории:")
for file in csv_files:
    print(f"- {file} (размер: {os.path.getsize(file)} байт")

df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(csv_files)

dim_customers = df.select(
    col("sale_customer_id").alias("customer_id"),
    "customer_first_name", "customer_last_name",
    "customer_age", "customer_email", "customer_country",
    "customer_postal_code", "customer_pet_type",
    "customer_pet_name", "customer_pet_breed"
).dropDuplicates()

dim_sellers = df.select(
    col("sale_seller_id").alias("seller_id"),
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code"
).dropDuplicates()

dim_products = df.select(
    col("sale_product_id").alias("product_id"),
    "product_name", "product_category", "product_price", "product_weight",
    "product_color", "product_size", "product_brand", "product_material",
    "product_description", "product_rating", "product_reviews",
    "product_release_date", "product_expiry_date"
).dropDuplicates()

dim_stores = df.select(
    "store_name", "store_location", "store_city",
    "store_state", "store_country", "store_phone", "store_email"
).dropDuplicates()

dim_suppliers = df.select(
    "supplier_name", "supplier_contact", "supplier_email",
    "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
).dropDuplicates()

dim_time = df.select("sale_date").dropDuplicates() \
    .withColumn("day", dayofmonth("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .withColumn("year", year("sale_date"))

fact_sales = df.select(
    col("sale_customer_id").alias("customer_id"),
    col("sale_seller_id").alias("seller_id"),
    col("sale_product_id").alias("product_id"),
    col("sale_date"),
    "sale_quantity", "sale_total_price"
)

pg_url = "jdbc:postgresql://postgres:5432/lab2"
pg_properties = {
    "user": "lowban",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

dim_customers.write \
    .option("numPartitions", "4") \
    .option("batchsize", "1000") \
    .jdbc(pg_url, "dim_customers", "overwrite", pg_properties)
dim_sellers.write.jdbc(pg_url, "dim_sellers", "overwrite", pg_properties)
dim_products.write.jdbc(pg_url, "dim_products", "overwrite", pg_properties)
dim_stores.write.jdbc(pg_url, "dim_stores", "overwrite", pg_properties)
dim_suppliers.write.jdbc(pg_url, "dim_suppliers", "overwrite", pg_properties)
dim_time.write.jdbc(pg_url, "dim_time", "overwrite", pg_properties)
fact_sales.write.jdbc(pg_url, "fact_sales", "overwrite", pg_properties)

print("✅ Данные успешно загружены в PostgreSQL")

spark.stop()