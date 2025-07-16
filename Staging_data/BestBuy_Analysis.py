# Databricks notebook source
products_csv_path = "/FileStore/tables/bestbuy_products_raw.csv"
reviews_csv_path = "/FileStore/tables/bestbuy_reviews_raw.csv"

# COMMAND ----------

products_df = spark.read.csv(products_csv_path, header=True, inferSchema= True)

# COMMAND ----------

print("\n--- Loading Reviews DataFrame with multiLine and quote options ---")
reviews_df = spark.read.csv(
    reviews_csv_path,
    header=True,
    inferSchema=True,
    multiLine=True, # Crucial for comments spanning multiple lines
    quote='"',      # Essential for fields enclosed in double quotes
    escape='"'      # Often needed if quotes appear within the quoted field itself
)
print("Reviews DataFrame loaded successfully with advanced options.")

# COMMAND ----------

display(products_df.head(7))
display(reviews_df.head(7))

# COMMAND ----------

# MAGIC %md
# MAGIC # Inspection Of Data 
# MAGIC 1. Checking for Nulls

# COMMAND ----------

from pyspark.sql.functions import col, sum
print("\n  Checking for the Products DataFrame Null Counts ")
products_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in products_df.columns]).show()

# COMMAND ----------

print("\n Checking for the Reviews DataFrame Null Counts ")
reviews_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in reviews_df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary statistics for numerical columns (like review_rating, regular_price)

# COMMAND ----------

print("\n--- Reviews DataFrame Description (summary statistics) ---")
reviews_df.describe("review_rating").show() 
products_df.describe("customer_review_average", "customer_review_count", "regular_price").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Count unique categories

# COMMAND ----------

print("\n--- Unique Product Categories and their Counts ---")
products_df.groupBy("category_path").count().orderBy(col("count").desc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Count unique review ratings

# COMMAND ----------

print("\n--- Unique Review Ratings and their Counts ---")
reviews_df.groupBy("review_rating").count().orderBy(col("count").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Understanding the Relationship (Merging/Joining in PySpark)

# COMMAND ----------

merged_df = reviews_df.join(
    products_df,
    col("product_sku") == col("sku"), # Join on your new consistent type columns
    "left"
)

# COMMAND ----------

display(merged_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning and Trasnfomations 

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Drop the 'sku' column from the products side as 'product_sku' is already present

# COMMAND ----------

print("\n Dropped Redundant 'sku' column from merged_df ")
merged_df = merged_df.drop("sku")

# COMMAND ----------

display(merged_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Check for Duplicate Rows 

# COMMAND ----------

print("\n--- Checking for Duplicates in the FULL Merged DataFrame ---")
initial_merged_count = merged_df.count()
distinct_merged_df = merged_df.dropDuplicates(['review_id']) # Assuming 'review_id' is your unique identifier
distinct_merged_count = distinct_merged_df.count()

print(f"Initial merged row count: {initial_merged_count}")
print(f"Distinct merged row count (by review_id): {distinct_merged_count}")
print(f"Number of duplicate merged rows found and removed: {initial_merged_count - distinct_merged_count}")

# Update your working DataFrame to include only distinct rows
merged_df = distinct_merged_df

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Handle Missing Values

# COMMAND ----------

from pyspark.sql.functions import lit

print("\n--- Handling Missing Values ---")

# For text fields: fill nulls with empty string or 'No Value'
merged_df = merged_df.na.fill(value="", subset=["review_title", "review_comment"])
merged_df = merged_df.na.fill(value="anonymous", subset=["reviewer_name"]) # If not handled by extraction script

# For numerical fields: fill nulls with 0 or a reasonable default
# You might want to calculate mean/median for more sophisticated imputation
merged_df = merged_df.na.fill(value=0.0, subset=["customer_review_average", "regular_price"])
merged_df = merged_df.na.fill(value=0, subset=["customer_review_count"])

# Re-check null counts after filling
from pyspark.sql.functions import sum, col
print("\n--- Null Counts After Filling ---")
display(merged_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in merged_df.columns]))


# COMMAND ----------

# MAGIC %md
# MAGIC 4. Correct Data Types

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

print("\n--- Correcting Data Types ---")

# Convert 'submission_time' to TimestampType
merged_df = merged_df.withColumn("submission_time", to_timestamp(col("submission_time")))

# Cast review_rating to IntegerType (assuming ratings are whole numbers)
merged_df = merged_df.withColumn("review_rating", col("review_rating").cast(IntegerType()))

# Re-verify other numerical columns, cast if needed (Spark's inferSchema is usually good for these)
merged_df = merged_df.withColumn("customer_review_average", col("customer_review_average").cast(DoubleType()))
merged_df = merged_df.withColumn("regular_price", col("regular_price").cast(DoubleType()))
merged_df = merged_df.withColumn("customer_review_count", col("customer_review_count").cast(IntegerType()))

print("\n--- Schema after Data Type Correction ---")
merged_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Standardize and Normalize Categorical/Text Data

# COMMAND ----------

from pyspark.sql.functions import lower, trim, split, element_at, when

print("\n--- Standardizing Text and Categories ---")

# Clean text fields: lowercase and trim whitespace
merged_df = merged_df.withColumn("name", lower(trim(col("name"))))
merged_df = merged_df.withColumn("review_title", lower(trim(col("review_title"))))
merged_df = merged_df.withColumn("review_comment", lower(trim(col("review_comment"))))
merged_df = merged_df.withColumn("reviewer_name", lower(trim(col("reviewer_name"))))

# Re-confirm 'anonymous' reviewer name handling (if needed after initial load fix)
merged_df = merged_df.withColumn(
    "reviewer_name",
    when(col("reviewer_name").isin("n/a", "null", ""), "anonymous").otherwise(col("reviewer_name"))
)

# Extract the main category from 'category_path'
# Based on your previous sample, this might extract "Laptops" or "Televisions" etc.
merged_df = merged_df.withColumn(
    "main_category",
    element_at(split(col("category_path"), ", "), 3) # Assumes main category is the 3rd element
)
# Important: Inspect 'main_category' after this step to ensure it's correct for your data!
merged_df.select("category_path", "main_category").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Derive New Useful Columns

# COMMAND ----------

from pyspark.sql.functions import length, year, month, dayofweek, dayofmonth

print("\n--- Feature Engineering ---")

# Length of the review comment
merged_df = merged_df.withColumn("review_length", length(col("review_comment")))

# Extract date components from submission_time
merged_df = merged_df.withColumn("review_year", year(col("submission_time")))
merged_df = merged_df.withColumn("review_month", month(col("submission_time")))
merged_df = merged_df.withColumn("review_day_of_week", dayofweek(col("submission_time"))) # 1=Sunday, 7=Saturday
merged_df = merged_df.withColumn("review_day_of_month", dayofmonth(col("submission_time")))

display(merged_df.select("review_comment", "review_length", "submission_time", "review_year", "review_month", "review_day_of_week").head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC 7. arranging columns 

# COMMAND ----------

from pyspark.sql.functions import col
print("\n--- Reordering Columns in merged_df ---")
desired_column_order = [
    "product_sku",
    "name",
    "main_category",
    "category_path",   
    "regular_price",
    "review_id",
    "reviewer_name",
    "review_title",
    "review_comment",
    "review_rating",
    "review_length",
    "customer_review_average",
    "customer_review_count",       
    "submission_time",
    "review_year",        
    "review_month",        
    "review_day_of_week",  
    "review_day_of_month", 
    "url"                 
]
merged_df = merged_df.select(desired_column_order)


# COMMAND ----------

display(merged_df.head(5))

# COMMAND ----------

print("\n--- Saving Cleaned and Structured Data ---")

# Define the output path for the Parquet file
output_parquet_path = "dbfs:/tables/bestbuy_reviews_cleaned.parquet"

# Define the output path for the CSV file (it will be a directory)
output_csv_path = "dbfs:/tables/bestbuy_reviews_cleaned_csv"

# --- Save as Parquet ---
print(f"Saving data to Parquet at: {output_parquet_path}")
merged_df.write.mode("overwrite").parquet(output_parquet_path)
print(f"Cleaned and structured data successfully saved to Parquet.")

# --- Save as CSV ---
print(f"Saving data to CSV at: {output_csv_path}")
merged_df.write.mode("overwrite").csv(output_csv_path, header=True) # header=True includes column names
print(f"Cleaned and structured data successfully saved to CSV.")