from pyspark.sql import SparkSession

# Create or get a Spark session
spark = SparkSession.builder.getOrCreate()

# Create a temporary view for device data from JSON files
spark.sql("""
    CREATE TEMPORARY VIEW vw_device
    USING org.apache.spark.sql.json
    OPTIONS (path "C:/Users/DNC-PC-264/Documents/GitHub/sparkProject/docs/files/device/*.json")       
""")

# Create a temporary view for subscription data from JSON files
spark.sql("""
    CREATE TEMPORARY VIEW vw_subscription
    USING org.apache.spark.sql.json
    OPTIONS (path "C:/Users/DNC-PC-264/Documents/GitHub/sparkProject/docs/files/subscription/*.json")
""")

# Print the list of tables in the Spark catalog to verify the views were created
print(spark.catalog.listTables())

# Select and show the first 10 rows from the device view
spark.sql("""SELECT * FROM vw_device LIMIT 10;""").show()

# Select and show the first 10 rows from the subscription view
spark.sql("""SELECT * FROM vw_subscription LIMIT 10;""").show()

# Create a new DataFrame by joining the device and subscription datasets on user_id
join_datasets = spark.sql("""
    SELECT dev.user_id,
           dev.model,
           dev.platform,
           subs.payment_method,
           subs.plan
    FROM vw_device AS dev
    INNER JOIN vw_subscription AS subs
    ON dev.user_id = subs.user_id
""")

# Show the resulting joined DataFrame
join_datasets.show()

# Print the schema of the joined DataFrame
join_datasets.printSchema()

# Print the number of rows in the joined DataFrame
print(join_datasets.count())
