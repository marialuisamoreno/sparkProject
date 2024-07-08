# Import libraries and initialize a Spark session
from pyspark.sql import SparkSession

# Create or get a Spark session
spark = SparkSession.builder.getOrCreate()
print(spark)

# Load data from a JSON file into a DataFrame
df_device = spark.read.json("C:/Users/DNC-PC-264/Documents/GitHub/sparkProject/docs/files/device/*.json")

# Display the DataFrame content
df_device.show()

# Print the schema of the DataFrame
df_device.printSchema()

# Print the columns of the DataFrame
print(df_device.columns)

# Print the number of rows in the DataFrame
print(df_device.count())

# Select specific columns from the DataFrame and display them
df_device.select("manufacturer", "model", "platform").show()

# Select specific columns and rename a column using an expression
df_device.selectExpr("manufacturer", "model", "platform as type").show()

# Filter the DataFrame where the manufacturer is "Xiamomi" and display the results
df_device.filter(df_device.manufacturer == "Xiamomi").show()

# Group the DataFrame by the "manufacturer" column and count the occurrences
df_device.groupBy("manufacturer").count().show()

# Create a new DataFrame with the grouped data and display it
df_grouped_manufacturer = df_device.groupBy("manufacturer").count()
df_grouped_manufacturer.show()
