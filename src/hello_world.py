import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Initialize contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Print Hello World to the logs
print("Hello World from AWS Glue!")

# Create a small test dataframe
# data = [("Hello", "World"), ("PySpark", "AWS Glue")]
# df = spark.createDataFrame(data, ["Col1", "Col2"])
# # Show the data in the logs
# df.show()

# First way to read local file
# df = spark.read.option("header", "true").csv("file:///home/hadoop/workspace/data/sales.csv")

# Second way to read local file
customers_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["file:///home/hadoop/workspace/data/customers.csv"]},
    format_options={
        "withHeader": True,
        # "optimizePerformance": True,
    },

)
#customers_dyf.show()
customers = customers_dyf.toDF()
customers.show()

customers.printSchema()

customers.select("customer_name").show()
#print(f"customer_name = {customers['customer_name']}")

customers.select(customers["customer_name"], customers["customer_id"] + 1).show()

customers.filter(customers["customer_id"] >= 50).show()

customers.groupBy("city").count().show()


orders = spark.read.option("header", "true").csv("file:///home/hadoop/workspace/data/sales.csv")
orders.show()

print("Job finished successfully.")
