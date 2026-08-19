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
data = [("Hello", "World"), ("PySpark", "AWS Glue")]
df = spark.createDataFrame(data, ["Col1", "Col2"])

# Show the data in the logs
df.show()

print("Job finished successfully.")