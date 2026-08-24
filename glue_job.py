"""
Glue ETL job: read two datasets from S3, clean them, load into Redshift.

Job parameters (--key value on the job definition):
    --JOB_NAME          set automatically by Glue
    --TempDir           s3://bucket/glue-temp/   Glue stages COPY files here
    --orders_path       s3://my-lake/raw/orders/
    --customers_path    s3://my-lake/raw/customers/
    --connection_name   name of the Glue Connection pointing at Redshift
    --redshift_schema   e.g. public
"""

import sys
import uuid

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "TempDir", "orders_path", "customers_path",
     "connection_name", "redshift_schema"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SCHEMA = args["redshift_schema"]

# Staging table names must be unique per run, or two concurrent runs will
# stomp on each other. getResolvedOptions only returns keys you asked for,
# so don't rely on JOB_RUN_ID being present -- just generate one.
RUN_ID = uuid.uuid4().hex[:8]


# ---------------------------------------------------------------- read

def read_s3(path: str, ctx: str):
    """
    transformation_ctx is what enables job bookmarks -- on the next run Glue
    skips files it already processed. Omit it and you reload everything.
    """
    return glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path], "recurse": True},
        format="parquet",
        transformation_ctx=ctx,
    )


orders_dyf = read_s3(args["orders_path"], "orders_src")
customers_dyf = read_s3(args["customers_path"], "customers_src")

# DynamicFrames handle messy schemas on read; convert to DataFrame for the
# actual work, because the DataFrame API is far richer.
orders = orders_dyf.toDF()
customers = customers_dyf.toDF()


# ---------------------------------------------------------------- transform

def clean_customers(df):
    return (df
            .dropDuplicates(["customer_id"])
            .filter(F.col("customer_id").isNotNull())
            .withColumn("country", F.upper(F.trim(F.col("country")))))


def clean_orders(df, customers_df):
    """Drop orders whose customer no longer exists, and tag each with its tier."""
    valid = customers_df.select("customer_id", "tier")
    return (df
            .filter(F.col("customer_id").isNotNull())
            .filter(F.col("amount") > 0)
            .join(F.broadcast(valid), "customer_id", "inner")
            .withColumn("amount", F.round(F.col("amount"), 2)))


customers_clean = clean_customers(customers)
orders_clean = clean_orders(orders, customers_clean)


# ---------------------------------------------------------------- write

def upsert_to_redshift(df, table: str, key: str, ctx: str) -> None:
    """
    Glue does not write row by row. It dumps the frame to TempDir as files and
    issues a COPY -- same mechanism as the Lambda version, just wrapped.

    preactions run before the COPY, postactions after, in the same session.
    That is where the upsert lives.
    """
    target = f"{SCHEMA}.{table}"
    staging = f"{SCHEMA}.{table}_stg_{RUN_ID}"

    # CREATE TABLE LIKE copies the distkey, sortkey and encodings, so the
    # staging table lands co-located with the target and the join below
    # does not redistribute data across nodes.
    preactions = (
        f"DROP TABLE IF EXISTS {staging};"
        f"CREATE TABLE {staging} (LIKE {target});"
    )

    postactions = (
        f"BEGIN;"
        f"DELETE FROM {target} USING {staging} s WHERE {target}.{key} = s.{key};"
        f"INSERT INTO {target} SELECT * FROM {staging};"
        f"DROP TABLE {staging};"
        f"END;"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(df, glueContext, table),
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": args["TempDir"],
            "useConnectionProperties": "true",
            "connectionName": args["connection_name"],
            "dbtable": staging,
            "preactions": preactions,
            "postactions": postactions,
        },
        transformation_ctx=ctx,
    )


upsert_to_redshift(customers_clean, "customers", "customer_id", "customers_sink")
upsert_to_redshift(orders_clean, "orders", "order_id", "orders_sink")

job.commit()
