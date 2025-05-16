from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("tmdb-silver") \
    .master("local[2]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.0") \
    .config("fs.s3a.access.key", "dataops") \
    .config("fs.s3a.secret.key", "Ankara06") \
    .config("fs.s3a.path.style.access", True) \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()


def load_delta(df, delta_name):
    df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"s3a://tmdb-silver/{delta_name}")
    

df_credits = spark.read.format("parquet").load("s3a://tmdb-bronze/credits/")


schema_cast = (
    ArrayType(
        StructType([
            StructField("cast_id", IntegerType()),
            StructField("character", StringType()),
            StructField("credit_id", StringType()),
            StructField("gender", IntegerType()),
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("order", IntegerType())
                  ])
            )
         )

schema_crew = (ArrayType(
        StructType([
            StructField("credit_id", StringType()),
            StructField("department", StringType()),
            StructField("gender", IntegerType()),
            StructField("id", IntegerType()),
            StructField("job", StringType()),
            StructField("name", StringType())
                  ])
            )
         )
# Cast
df_credits_nested = df_credits.withColumn("cast", F.from_json(df_credits.cast, schema_cast)).withColumn("crew", F.from_json(df_credits.crew, schema_crew))
cast_df = df_credits_nested.select("movie_id", "title", F.explode_outer("cast")).select("movie_id","title", "col.*").drop("order")
cast_df = cast_df.na.fill("0000000000",["credit_id"])


# Crew
crew_df = df_credits_nested.select("movie_id", "title", F.explode_outer("crew")).select("movie_id", "title", "col.*")
crew_df = crew_df.na.fill("0000000000",["credit_id"])


# load to delta
load_delta(cast_df, 'cast')

load_delta(crew_df, 'crew')


