import sys
import argparse

from pyspark.sql import SparkSession

def main(source_path, target_s3_path):
    spark = SparkSession.builder \
        .appName("ExcelToParquetTransfer") \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7") \
        .config("spark.hadoop.fs.s3a.endpoint", "minio-external.default.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "adminic") \
        .config("spark.hadoop.fs.s3a.secret.key", "adminic123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


    df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(f"s3a://ahajiyev/{source_path}")

    print(f"Read data from s3a://ahajiyev/{source_path}:")
    df.show(5)

    df.write.mode("overwrite").parquet(f"s3a://ahajiyev/{target_s3_path}")

    print(f"Successfully written to s3a://ahajiyev/{target_s3_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer Excel to S3 as Parquet")
    parser.add_argument("--source", required=True, help="MinIO S3 path to Excel file (e.g. bucket/file.xlsx)")
    parser.add_argument("--target", required=True, help="MinIO S3 output path (e.g. bucket/folder/table)")

    args = parser.parse_args()
    main(args.source, args.target)
