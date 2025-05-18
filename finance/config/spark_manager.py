from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

spark_session = SparkSession.builder \
    .master("local[*]") \
    .appName("finance_complaint") \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.executor.memoryOverhead", "12g") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .getOrCreate()

hadoop_conf = spark_session._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", access_key_id)
hadoop_conf.set("fs.s3a.secret.key", secret_access_key)
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
