import findspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
findspark.init()
from pyspark import SparkContext, SparkConf
from kafka import KafkaConsumer
from pyspark.streaming import StreamingContext
from pyspark.sql.avro.functions import from_avro
import avro.schema
import time
from avro.io import DatumWriter
from kafka import KafkaClient, KafkaProducer
from datetime import datetime

#from pyspark.streaming.kafka  KafkaUtils


from json import loads


findspark.add_packages(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1,org.apache.spark:spark-avro_2.12:3.3.0"])

TOPICO = "Bank_Transaction"
KAFKA_SERVER = "localhost:9092"



spark = SparkSession.builder.appName("test").getOrCreate()
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVER).option("startingOffsets", "earliest").option("subscribe", TOPICO).load()