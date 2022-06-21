# import findspark
# findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro, to_avro
import avro.schema


# from kafka import KafkaConsumer
# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'


# spark-submit --packages org.apache.spark:spark-avro_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_consumer.py





if __name__ == "__main__":

    # findspark.add_packages(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"])

    spark = SparkSession.builder.appName("Consume Kafka").getOrCreate()
    jsonFormatSchema = open("schema_transaction.avsc", "r").read()

    TOPICO = "Bank_Transaction"
    KAFKA_SERVER = "localhost:9092"
    SCHEMA_PATH = "schema_transaction.avsc"
    

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", TOPICO)
        .option("startingOffsets", "latest")
        .load()
    )

    output = df\
        .select(from_avro(col("value"), jsonFormatSchema).alias("user"))\
        .select("user.*")
        
 
    # output = df\
    #     .select(from_avro("value", jsonFormatSchema).alias("user"))\
    #     .select(to_avro("user.agency").alias("value"))

    query = output \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime="5 second") \
        .option("checkpointLocation","checkpoints")\
        .start()

    query.awaitTermination()
    


    # rawQuery = output.writeStream.queryName("qraw").format("memory").start().processAllAvailable()
    # raw = spark.sql("select * from qraw")

    # operacaoDF = kafka_df.select(from_avro(col("value"), jsonFormatSchema).alias("operacao")).select("operacao.*")



    print('\n\n\n') 
    # print(df.value)
    print(df.columns)


    print(type(df))
    print(type(output))
    print(type(query))
    print('\n\n\n')

    # operacaoDF = df.select(from_avro(col("value"), SCHEMA_PATH).alias("operacao")).select("operacao.*")

  

   