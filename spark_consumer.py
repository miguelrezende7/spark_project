# import findspark
# findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro, to_avro
import avro.schema

# from kafka import KafkaConsumer
# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'
# findspark.add_packages(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"])


# \i /home/miguel/impacta-spark/project/scripts/createtable_transactions.sql   <<<Criar tablea 
# sudo service postgresql start
# sudo -u postgres psql 
# \c sparkproject

# spark-submit --packages org.apache.spark:spark-avro_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --jars /home/miguel/download/postgresql-42.4.0.jar spark_consumer.py

if __name__ == "__main__":


    def get_from_db():
        df_db=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/sparkproject").option("dbtable","accounts").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").load()
        return df_db
    
    def save_to_db(df):
        df.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/sparkproject").option("dbtable","transactions").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").mode('append').save()
        pass
        
    def etl(df):
        df = df.drop("clientcode")
        # conditions = when(col("operation_value")>5000.00 & col("accounttype") =="Silver",True).otherwise(False)
        conditions = when( (col("operation_value") >5000.00) & (col("accounttype") =="Silver"),True).otherwise(False)       
        df=df.withColumn("risk_operation",conditions)   
        df = df.withColumnRenamed("date", "transaction_date")         
        return df
    
    def enrich(df_stream,batchId):
        print('\n\n\n\n\n\n')
     
        # Verify if stream has data
        if df_stream.count()>0:            
            df_db=get_from_db()            
            df_rich=df_stream.join(df_db,df_stream.client_code==df_db.clientcode,"inner")        
            df_rich=etl(df_rich)
            print('\n\n\n\n\n\n')
            df_rich.show()
            print('\n\n\n\n\n\n')
            save_to_db(df_rich)
            

        

       


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
        

    print('\n\n\n') 
       
    query = output \
        .writeStream \
        .outputMode("append") \
        .trigger(processingTime="5 second") \
        .foreachBatch(enrich) \
        .option("checkpointLocation","checkpoints")\
        .start()

    query.awaitTermination()
        
       
    
    print('\n\n\n')

    

   