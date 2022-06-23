from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro, to_avro
import avro.schema


if __name__ == "__main__":

    # spark-submit --jars /home/miguel/download/postgresql-42.4.0.jar postgres_test.py
    # sudo service postgresql start

    spark = SparkSession.builder.appName("Consume Kafka").getOrCreate()

    resumo=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/sparkproject").option("dbtable","accounts").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").load()

    print('\n\n\n') 
    resumo.show()
    print('\n\n\n') 
