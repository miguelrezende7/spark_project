# from kafka import KafkaProducer

# producer = KafkaProducer()

# producer.send('mensagens',key=b"Chave %d" % 1,value=b'teste99')
# producer.flush()



from kafka import KafkaProducer
import io
from avro.schema import Parse
from avro.io import DatumWriter, DatumReader, BinaryEncoder, BinaryDecoder

# Create a Kafka client ready to produce messages
bootstrap_servers = ['localhost:9092']
topicName = 'Bank_Transaction'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Get the schema to use to serialize the message
schema = Parse(open('schema_transaction.avsc', "rb").read())

# serialize the message data using the schema
buf = io.BytesIO()
encoder = BinaryEncoder(buf)
writer = DatumWriter(writer_schema=schema)
# writer.write({"name": "Ben", "favorite_number": 7, "favorite_color": "red"}, encoder)
writer.write({"client_code": "001", "agency": "001", "operation_value": 5.00 , "operation_type":"Deposity","date":"04/06/2022","account_balance":505.00}, encoder)
# writer.write({"name": "Alyssa", "favorite_number": 256}, encoder)
buf.seek(0)
message_data = (buf.read())


# message key if needed
key = None

# headers if needed
headers = []

# Send the serialized message to the Kafka topic
producer.send(topicName,
              message_data,
              key,
              headers)
producer.flush()



#iniciar zookeeper
# ./zookeeper-server-start.sh ../config/zookeeper.properties

#iniciar broker
# ./kafka-server-start.sh ../config/server.properties