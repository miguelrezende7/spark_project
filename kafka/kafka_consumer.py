# from kafka import KafkaConsumer as kc
# consumidor = kc("mensagens", bootstrap_servers="127.0.0.1:9092",consumer_timeout_ms=1000, 
#                group_id="consumidores")

# for mensagem in consumidor:
#     # print(mensagem)
#     print("Topic: ", mensagem.topic)
#     print("Partição ", mensagem.partition)
#     print("Chave ", mensagem.key)
#     print("Offset ", mensagem.offset)
#     print("Mensagem ", mensagem.value)
#     print("------------------")




import io
import avro.schema
import avro.io
from kafka import KafkaConsumer

# To consume messages
CONSUMER = KafkaConsumer('Bank_Transaction',
                         group_id='group1',
                         bootstrap_servers=['localhost:9092'])

SCHEMA_PATH = "schema_transaction.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())

for msg in CONSUMER:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(SCHEMA)
    transaction1 = reader.read(decoder)
    print(transaction1)
    print(type(transaction1))