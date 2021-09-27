#Producer
from time import sleep
from json import dumps
import datetime
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

f = open(r"C:\kafka_2.13-2.7.0\book.txt", "r",encoding="utf8")





for i in f:
    data = i
    print(data)
    producer.send('streams-plaintext-input', value=data)
    sleep(1)
