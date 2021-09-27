from kafka import KafkaConsumer
from json import loads
from time import sleep

consumer = KafkaConsumer(
    'streams-wordcount-output',
     bootstrap_servers=['localhost:9092'],)
int_val = 0
tmp_key = None
for message in consumer:
    #print(message)
    #print(message.key)
    j = message.key[0:5].decode('utf-8','ignore')
    
    
    if j == 'harry':
        if tmp_key == None:
            tmp_key = message.key[-4:]
            message = message.value.replace(b'\x00',b'')
            int_val = int.from_bytes(message,"big")
            
        elif tmp_key != None and tmp_key == message.key[-4:]:
            message = message.value.replace(b'\x00',b'')
            int_val = int.from_bytes(message,"big")
            
        elif tmp_key != None and tmp_key != message.key[-4:]:
            print("harry :",int_val)
            tmp_key = message.key[-4:]
            message = message.value.replace(b'\x00',b'')
            int_val = int.from_bytes(message,"big")
            
    else:
        if tmp_key == None:
            tmp_key = message.key[-4:]
        elif tmp_key != None and tmp_key != message.key[-4:]:
            print("harry :",int_val)
            tmp_key = message.key[-4:]
            int_val = 0
	
	
    #message = message.value.replace(b'\x00',b'')
    #print(message)
    #int_val = int.from_bytes(message,"big")
    #message = message.decode('utf-8','ignore')
    
    #print(int_val)
    #sleep(1)
