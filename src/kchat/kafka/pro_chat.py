from kafka import KafkaProducer
from json import dumps
import time

producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
        )
print("채팅 프로그램 - 메시지 발신")
print("메시지를 입력하세요. (종료하려면 'exit' 입력하삼)")

while True:
    msg = input("YOU: ")
    if msg == 'exit':
        break
    
    data = {'message': msg, 'time': time.time()}
    
    producer.send('chat', value=data)
    producer.flush()

print("채팅 종료")

producer.close()
