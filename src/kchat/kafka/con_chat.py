from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
        'chat',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id="chat-group",
        enable_auto_commit=True,
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        )
print("채팅 프로그램 - 메시지 수신")
print("메시지 대기 중 ... 잠시만 기다료")

try:
    for m in consumer:
        data = m.value
        print(f"[박만주] {data['message']}")
except KeyboardInterrupt:
    print("채팅 종료")
finally:
    consumer.close()
