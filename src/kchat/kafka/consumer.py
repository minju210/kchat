from kafka import KafkaConsumer, TopicPartition
from json import loads
import os

OFFSET_FILE = 'consumer_offset.txt'

def save_offset(offset):
    with open(OFFSET_FILE, 'w') as f:
        f.write(str(offset))

def read_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return int(f.read().strip())
    return None
saved_offset = read_offset()

consumer = KafkaConsumer(
        #"topic1",
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=5000,
        #auto_offset_reset='earliest' if saved_offset is None  else 'none',
        #auto_offset_reset='latest',
        group_id="aespa",
        enable_auto_commit=True
        )

print('[Start] get consumer')

if saved_offset is not None:
    p = TopicPartition('topic1', 0)
    consumer.assign([p])
    consumer.seek(p, saved_offset)

for m in consumer:
    print(f"offset={m.offset}, value={m.value}")

    save_offset(m.offset + 1)

    #topic='topic1', partition=0, offset=102, timestamp=1724218049304, timestamp_type=0, key=None, value={'str': 'value9'}, headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=17, serialized_header_size=-1

print('[End] get consumer')
