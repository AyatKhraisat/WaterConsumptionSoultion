import ast
import json
import time
import os
import pandas as pd
from confluent_kafka import Consumer, KafkaException, KafkaError
from pathlib import Path


def get_new_data(**kwargs):
    consumer = Consumer({'bootstrap.servers': "kafka:9092", 'group.id': '0', 'auto.offset.reset': 'earliest'})
    consumer.subscribe(['consumption_topic'])
    messages = []
    consumer_have_data=True
    none_messages=0
    try:
        while consumer_have_data:
                try:
                    msg = consumer.poll(1)

                    if msg is None:
                        print('message is none')
                        none_messages=none_messages+1
                        if(none_messages>=20):
                            break
                        else:
                            continue
                    else:
                        none_messages=0
                        message_value = msg.value().decode('utf-8')
                        print(json.dumps(message_value))
                        msg_value = ast.literal_eval(message_value)
                        messages.append(msg_value)

                except Exception as e:
                    consumer_have_data = False
                    print('Kafka failure ', e)


    except Exception as e:
        print('Kafka failure ', e)

    path = kwargs['data_path']
    filepath = Path(path)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(messages)

    with open(filepath, 'a') as f:
        df.to_csv(f, header=f.tell() == 0, index=False)
    consumer.close()


def archive_data(**kwargs):
    os.rename(kwargs['data_path'], '/data/archived_data_' + str(time.strftime("%Y%m%d_%H%M")) + '.csv')
