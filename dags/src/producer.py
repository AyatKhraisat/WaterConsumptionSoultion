import csv
import json
import time
import socket

from confluent_kafka import Producer


def create_stream():
    def acked(err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

    producer = Producer({'bootstrap.servers': "kafka:9092"})

    with open('/data/test.csv') as file:
        reader = csv.DictReader(file, delimiter=",")
        for row in reader:
            message = json.dumps(row)

            print("send message: ",message)

            producer.produce(topic='consumption_topic',value=message, callback=acked)
            producer.poll(1)
    producer.flush()

#
# if __name__ == '__main__':
#     create_stream()