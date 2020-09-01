import sys
from time import sleep
from json import dumps
from kafka import KafkaProducer
import json
import pyarrow.parquet as pq
import os


class Producer:
    def __init__(self, test_data_path, host):
        # read the test data(label, features) stored as a parquet file object
        self.parquet_file = pq.ParquetFile(test_data_path)
        print('-----------------------')
        # creating the Kafka producer object
        self.producer = KafkaProducer(bootstrap_servers=[host],
                                      value_serializer=lambda x:
                                      dumps(x).encode('utf-8'))

    def produce(self):
        # read the two attributes and put them in a table
        table = self.parquet_file.read(columns=["label", "features1"], use_threads=True)
        # column 0 is the label
        label = table.columns[0]
        # column 1 is the features
        features = table.columns[1]

        # creating an empty dictionary that will hold the message that we will send to Kafka
        d = dict()
        for i in range(len(label)):
            l = label[i].as_py()  # converting the arrow scalar types to python types
            f = features[i].as_py()  # converting the arrow scalar types to python types
            # formatting the string to look like json string
            feature_str = f.replace("(", "[").replace(")", "]")
            # converts the json array string to list object
            feature_list = json.loads(feature_str)
            d["label"] = l
            d["feature"] = feature_list
            print(d)  # printing the dictionary that has the label and the feature
            # send the message (dictionary with the label and features) to Kafka
            self.producer.send('ml', value=d)
            sleep(3)


if __name__ == "__main__":
    # test_data_path_env = '/Users/amanyabdelhalim/Desktop/weCloudData/criteo/feature_label_only.parquet'
    if 'TEST_DATA_PATH' not in os.environ:
        print("TEST_DATA_PATH environment variable can't be empty. Exiting...")
        sys.exit(1)
    test_data_path_env = os.environ['TEST_DATA_PATH']

    if 'KAFKA_BOOTSTRAP_PORT' not in os.environ:
        print("KAFKA_BOOTSTRAP_PORT environment variable can't be empty. Exiting...")
        sys.exit(1)
    host_env = os.environ['KAFKA_BOOTSTRAP_PORT']


    p = Producer(test_data_path_env, host_env)
    p.produce()
