
from time import sleep
from json import dumps
from kafka import KafkaProducer
import json
import pyarrow.parquet as pq

# read the test data(label, features) stored as a parquet file object
parquet_file = pq.ParquetFile('/Users/amanyabdelhalim/Desktop/weCloudData/criteo/feature_label_only.parquet')

print('-----------------------')
# creating the Kafka producer object
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
# read the two attributes and put them in a table
table = parquet_file.read(columns=["label","features1"],use_threads=True)
# column 0 is the label
label = table.columns[0]
# column 1 is the features
features = table.columns[1]

# creating an empty dictionary
d = dict()
for i in range(len(label)):
    l=label[i].as_py() # converting the arrow scalar types to python types
    f=features[i].as_py() # converting the arrow scalar types to python types
    # formatting the string to look like json string
    featureStr = f.replace("(", "[").replace(")", "]")
    # converts the json array string to list object
    featureList = json.loads(featureStr)
    d["label"]=l
    d["feature"]= featureList
    print(d)# printing the dictionary that has the label and the feature
    # send the message (dictionary with the label and features) to Kafka
    producer.send('ml', value=d)
    sleep(3)



