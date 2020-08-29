# # Extends Bundle.ML Serialization for Pipelines
# import mleap.sklearn.pipeline
#
# import numpy as np
# from mleap.sklearn.logistic import LogisticRegression
# node_name = "root"
# #node_name = "{}.node".format("model.json")
#
# logistic_regression_tf = LogisticRegression()
#
# model = logistic_regression_tf.deserialize_from_bundle("/Users/amanyabdelhalim/Downloads/lr_model", node_name)
# print(model)
#
# #input=[0, 110, [12, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 27, 29, 30, 31, 32, 33, 34, 35, 36, 37, 59, 62, 89, 96], [5, 954, 4, 1, 5, 1, 1401, 1, 2, 450, 1, 3, 1, 3, 3, 3, 2, 3, 2, 1, 1, 1, 1, 1, 1]]
# input=[0, 110, [12, 13, 15, 16, 17, 18, 20, 21, 23, 24, 27, 28, 30, 31, 34, 37, 59, 61, 87, 96], [5, 1, 1476, 4, 1, 5, 588, 1, 82, 1, 2, 3, 3, 3, 3, 1, 1, 1, 1, 1]]
# def convertInput(input):
#     output=[]
#     input = input.replace("(","[").replace(")","]")

    # input=json.loads(input)
#     for i in range(110):
#         output.append(0)
#     ind=0
#     for num in input[1]:
#         output[num]=input[2][ind]
#         ind +=1
#     return output
#
# expected = model.predict(np.array(convertInput(input)).reshape(1, -1))
# print(expected)

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



