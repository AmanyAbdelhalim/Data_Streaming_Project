
from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import dumps,loads
from mleap.sklearn.logistic import LogisticRegression
#from sklearn.ensemble import RandomForestClassifier
from mleap.sklearn.svm import LinearSVC

import numpy as np
node_name = "root"
logistic_regression_tf = LogisticRegression()
#random_forest = RandomForestClassifier()
svm = LinearSVC()
# creating the Kafka producer object
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

#model_RF = random_forest
model_LR = logistic_regression_tf.deserialize_from_bundle("/Users/amanyabdelhalim/Desktop/weCloudData/criteo/lr_model", node_name)
model_LSVM = svm.deserialize_from_bundle("/Users/amanyabdelhalim/Desktop/weCloudData/criteo/lsvm_model", node_name)

#print(model_LSVM)

# convert the sparse vector (features column) to dense vector
def convertToDenseVector(input):
    output=[]
    for i in range(input[0]):
        output.append(0)
    ind=0
    for num in input[1]:
        output[num]=input[2][ind]
        ind +=1
    return output
# create a kafka consumer object
consumer = KafkaConsumer(
    'ml',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='criteo-consumer-2',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

# for each message consumed, we will convert the features into dense vector,
# predict the label using the ML model
# check whether the prediction matches the original label (correct) or not (incorrect).
# 1 represents correct = True and 0 represents correct = False.
for message in consumer:
    message = message.value
    featureVector = convertToDenseVector(message['feature'])
    predicted_lr = model_LR.predict(np.array(featureVector).reshape(1, -1))
    predicted_svm = model_LSVM.predict(np.array(featureVector).reshape(1, -1))
    if predicted_lr[0] == message['label']:
        correct_lr = 1
    else:
        correct_lr = 0

    if predicted_svm[0] == message['label']:
        correct_svm = 1
    else:
        correct_svm = 0

    print('Expected {}  -- > Got: {} with logistic regression model and correct {}'.format(message['label'],predicted_lr,correct_lr))
    print('Expected {}  -- > Got: {} with random forest model and correct {}'.format(message['label'],predicted_svm,correct_svm))

    # preparing the two dictionaries that will be sent to kafka, one for each model
    # logistic regression dictionary
    lr_d = dict()
    lr_d["model"] = "lr"
    lr_d["prediction"] = str(predicted_lr[0])
    lr_d["label"] = message['label']
    # adding correct value to the dictionaries
    lr_d["correct"] = correct_lr

    # SVM dictionary
    svm_d = dict()
    svm_d["model"] = "svm"
    svm_d["prediction"] = str(predicted_svm[0])
    svm_d["label"] = message['label']
    # adding correct value to the dictionaries
    svm_d["correct"] = correct_svm

    # send the prediction, label and correct to kafka again to be consumed by logstash
    producer.send('predictions', value=lr_d)
    producer.send('predictions', value=svm_d)

