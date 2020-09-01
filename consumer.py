
from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import dumps,loads
from mleap.sklearn.logistic import LogisticRegression
#from sklearn.ensemble import RandomForestClassifier
from mleap.sklearn.svm import LinearSVC

import numpy as np
class ConsumerProducer:

    def __init__(self,pHost,cHost,gId,lrModelPath,svmModelPath) :
        self.node_name = "root"
        # creating the Kafka producer object
        self.producer = KafkaProducer(bootstrap_servers=[pHost],
                                 value_serializer=lambda x:
                                 dumps(x).encode('utf-8'))
        # create a kafka consumer object
        self.consumer = KafkaConsumer(
            'ml',
            bootstrap_servers=[cHost],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=gId,
            value_deserializer=lambda x: loads(x.decode('utf-8')))
        self.logistic_regression_tf = LogisticRegression()
        # random_forest = RandomForestClassifier()
        self.svm = LinearSVC()
        self.model_LR = self.logistic_regression_tf.deserialize_from_bundle(lrModelPath, self.node_name)
        self.model_LSVM = self.svm.deserialize_from_bundle(svmModelPath, self.node_name)

    # convert the sparse vector (features column) to dense vector
    def convertToDenseVector(self,input):
        output=[]
        for i in range(input[0]):
            output.append(0)
        ind=0
        for num in input[1]:
            output[num]=input[2][ind]
            ind +=1
        return output


    # for each message consumed, we will convert the features into dense vector,
    # predict the label using the ML model
    # check whether the prediction matches the original label (correct) or not (incorrect).
    # 1 represents correct = True and 0 represents correct = False.
    def prepareConsumedMaessagesAndSend(self):
        for message in self.consumer:
            message = message.value
            featureVector = self.convertToDenseVector(message['feature'])
            predicted_lr = self.model_LR.predict(np.array(featureVector).reshape(1, -1))
            predicted_svm = self.model_LSVM.predict(np.array(featureVector).reshape(1, -1))
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
            self.producer.send('predictions', value=lr_d)
            self.producer.send('predictions', value=svm_d)

if __name__=="__main__":
    lrModelPath="/Users/amanyabdelhalim/Desktop/weCloudData/criteo/lr_model"
    svmModelPath="/Users/amanyabdelhalim/Desktop/weCloudData/criteo/lsvm_model"
    pHost = 'localhost:9092'
    cHost = 'localhost:9092'
    groupId = 'criteo-consumer-2'
    pC = ConsumerProducer(pHost, cHost, groupId, lrModelPath, svmModelPath)
    pC.prepareConsumedMaessagesAndSend()
