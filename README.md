# Data_Streaming_Project

The code that I wrote in this project is simulating the process of ML models predicting labels on streaming data (criteo dataset).
I Developed a producer:
where I used "pyarrow" library to read the parquet file that has the test dataset. 
The producer then sends the label (class decision) and the features column to Kafka in a streaming fashion.

I Developed a consumer/producer where the consumer part: 
consumes the label (class decision) and the features column from Kafka and deserializes the logistic regression model and the SVM model. 
Converts the features column from a sparse vector to a dense vector. 
Uses the two models to predict a class label from the input features column.
The producer part:
Writes the prediction along with the original label and whether the output was correct or not to Kafka.
A value of 1 for correct indicates that the model's prediction and the original label match, 0 indicates that they didn't match.


Complete description can be found on the following link:
