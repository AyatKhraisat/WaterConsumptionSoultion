# WaterConsumersSolution


This Solution was created to process the SARIMAX forecasting ML Model and update the model based on the model accuracy based on the new data available from Kafka.

Installing

make sure you have up-and-running docker on your machine, clone the repo and start the terminal from  the repo location and run 
`docker compose up`
12 images should be created including images for Kafka, Zookeeper, MLflow, Airflow, and Kafka UI.

##The Architecture

[<img src="https://i.ibb.co/xsLdB27/cccc-drawio-1.png">](https://i.ibb.co/xsLdB27/cccc-drawio-1.png)

##Kafka

[<img src="https://i.ibb.co/bNgg7Zr/vvvvv.png">](https://i.ibb.co/xsLdB27/cccc-drawio-1.png)

in this solution, we have Producer in producer.py the simulate producing stream using the data available from stream_dataset.csv and sent data to topic consumption_topic
Consumer in conumer.py file will consume the stream and apppend the data to  retrain_data.csv.

you can access the Kafka UI from localhost:8090 to see the messages, topics..etc.

##MLFlow 
[<img src="https://i.ibb.co/JK5S80r/bbbb.png">](https://i.ibb.co/JK5S80r/bbbb.png)

I have added the models generated to the Mlflow and saved AIC as metrics and step order as model param 
you can access Mlflow from localhost:5000

##Airflow
[<img src="https://i.ibb.co/bmnc9jj/mmmm.png">](https://i.ibb.co/bmnc9jj/mmmm.png)

you can access it from localhost:8080
contains three dags:
1. init_model_dag:
this dag should run once and create the first model using the intial_training_dataset.csv file (20% on the full dataset) which will be saved to the model's path using joblib library
2. stream dag:
will run hourly check if there is new data and send it to the topic consumption_topic using Kafka
   
3. retrain_model_dag
will run daily, and it contains the following tasks:
   1. consumer task which will get the data from the consumption topic and save the data as CSV file to retrain_data.csv
   2. retrain model task which will create a new model using the stream data predict it and compare it to the old model production based on the AIC value
   3. archiving task which will archive the stream data that already used to train the model 
    


##Machine Learning Model
I have used AIC to compare the model(lower AIC is better) if the new model has a prediction better than the old model it will replace it.


