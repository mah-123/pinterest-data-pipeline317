# pinterest-data-pipeline

## Introduction
This project compromises of using user information to implement a pinterest data pipeline by using Kafka and aws3
services.

## 1.1 Creating an EC2 instance environment
For this section you would have to utilise the given IAM user_id information to create the following:
* Find the key pair value associated with the user_id to save the information to a pem.
* Create a EC2 instance on the terminal with the following settings shown on EC2 console.
* Set up the Kafka package on the EC2 intance, finding the arn information to the user_id and kafka clusters setup
* Create the three topics from the user_id information e.g. finding the bootstrap string and the zoo apache string.

This is a follow up to creating the batch processing.

## 1.2 Creating a MSK connection
Once you have created the EC2 instance you would be looking to make MSK:
* Finding the assocaited S3 bucket from the user_id for the IAM account with user info
* downloading the a confluent.io on the EC2 client terminal directly to the S3 bucket
* create a plug-in for MSK, ensuring that the naming of plug-in is assocaited with the user-id
* The MSK plug-in can used to make the MSK connector with right configuration setting to the cluster.

This will enable us to help with setting up API gateway.
## 1.3 Creating

## Installation
At the moment there is no full structured programme that can be utilised.

## License

##