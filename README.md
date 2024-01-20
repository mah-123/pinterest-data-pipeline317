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
## 1.3 Creating a API Gateway for batch processing
After creating the MSK connection, we can use the S3 and the previous kafka topics to store it
into the s3 bucket to collect data from the kafka consumer.
* On the aws specific account, open the API gateway console and create a proxy integration
* copy a your unique endpoint url and apply it to the creates proxy API with all http access
* Set up by deploying the API.
* Once deployed, you would require to download additional confluent package to enable EC2 rest requets 
* Adjust the confluent properties by using nano to change the file and additional info e.g. IAM authentication
* Save the file information and you can start the kafka consumer, to listen in a request.
* Make sure to copy the Invoke_url which will be implemented to user_posting_emulation.py
* Adjust the script to take in the geo, pin and user into payload and add a response to take "post" from the Invoke_URL

## 2.1 Databrick
In this section, when you have created the API gateway to store the three topics, you would need access to your Databricks account which will be mounted to with your AWS S3 bucket access.
* Create a s3 authenitcation file containing access key and security on AWS 
* Once created, upload it directly to databrick csv file.
* Since the csv file was preuploaded for the specific Databrick account, you can a create notebook to munt the S3
* When mounting, ensure that the s3 bucket matches with the previous s3 bucket info and the mount_name should have a suitable name format
* Mounting can be done once, so you do not need to rerun the specific mount scripts
* Once mounted, you can now read directly the .json on the specific topics pin, geo and user into dataframe.
* Make sure the file path to each topics matches to s3 url path e.g."mnt/aws_mount/user_info_file-path/*json." for the file_path variable.
* This should be configured on three topics.

## 2.2 Databrick query
From the previous dataframe created on the notebook, you can use it to query the data for specifc output for user, pin and geo information
which can be seen on the notebook. Before using the data frame, you would need to clean the df_pin, df_user and df_pin for any erroneous value, adjust the order and correcting data type such as age to integer and timestamp for post_date and date_joined column. Some of the query examples using pyspark include:
* popular category for each country
* follower counts for each country
* age groups based on certain category

## 2.3
 
## Installation
At the moment there is no full structured programme that can be utilised.

## License

##