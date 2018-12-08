# Metservice Assessment

Unfortunately I haven't had time to create the SAM template to share with you for this assessment, so I'm attaching the lambda functions, the API Gateway template, and the front-end.

I created the following serverless services:

* S3 Bucket for web hosting and store data file (CSV and JSON)
* When someone upload a file, a lambda function is triggered to parse and save this data to DynamoDB tables
* 2 Lambda function (parsing CSV and JSON files) and 1 Lambda to clear the Tables
* 4 DynamoDB tables: direction, speed, height, and temperature
* API Gateway with 5 endpoints. Example for 'direction' data:
    ### https://59kkf4hheb.execute-api.ap-southeast-2.amazonaws.com/prod/direction/20181114000000 --> Specific date
    ### https://59kkf4hheb.execute-api.ap-southeast-2.amazonaws.com/prod/direction/ --> All Items

### Web Endpoint: http://met-service-assesment.s3-website-ap-southeast-2.amazonaws.com/

Serverless Architecture:

![alt text](https://raw.githubusercontent.com/lobo-nz/metservices_serverless/master/Arch.jpeg)
