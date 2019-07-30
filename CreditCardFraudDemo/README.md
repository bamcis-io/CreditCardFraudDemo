# Implementation Walkthrough

1) Deploy the Kineis Stream Awaiter lambda function to be used as a custom resource.

2) Deploy the CloudFormation & Lambda function for the CC fraud demo. 

3) Deploy the AmazonS3KeyToHivePartitionScheme Lambda function. Note the SNS topic ARN that is the output of this project's CFN template, it is a parameter you need to supply to the CFN template for `LambdaSNSLauncherTopicArn`. The value you use for the parameter `CreditCardTransactionsBucketName` in this project will be the `OutputBucketName` parameter value in the CFN template (which will also create that bucket). Also, make sure you specify SNS as the `EventNotificationMethod`.

4) Instantiate Quicksight and load the custom SQL queries created in this CFN as data sources (if desired) that were created in Athena.

	Sum of Fraud by Amount (Bar Graph)
	Sum of Amount by Fraud (Pie Chart)
	Sum of Fraud by Lat/Long (Geo graph)

5) Manually enable the AWS Glue Job Trigger in the AWS Management Console (cannot be done via CFN). It will run every 5 minutes and add new data that can be observed in Quicksight.

6) Update the API Gateway endpoint value in the `Producer.ps1` script and run it to start simulating transactions.

7) Run periodic (hourly, since this is how the data is partitioned) SQL queries to add new partitions to the raw table `MSCK REPAIR TABLE rawcctransactions` (the processed table does not use partitions for ease of use during the demo).
