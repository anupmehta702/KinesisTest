**Kinesis project to test consumer and producer **
_Reference https://docs.aws.amazon.com/streams/latest/dev/tutorial-stock-data-kplkcl.html

KCL2 -- https://docs.aws.amazon.com/streams/latest/dev/kcl2-standard-consumer-java-example.html

Reflection -- http://tutorials.jenkov.com/java-json/jackson-objectmapper.html#read-object-from-json-string

CDC -- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.KCLAdapter.Walkthrough.html 

**Steps to run **
<br /> 1. Create a user on AWS and ensure he has Kinesis full access. Copy the accessKey and secretKey and configure it in aws cli using "aws configure"
<br /> 2. Create a stream called "StockRecordStream"
<br /> 3. Run StockTradeWriter to produce a record
<br /> 4. Run StockTradeReaderConfiguration to consume the message sent in step 2

** Steps to run for CDC **
<br /> 1. Create a table called "customer" in DynamoDB
<br /> 2. Create a Kinesis data stream named "CustomerCdcStream"
<br /> 3. Also create an S3 bucket named "anupmehts702". This the bucket where it would create a file and insert CDC content of "customer" table
<br /> 4. Add/Update/Delete record in table from aws UI. This would generate an event in the data stream. 
<br /> 5. Run V2CustomerCdcReader.java 
<br /> 6. It first creates a local csv file named "cdcCustomer.csv". 
and then updates the file on S3 with same file name
<br /> 7. YOu can use BucketOperation class to read and download the uploaded file in step 6 by running BucketOperation.downloadObject() method 