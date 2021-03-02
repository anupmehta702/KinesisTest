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
