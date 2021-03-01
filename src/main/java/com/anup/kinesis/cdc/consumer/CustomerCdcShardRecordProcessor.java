package com.anup.kinesis.cdc.consumer;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.anup.kinesis.model.Customer;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.Map;

public class CustomerCdcShardRecordProcessor implements ShardRecordProcessor {
    private String shardId;
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    @Override
    public void initialize(InitializationInput initializationInput) {
        System.out.println("Initializing record processor for shardId -- "+initializationInput.shardId());
    }


    /*
    {"awsRegion":"us-east-2","dynamodb":{"ApproximateCreationDateTime":1614603432364,
    "Keys":{"id":{"S":"2"},"name":{"S":"Anoop"}},"NewImage":{"id":{"S":"2"},"name":{"S":"Anoop"}},"SizeBytes":24},
    "eventID":"62e4ea4d-c275-4949-a96d-6f8e5fd34d79",
    "eventName":"INSERT","userIdentity":null,"recordFormat":"application/json","tableName":"customer","eventSource":"aws:dynamodb"}
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput){
        System.out.println("Processing " + processRecordsInput.records().size() + " record(s)");
        List<KinesisClientRecord> records = processRecordsInput.records();
        for(KinesisClientRecord record : processRecordsInput.records()) {
            try {
                System.out.println("Printing partition key "+record.partitionKey());
                //String data = new String(record.data().array());
                String data = decoder.decode(record.data()).toString();
                System.out.println("Printing data -->"+data);
                /*if (record instanceof RecordAdapter) {
                    com.amazonaws.services.dynamodbv2.model.Record streamRecord = ((RecordAdapter) record)
                            .getInternalObject();

                    switch (streamRecord.getEventName()) {
                        case "INSERT":
                            System.out.println("INSERT record set values -->"+streamRecord.getDynamodb().getNewImage());
                            Map<String, AttributeValue> newData = streamRecord.getDynamodb().getNewImage();
                            Customer customer = new Customer();
                            customer.setId(newData.get("id").getS());
                            customer.setEmail(newData.get("name").getS());
                            customer.setName(newData.get("email").getS());
                            System.out.println("Retrived new record --> "+customer);
                            break;
                        case "MODIFY":

                            break;
                        case "REMOVE":

                    }
                }*/
            }catch(Exception ex){
                System.out.println("Exception while reading record -- "+ex.getMessage());
                ex.printStackTrace();
            }

        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        System.out.println("Lost lease so terminating");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            System.out.println("Reached shard endpoint !");
            shardEndedInput.checkpointer().checkpoint();
        } catch (InvalidStateException e) {
            e.printStackTrace();
        } catch (ShutdownException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        checkpoint(shutdownRequestedInput.checkpointer());
    }
    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        System.out.println("Checkpointing shard " + shardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            System.out.println("Caught shutdown exception, skipping checkpoint."+ se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            System.out.println("Caught throttling exception, skipping checkpoint."+ e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            System.out.println("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library."+ e);
        }
    }
}
