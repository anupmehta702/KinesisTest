package com.anup.kinesis.consumer;

import com.anup.kinesis.model.StockTrade;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.List;

public class StockTradeRecordProcessor implements ShardRecordProcessor {
    private String shardId;
    @Override
    public void initialize(InitializationInput initializationInput) {
        System.out.println("Initializing record processor for shardId -- "+initializationInput.shardId());
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput){
        System.out.println("Processing " + processRecordsInput.records().size() + " record(s)");
        List<KinesisClientRecord> records = processRecordsInput.records();
        for(KinesisClientRecord record : records) {
            byte[] arr = new byte[record.data().remaining()];
            record.data().get(arr);
            StockTrade trade = StockTrade.fromJsonAsBytes(arr);
            System.out.println("Retrieved data from stream --> "+trade);
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
