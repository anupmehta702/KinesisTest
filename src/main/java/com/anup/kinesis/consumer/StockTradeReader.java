package com.anup.kinesis.consumer;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;

import java.util.UUID;

public class StockTradeReader {
    private KinesisAsyncClient kinesisAsyncClient;
    private Region region = Region.US_EAST_2;
    private String streamName = "StockTradeStream";
    private String applicationName = "StockTradeProcessor";
    private Scheduler scheduler;

    public StockTradeReader(){
        setup();
    }

    private void setup(){
        kinesisAsyncClient = KinesisClientUtil
                .createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));

        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
        StockTradeRecordProcessorFactory shardRecordProcessor = new StockTradeRecordProcessorFactory();

        ConfigsBuilder configsBuilder = new
                ConfigsBuilder(streamName, applicationName, kinesisAsyncClient, dynamoClient,
                cloudWatchClient, UUID.randomUUID().toString(), shardRecordProcessor);

         scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );


    }
    public void startReading(){
        int exitCode = 0;
        try {
            System.out.println("Started reading data from stream -"+streamName);
            scheduler.run();
        } catch (Throwable t) {
            System.out.println("Caught throwable while processing data."+ t);
            exitCode = 1;
        }
        System.exit(exitCode);

    }

    public static void main(String[] args) {
        System.out.println("Starting Consumer ...");
        StockTradeReader reader = new StockTradeReader();
        reader.startReading();
    }
}
