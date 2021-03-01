package com.anup.kinesis.cdc.consumer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.anup.kinesis.cdc.consumer.iRecordProcessor.CustomerCdcRecordFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.coordinator.Scheduler;

public class CustomerCdcReader {
    private Regions region = Regions.US_EAST_2;
    private String streamName = "CustomerCdcStream";
    private String applicationName = "customer-cdc-reader-processor";
    private String streamArn = "arn:aws:kinesis:us-east-2:406556759297:stream/CustomerCdcStream";

    public CustomerCdcReader() throws InterruptedException {
        setup();
    }

    private void setup() throws InterruptedException {

        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
        AmazonCloudWatch cloudWatchClient = AmazonCloudWatchClientBuilder.standard()
                .withRegion(region)
                .build();
        CustomerCdcRecordFactory iRecordProcessor = new CustomerCdcRecordFactory();

        AmazonDynamoDBStreams dynamoDBStreamsClient = AmazonDynamoDBStreamsClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();

        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreamsClient);

        KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration(
                applicationName,
                streamArn,
                DefaultAWSCredentialsProviderChain.getInstance(),
                "customer-cdc-worker")
                .withMaxRecords(1000)
                .withIdleTimeBetweenReadsInMillis(500)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        System.out.println("Creating worker for stream: " + streamArn);
        Worker worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(iRecordProcessor, workerConfig,
                adapterClient, dynamoClient, cloudWatchClient);
        System.out.println("Starting worker...");
        Thread t = new Thread(worker);
        t.start();

        Thread.sleep(25000);
        worker.shutdown();
        t.join();

        /*ConfigsBuilder configsBuilder = new
                ConfigsBuilder(streamName, applicationName, kinesisAsyncClient, dynamoClient,
                cloudWatchClient, UUID.randomUUID().toString(), iRecordProcessor);

         scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );
*/

    }

    /*public void startReading() {
        int exitCode = 0;
        try {
            System.out.println("Started reading data from stream -" + streamName);
            scheduler.run();
        } catch (Throwable t) {
            System.out.println("Caught throwable while processing data." + t);
            exitCode = 1;
        }
        System.exit(exitCode);

    }*/

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Consumer for CDC stream ...");
        CustomerCdcReader reader = new CustomerCdcReader();
        //reader.startReading();
    }
}
