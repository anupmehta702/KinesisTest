package com.anup.kinesis.producer;

import com.anup.kinesis.model.StockTrade;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.kinesis.common.KinesisClientUtil;

import java.util.concurrent.ExecutionException;

public class StockTradeWriter {
    private KinesisAsyncClient kinesisClient;
    private Region region = Region.US_EAST_2;
    private String streamName = "StockTradeStream";

    public StockTradeWriter(){
        setUpProducer();
        validateStream();
    }
    private void setUpProducer(){
         kinesisClient = KinesisClientUtil
                .createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));
    }

    private void validateStream(){
        try {
            DescribeStreamRequest describeStreamRequest =  DescribeStreamRequest.builder()
                    .streamName(streamName).build();
            DescribeStreamResponse describeStreamResponse = kinesisClient
                    .describeStream(describeStreamRequest).get();
            if(!describeStreamResponse.streamDescription().streamStatus().toString().equals("ACTIVE")) {
                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }else{
                System.out.println("Stream " + streamName + " is active");
            }
        }catch (Exception e) {
            System.err.println("Error found while describing the stream " + streamName);
            System.err.println(e);
            System.exit(1);
        }
    }

    public void sendStockTrade(){
        StockTrade stockTrade = new StockTradeGenerator().getRandomTrade();
        byte[] bytes = stockTrade.toJsonAsBytes();
        PutRecordRequest request = PutRecordRequest
                .builder()
                .data(SdkBytes.fromByteArray(bytes))
                .partitionKey(stockTrade.getTickerSymbol())
                .streamName(streamName)
                .build();

        try {
            System.out.println("Sending stock - "+stockTrade);
            PutRecordResponse response = kinesisClient.putRecord(request).get();
            System.out.println("Stock sent successfully with response -- "+response);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting Producer ...");
        StockTradeWriter tradesWriter = new StockTradeWriter();
        tradesWriter.sendStockTrade();
    }

}
