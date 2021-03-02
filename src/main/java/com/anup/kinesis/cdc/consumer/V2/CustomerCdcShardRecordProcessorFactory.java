package com.anup.kinesis.cdc.consumer.V2;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class CustomerCdcShardRecordProcessorFactory implements ShardRecordProcessorFactory {
    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new CustomerCdcShardRecordProcessor();
    }
}
