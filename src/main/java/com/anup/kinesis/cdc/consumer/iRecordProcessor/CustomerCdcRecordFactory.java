package com.anup.kinesis.cdc.consumer.iRecordProcessor;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class CustomerCdcRecordFactory implements IRecordProcessorFactory {
    @Override
    public IRecordProcessor createProcessor() {
        return new CustomerCdcRecordProcessor();
    }
}
