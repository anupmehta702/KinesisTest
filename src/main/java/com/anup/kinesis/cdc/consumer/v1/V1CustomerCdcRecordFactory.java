package com.anup.kinesis.cdc.consumer.v1;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class V1CustomerCdcRecordFactory implements IRecordProcessorFactory {
    @Override
    public IRecordProcessor createProcessor() {
        return new V1CustomerCdcRecordProcessor();
    }
}
