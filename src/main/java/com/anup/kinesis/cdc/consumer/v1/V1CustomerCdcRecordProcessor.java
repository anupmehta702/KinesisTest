package com.anup.kinesis.cdc.consumer.v1;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.anup.kinesis.model.Customer;

import java.nio.charset.Charset;
import java.util.Map;

public class V1CustomerCdcRecordProcessor implements IRecordProcessor {
    private Integer checkpointCounter;

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        for (Record record : processRecordsInput.getRecords()) {
            String data = new String(record.getData().array(), Charset.forName("UTF-8"));
            System.out.println(data);
            if (record instanceof RecordAdapter) {
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
                        System.out.println("Retrieved new record --> "+customer);
                        break;
                    case "MODIFY":
                        break;
                    case "REMOVE":
                }
            }
            checkpointCounter += 1;
            if (checkpointCounter % 10 == 0) {
                try {
                    processRecordsInput.getCheckpointer().checkpoint();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }


    @Override
    public void initialize(InitializationInput initializationInput) {

    }


    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            try {
                shutdownInput.getCheckpointer().checkpoint();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
