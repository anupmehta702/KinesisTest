package com.anup.kinesis.cdc.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import lombok.*;

/*
    INSERT
    {"awsRegion":"us-east-2",
    "dynamodb":{
        "ApproximateCreationDateTime":1614669851662,
        "Keys":{"id":{"S":"3"},"name":{"S":"ABC"}},
        "NewImage":{"email":{"S":"abc@email.com"},"id":{"S":"3"},"name":{"S":"ABC"}},
        "SizeBytes":38
    },
    "eventID":"273538ad-fc6e-453c-90d5-999d2399375d",
    "eventName":"INSERT","userIdentity":null,"recordFormat":"application/json",
    "tableName":"customer","eventSource":"aws:dynamodb"
    }
    UPDATE
    {"awsRegion":"us-east-2","dynamodb":{"ApproximateCreationDateTime":1614669922526,
    "Keys":{"id":{"S":"3"},"name":{"S":"ABC"}},
    "NewImage":{"email":{"S":"abc123@email.com"},"id":{"S":"3"},"name":{"S":"ABC"}},
    "OldImage":{"email":{"S":"abc@email.com"},"id":{"S":"3"},"name":{"S":"ABC"}},"SizeBytes":69},
    "eventID":"b2945a3e-239e-4c49-8978-bb96c843bee8","eventName":"MODIFY","userIdentity":null,
    "recordFormat":"application/json",
    "tableName":"customer","eventSource":"aws:dynamodb"}
     */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class CDCData {

    String awsRegion;
    String tableName;
    String eventName;
    String eventSource;
    String userIdentity;
    String recordFormat;
    String eventID;

}
