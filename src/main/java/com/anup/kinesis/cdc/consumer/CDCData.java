package com.anup.kinesis.cdc.consumer;

import lombok.*;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
//NOte - this is not used as of now . Instead Record is used
public class CDCData {

    String awsRegion;
    String tableName;
    String eventName;
    String eventSource;
    String userIdentity;
    String recordFormat;
    String eventID;

}
