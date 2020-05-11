package com.rabbitMq.rabbitmqscheduler.DTO;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@NoArgsConstructor
public class TransferJobRequestWithMetaData{
    private String ownerId;
    public String id;

    @NonNull protected TransferJobRequest.Source source;
    @NonNull protected TransferJobRequest.Destination destination;
    protected TransferOptions options;

    public static TransferJobRequestWithMetaData getTransferRequestWithMetaData(String owner,
                                                                                TransferJobRequest request){
        TransferJobRequestWithMetaData requestWithMetaData = new TransferJobRequestWithMetaData();
        requestWithMetaData.ownerId = owner;
        requestWithMetaData.source = request.getSource();
        requestWithMetaData.destination = request.getDestination();
        requestWithMetaData.options = request.getOptions();
        requestWithMetaData.id = "100";
        return requestWithMetaData;
    }
}