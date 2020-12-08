package com.rabbitMq.rabbitmqscheduler.DTO;

import com.rabbitMq.rabbitmqscheduler.Enums.EndPointType;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.HashSet;

@Data
@NoArgsConstructor
@Accessors(chain = true)
public class TransferJobRequest {
    @NonNull protected Source source;
    @NonNull protected Destination destination;
    protected TransferOptions options;


    @Data
    @Accessors(chain = true)
    public static class Destination {
        @NonNull protected EndPointType type;
        @NonNull protected String credId;
        @NonNull protected EntityInfo info;
        protected EndpointCredential credential;
    }

    @Data
    @Accessors(chain = true)
    public static class Source {
        @NonNull protected EndPointType type;
        @NonNull protected String credId;
        @NonNull protected EntityInfo info;
        @NonNull protected HashSet<EntityInfo> infoList;
        protected EndpointCredential credential;
    }

    @Data
    @Accessors(chain = true)
    public static class EntityInfo {
        protected String id;
        protected String path;
        protected long size;
    }

}
