package com.onedatashare.scheduler.services;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.onedatashare.scheduler.model.EntityInfo;
import com.onedatashare.scheduler.model.Group;
import com.onedatashare.scheduler.model.InfluxData;
import com.onedatashare.scheduler.model.TransferJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
public class JobAndNodeClassification {

    private final MetaDataService metaDataService;

    private static final double SMALL_THRESHOLD_Gbps = 1.0; // 1 Gbps
    private static final double MEDIUM_THRESHOLD_Gbps = 10.0; // 10 Gbps
    private static final double BITS_PER_Gbps = 1_000_000_000.0; // Conversion factor from bits to Gbps
    private static final double BYTES_PER_GB = 1_073_741_824.0;
    public final IMap<UUID, Group> jobCategory;
    public final IMap<String, Group> hostCategory;
    private final Logger logger = LoggerFactory.getLogger(JobAndNodeClassification.class);


    public JobAndNodeClassification(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance, MetaDataService metaDataService) {
        this.metaDataService = metaDataService;
        this.jobCategory = hazelcastInstance.getMap("job_category");
        this.hostCategory = hazelcastInstance.getMap("host_category");
    }

    public Group classifyThroughput(String userEmail, String nodeName, int limit) {
        List<InfluxData> influxDataList = this.metaDataService.getLatestNodeMeasurements(userEmail, nodeName, limit);
        if (influxDataList == null || influxDataList.isEmpty()) {
            return Group.SMALL;
        }
        double totalThroughputBps = 0.0;
        int count = 0;

        for (InfluxData data : influxDataList) {
            logger.info(data.toString());
            if (data.getReadThroughput() != null) {
                totalThroughputBps += data.getReadThroughput();
                count++;
            }
            if (data.getWriteThroughput() != null) {
                totalThroughputBps += data.getWriteThroughput();
                count++;
            }
        }

        double averageThroughputBps = count > 0 ? totalThroughputBps / count : 0.0;
        double averageThroughputGbps = averageThroughputBps / BITS_PER_Gbps;
        Group group;
        if (averageThroughputGbps < SMALL_THRESHOLD_Gbps) {
            group = Group.SMALL;
        } else if (averageThroughputGbps < MEDIUM_THRESHOLD_Gbps && averageThroughputGbps > SMALL_THRESHOLD_Gbps) {
            group = Group.MEDIUM;
        } else {
            group = Group.LARGE;
        }

        this.hostCategory.put(nodeName, group);
        return group;
    }

    public Group classifyJobGroup(TransferJobRequest.Source source, UUID jobUuid) {
        Long jobSize = source.getInfoList().stream().mapToLong(EntityInfo::getSize).sum();
        double sizeInGB = jobSize / BYTES_PER_GB;
        Group group;
        if (sizeInGB > 100) {
            group = Group.LARGE;
        } else if (sizeInGB < 100 && sizeInGB > 10) {
            group = Group.MEDIUM;
        } else {
            group = Group.SMALL;
        }

        this.jobCategory.put(jobUuid, group);

        return group;
    }

    public Long computeJobLengthIfThroughputAvailable(TransferJobRequest.Source source) {

    }

}
