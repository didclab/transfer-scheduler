package com.onedatashare.scheduler.services;

import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.onedatashare.scheduler.enums.MessageType;
import com.onedatashare.scheduler.model.TransferJobRequest;
import com.onedatashare.scheduler.model.TransferSla;
import com.onedatashare.scheduler.model.carbon.CarbonIpEntry;
import com.onedatashare.scheduler.model.carbon.CarbonMeasurement;
import com.onedatashare.scheduler.services.maps.CarbonMapService;
import com.onedatashare.scheduler.services.maps.InitialAndFinalJobCarbonMapService;
import com.onedatashare.scheduler.services.maps.TransferSchedulerMapService;
import jakarta.annotation.PostConstruct;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class JobSchedulerAlgo implements Runnable {

    private final CarbonMapService carbonIntensityMap;
    private final TransferSchedulerMapService transferSchedulerMapService;
    private final InitialAndFinalJobCarbonMapService initialAndFinalJobCarbonMapService;
    private final MessageSender messageSender;
    private final Logger logger = LoggerFactory.getLogger(JobSchedulerAlgo.class);
    private final IScheduledExecutorService jobSchedulerExecutorService;
    private final JobAndNodeClassification jobAndNodeClassification;

    @Autowired
    FileTransferNodeDiscovery fileTransferNodeDiscovery;

    public JobSchedulerAlgo(IScheduledExecutorService jobSchedulerExecutorService, InitialAndFinalJobCarbonMapService initialAndFinalJobCarbonMapService, TransferSchedulerMapService transferSchedulerMapService, CarbonMapService carbonMapService, MessageSender messageSender, JobAndNodeClassification jobAndNodeClassification) {
        this.transferSchedulerMapService = transferSchedulerMapService;
        this.carbonIntensityMap = carbonMapService;
        this.initialAndFinalJobCarbonMapService = initialAndFinalJobCarbonMapService;
        this.messageSender = messageSender;
        this.jobSchedulerExecutorService = jobSchedulerExecutorService;
        this.jobAndNodeClassification = jobAndNodeClassification;
    }

    @PostConstruct
    public void init() {
        this.jobSchedulerExecutorService.scheduleOnAllMembersAtFixedRate(this, 1, 20, TimeUnit.SECONDS);
    }

    @SneakyThrows
    public void run() {
        //iterate over potential jobs and see which meet the carbon sla
        for (UUID uuid : this.transferSchedulerMapService.getLocalKeys()) {
            TransferJobRequest scheduledFileTransfer = this.transferSchedulerMapService.getValue(uuid);
            //get all carbon measurements pertaining to job done by all free nodes.
            List<CarbonMeasurement> carbonMeasurements = this.carbonIntensityMap.getCarbonMeasurementsForUserAndJob(scheduledFileTransfer.getOwnerId(), scheduledFileTransfer.getJobUuid());
            if (carbonMeasurements.isEmpty()) {
                continue;
            }

            //For a Job get the latest measurement for every node.
            HashMap<String, CarbonMeasurement> transferNodeToLatestCarbonMeasurement = new HashMap<>();
            for (CarbonMeasurement carbonMeasurement : carbonMeasurements) {
                CarbonMeasurement latestCarbonMeasurement = transferNodeToLatestCarbonMeasurement.get(carbonMeasurement.getTransferNodeName());
                if (latestCarbonMeasurement == null) {
                    transferNodeToLatestCarbonMeasurement.put(carbonMeasurement.getTransferNodeName(), carbonMeasurement);
                } else {
                    if (latestCarbonMeasurement.getTimeMeasuredAt().isAfter(carbonMeasurement.getTimeMeasuredAt())) {
                        transferNodeToLatestCarbonMeasurement.put(carbonMeasurement.getTransferNodeName(), carbonMeasurement);
                    }
                }
            }

            //iterate over all possible options for 1 job and find the node with the lowest carbon intensity
            double minCi = Double.MAX_VALUE;
            CarbonMeasurement lowestCarbonMeasurement = new CarbonMeasurement();
            //find node with lowest ci to run the job
            for (Map.Entry<String, CarbonMeasurement> entry : transferNodeToLatestCarbonMeasurement.entrySet()) {
                double currentCi = this.averageCarbonIntensity(entry.getValue());
                if (minCi > currentCi) {
                    minCi = currentCi;
                    lowestCarbonMeasurement = entry.getValue();
                }
            }

            //if this is the first min measurement, store it in map and try to find lower.
            if (!this.initialAndFinalJobCarbonMapService.containsKey(scheduledFileTransfer.getJobUuid())) {
                this.initialAndFinalJobCarbonMapService.putStartMeasurementForJob(scheduledFileTransfer.getJobUuid(), lowestCarbonMeasurement);
                continue;
            }

            //check if SLA is met
            TransferSla sla = scheduledFileTransfer.getTransferSla();
            double initialScheduledJobCi = this.averageCarbonIntensity(this.initialAndFinalJobCarbonMapService.getInitialMeasurement(scheduledFileTransfer.getJobUuid()));
            double targetCi = initialScheduledJobCi * (1 + sla.getPercentCarbon());
            logger.info("Min Ci: {} the target Ci we are going for is: {} \n Min POJO is: {} for job: {}", minCi, targetCi, lowestCarbonMeasurement, scheduledFileTransfer.getJobUuid());

            if (minCi <= targetCi && !this.fileTransferNodeDiscovery.getFileTransferNode(lowestCarbonMeasurement.getTransferNodeName()).getRunningJob()) {
                //we launch the job
                logger.info("Min Ci meets the threshold Target CI using TransferNode: {} to run job: {}", lowestCarbonMeasurement.getTransferNodeName(), scheduledFileTransfer);
                scheduledFileTransfer.setTransferNodeName(lowestCarbonMeasurement.getTransferNodeName());
                this.initialAndFinalJobCarbonMapService.putFinalMeasurementForJob(scheduledFileTransfer.getJobUuid(), lowestCarbonMeasurement);
                this.messageSender.sendMessage(scheduledFileTransfer, MessageType.TRANSFER_JOB_REQUEST, scheduledFileTransfer.getTransferNodeName());
                this.transferSchedulerMapService.deleteJob(scheduledFileTransfer.getJobUuid());
            }
        }
    }

    public double averageCarbonIntensity(CarbonMeasurement carbonMeasurement) {
        return carbonMeasurement.getTraceRouteCarbon().stream().mapToDouble(CarbonIpEntry::getCarbonIntensity).average().getAsDouble();
    }
}
