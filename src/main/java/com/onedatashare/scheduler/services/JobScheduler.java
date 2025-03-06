package com.onedatashare.scheduler.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.onedatashare.scheduler.enums.EndPointType;
import com.onedatashare.scheduler.enums.MessageType;
import com.onedatashare.scheduler.model.FileTransferNodeMetaData;
import com.onedatashare.scheduler.model.RequestFromODSDTO;
import com.onedatashare.scheduler.model.TransferJobRequest;
import com.onedatashare.scheduler.services.maps.TransferSchedulerMapService;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.UUID;

@Service
public class JobScheduler {

    private final FileTransferNodeDiscovery fileTransferNodeDiscovery;
    protected TransferSchedulerMapService fileTransferScheduleMap; // the map that stores the scheduled jobs

    RequestModifier requestModifier;

    MessageSender messageSender;


    public JobScheduler(FileTransferNodeDiscovery fileTransferNodeDiscovery, TransferSchedulerMapService transferSchedulerMapService, RequestModifier requestModifier, MessageSender messageSender) {
        this.requestModifier = requestModifier;
        this.messageSender = messageSender;
        this.fileTransferScheduleMap = transferSchedulerMapService;
        this.fileTransferNodeDiscovery = fileTransferNodeDiscovery;
    }

    public Collection<TransferJobRequest> listScheduledJobs(String userEmail) throws JsonProcessingException {
        return this.fileTransferScheduleMap.queryUserScheduledJobs(userEmail);
    }

    public TransferJobRequest getScheduledJobDetails(UUID jobUuid) throws JsonProcessingException {
        return this.fileTransferScheduleMap.queryJob(jobUuid);
    }

    public UUID saveScheduledJob(RequestFromODSDTO transferRequest, LocalDateTime jobStartTime) throws JsonProcessingException {
        if (transferRequest == null) {
            return null;
        }
        UUID jobUuid = UUID.randomUUID();
        TransferJobRequest transferJobRequest = this.requestModifier.createRequest(transferRequest, jobUuid);
        //decide if the node to use is an ODS Connector= Vfs= Virtual file system
        boolean sourceVfs = transferRequest.getSource().getType().equals(EndPointType.vfs);
        boolean destVfs = transferRequest.getDestination().getType().equals(EndPointType.vfs);
        if (sourceVfs) {
            transferJobRequest.setTransferNodeName(transferRequest.getSource().getCredId());
        } else if (destVfs) {
            transferJobRequest.setTransferNodeName(transferRequest.getDestination().getCredId());
        }
        this.fileTransferScheduleMap.putJob(transferJobRequest, jobStartTime);

        return jobUuid;
    }

    public void deleteScheduledJob(UUID jobUuid) {
        this.fileTransferScheduleMap.deleteJob(jobUuid);
    }

    public void stopJob(UUID jobUuid) throws JsonProcessingException, InterruptedException {
        if (this.fileTransferScheduleMap.containsKey(jobUuid)) {
            this.fileTransferScheduleMap.deleteJob(jobUuid);
        } else {
            FileTransferNodeMetaData fileTransferNodeMetaData = this.fileTransferNodeDiscovery.getNodeRunningJob(jobUuid);
            messageSender.sendMessage(null, MessageType.STOP_JOB, fileTransferNodeMetaData.getNodeName());
        }
    }
}
