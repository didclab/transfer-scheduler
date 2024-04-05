package com.onedatashare.scheduler.services;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.onedatashare.scheduler.enums.EndPointType;
import com.onedatashare.scheduler.model.RequestFromODS;
import com.onedatashare.scheduler.model.RequestFromODSDTO;
import com.onedatashare.scheduler.model.TransferJobRequest;
import com.onedatashare.scheduler.model.carbon.CarbonIpEntry;
import com.onedatashare.scheduler.model.carbon.CarbonMeasureRequest;
import com.onedatashare.scheduler.model.credential.EndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class JobScheduler {

    protected IMap<UUID, RequestFromODS> jobIMap; // the map that stores the scheduled jobs
    public IMap<UUID, List<CarbonIpEntry>> currentJobIdToCarbonIntensity; //scheduling in time is abusing this map.

    private EntryExpiredHazelcast entryExpiredHazelcast;
    Logger logger = LoggerFactory.getLogger(JobScheduler.class);

    @Value("${ods.rabbitmq.routingkey}")
    private String routingKey;

    @Autowired
    CarbonRpcService carbonRpcService;

    @Autowired
    CredentialService credentialService;

    RequestModifier requestModifier;

    MessageSender messageSender;


    public JobScheduler(HazelcastInstance hazelcastInstance, RequestModifier requestModifier, MessageSender messageSender) {
        this.jobIMap = hazelcastInstance.getMap("scheduled_jobs");
        this.currentJobIdToCarbonIntensity = hazelcastInstance.getMap("currentJobIdToCarbonIntensity");
        this.entryExpiredHazelcast = new EntryExpiredHazelcast(requestModifier, messageSender);
        this.requestModifier = requestModifier;
        this.messageSender = messageSender;
    }

    public Collection<RequestFromODS> listScheduledJobs(String userEmail) {
        PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate<UUID, RequestFromODS> predicate = entry -> entry.getValue().getOwnerId().equals(userEmail);
        return this.jobIMap.values(predicate);
    }

    public RequestFromODS getScheduledJobDetails(UUID jobUuid) {
        return this.jobIMap.get(jobUuid);
    }

    public boolean deleteScheduledJob(UUID jobUuid) {
        if (this.jobIMap.containsKey(jobUuid)) {
            this.jobIMap.delete(jobUuid);
            return true;
        } else {
            return false;
        }
    }

    public UUID saveScheduledJob(RequestFromODSDTO transferRequest, LocalDateTime jobStartTime) {
        if (transferRequest == null) {
            return null;
        }
        UUID id = UUID.randomUUID();
        RequestFromODS transferJob = new RequestFromODS();
        transferJob.setOptions(transferRequest.getOptions());
        transferJob.setDestination(transferRequest.getDestination());
        transferJob.setSource(transferRequest.getSource());
        transferJob.setOwnerId(transferRequest.getOwnerId());
        transferJob.setJobUuid(id);
        transferJob.setJobStartTime(jobStartTime);
        //assign the job to a node: either the user tells us which, its an ODS Connector(vfs) or we just use one of the ODS running nodes(routing key)
        boolean sourceVfs = transferRequest.getSource().getType().equals(EndPointType.vfs);
        boolean destVfs = transferRequest.getDestination().getType().equals(EndPointType.vfs);
        if (transferRequest.getTransferNodeName() != null || !transferRequest.getTransferNodeName().isEmpty()) {
            transferJob.setTransferNodeName(transferRequest.getTransferNodeName());
        } else if (sourceVfs) {
            transferJob.setTransferNodeName(transferRequest.getSource().getCredId());
        } else if (destVfs) {
            transferJob.setTransferNodeName(transferRequest.getDestination().getCredId());
        } else {
            transferJob.setTransferNodeName(routingKey);
        }

        Instant currentDate = Instant.now();
        long delay = Duration.between(LocalDateTime.now(), jobStartTime).getSeconds();
        logger.info("Now: {} \t jobStartTime: {} \t delay: {}", currentDate, jobStartTime, delay);
        if (delay < 0) delay = 1;

        List<CarbonIpEntry> traceRoute = this.measureCarbonForJob(transferJob, id);
        this.jobIMap.put(id, transferJob, delay, TimeUnit.SECONDS);
        this.jobIMap.addEntryListener(this.entryExpiredHazelcast, id, true);
        this.currentJobIdToCarbonIntensity.put(id, traceRoute);
        return id;
    }

    @Scheduled(cron = "* */30 * * *")
    public void measureCarbonForQueuedJobs() {
        for (UUID jobId : this.jobIMap.keySet()) {
            RequestFromODS transferJob = this.jobIMap.get(jobId);
            List<CarbonIpEntry> traceRoute = this.measureCarbonForJob(transferJob, jobId);
            Double currentIntensity = this.carbonRpcService.averageCarbonIntensityOfTraceRoute(traceRoute);
            Double pastIntensity = this.carbonRpcService.averageCarbonIntensityOfTraceRoute(this.currentJobIdToCarbonIntensity.get(jobId));
            this.currentJobIdToCarbonIntensity.put(jobId, traceRoute);

            boolean runNow = this.runJobCarbonSla(pastIntensity, currentIntensity, Double.valueOf(transferJob.getOptions().getUserDesiredCarbonIntensity()));
            if (runNow) {
                this.jobIMap.remove(jobId);
                this.currentJobIdToCarbonIntensity.remove(jobId);
                TransferJobRequest transferJobRequest = requestModifier.createRequest(transferJob);
                messageSender.sendTransferRequest(transferJobRequest);
            }
        }
    }

    public List<CarbonIpEntry> measureCarbonForJob(RequestFromODS requestFromODS, UUID jobUuid) {
        EndpointCredential sourceCred;
        EndpointCredential destCred;
        if (RequestModifier.vfsCredType.contains(requestFromODS.getSource().getType().toString())) {
            sourceCred = credentialService.fetchAccountCredential(requestFromODS.getSource().getType().toString(), requestFromODS.getOwnerId(), requestFromODS.getSource().getCredId());
        } else {
            sourceCred = credentialService.fetchOAuthCredential(requestFromODS.getSource().getType(), requestFromODS.getOwnerId(), requestFromODS.getSource().getCredId());
        }
        if (RequestModifier.vfsCredType.contains(requestFromODS.getDestination().getType().toString())) {
            destCred = credentialService.fetchAccountCredential(requestFromODS.getDestination().getType().toString(), requestFromODS.getOwnerId(), requestFromODS.getDestination().getCredId());
        } else {
            destCred = credentialService.fetchOAuthCredential(requestFromODS.getDestination().getType(), requestFromODS.getOwnerId(), requestFromODS.getDestination().getCredId());
        }
        String sourceUri = ODSConstants.uriFromEndpointCredential(sourceCred, requestFromODS.getSource().getType());
        String destUri = ODSConstants.uriFromEndpointCredential(destCred, requestFromODS.getDestination().getType());
        return this.carbonRpcService.traceRoute(new CarbonMeasureRequest(requestFromODS.getTransferNodeName(), sourceUri, destUri));
    }

    public boolean runJobCarbonSla(Double pastSla, Double currentSla, Double userSla) {
        if (userSla == -1) {
            return currentSla < pastSla;
        } else {
            return currentSla <= userSla;
        }
    }

    public Double getCarbonIntensityOfJob(UUID jobUuid) {
        return this.carbonRpcService.averageCarbonIntensityOfTraceRoute(this.currentJobIdToCarbonIntensity.get(jobUuid));
    }

    public List<CarbonIpEntry> getTraceRoute(UUID jobUuid){
        return this.currentJobIdToCarbonIntensity.get(jobUuid);
    }
}
