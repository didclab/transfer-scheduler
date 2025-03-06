package com.onedatashare.scheduler.services;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.onedatashare.scheduler.model.EntityInfo;
import com.onedatashare.scheduler.model.Group;
import com.onedatashare.scheduler.model.InfluxData;
import com.onedatashare.scheduler.model.TransferJobRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
class JobAndNodeClassificationTest {

    HazelcastInstance hazelcastInstance;

    MetaDataService metaDataService = Mockito.mock(MetaDataService.class);

    @InjectMocks
    private JobAndNodeClassification jobAndNodeClassification;

    public JobAndNodeClassificationTest() {
        Config config = new Config();
        String key = System.getenv("HAZELCAST_LICENSE_KEY");
        config.setLicenseKey(key);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        this.jobAndNodeClassification = new JobAndNodeClassification(hazelcastInstance, this.metaDataService);

    }

    @Test
    public void testJobClassificationDefaultShouldBeSmall() {
        TransferJobRequest.Source smallSource = new TransferJobRequest.Source();
        smallSource.setInfoList(new ArrayList<>());
        UUID smallJobuuid = UUID.randomUUID();
        Group group = jobAndNodeClassification.classifyJobGroup(smallSource, smallJobuuid);
        Assertions.assertEquals(Group.SMALL, group);
        Assertions.assertTrue(this.jobAndNodeClassification.jobCategory.containsKey(smallJobuuid));
        Assertions.assertEquals(Group.SMALL, this.jobAndNodeClassification.jobCategory.get(smallJobuuid));
    }

    @Test
    public void testJobClassificationLargeFileTransfer() {
        TransferJobRequest.Source largeSource = new TransferJobRequest.Source();
        ArrayList<EntityInfo> files = new ArrayList<>();
        EntityInfo entityInfo = new EntityInfo();
        entityInfo.setId("");
        entityInfo.setSize(150000000000L);
        files.add(entityInfo);
        largeSource.setInfoList(files);
        UUID largeJobuuid = UUID.randomUUID();
        Group group = this.jobAndNodeClassification.classifyJobGroup(largeSource, largeJobuuid);
        Assertions.assertEquals(Group.LARGE, group);
        Assertions.assertTrue(this.jobAndNodeClassification.jobCategory.containsKey(largeJobuuid));
        Assertions.assertEquals(Group.LARGE, this.jobAndNodeClassification.jobCategory.get(largeJobuuid));
    }

    @Test
    public void testJobClassificationMediumFileTransfer() {
        TransferJobRequest.Source largeSource = new TransferJobRequest.Source();
        ArrayList<EntityInfo> files = new ArrayList<>();
        EntityInfo entityInfo = new EntityInfo();
        entityInfo.setId("");
        entityInfo.setSize(15000000000L);
        files.add(entityInfo);
        largeSource.setInfoList(files);
        UUID largeJobuuid = UUID.randomUUID();
        Group group = this.jobAndNodeClassification.classifyJobGroup(largeSource, largeJobuuid);
        Assertions.assertEquals(Group.MEDIUM, group);
        Assertions.assertTrue(this.jobAndNodeClassification.jobCategory.containsKey(largeJobuuid));
        Assertions.assertEquals(Group.MEDIUM, this.jobAndNodeClassification.jobCategory.get(largeJobuuid));
    }

    @Test
    public void testJobClassificationSmallNode() {
        Mockito.when(this.metaDataService.getLatestNodeMeasurements("", "", 10)).thenReturn(new ArrayList<>());
        Group group = this.jobAndNodeClassification.classifyThroughput("", "", 10);
        Assertions.assertEquals(Group.SMALL, group);
        List<InfluxData> influxDatas = this.influxDataSet(100_000_000.0, 0.0);
        Mockito.when(this.metaDataService.getLatestNodeMeasurements("", "", 10)).thenReturn(influxDatas);
        group = this.jobAndNodeClassification.classifyThroughput("", "", 1);
        Assertions.assertEquals(Group.SMALL, group);
    }

    @Test
    public void testJobClassificationMediumNode() {
        List<InfluxData> influxDatas = this.influxDataSet(1_000_000_000_000.0, 0.0);
        Mockito.when(this.metaDataService.getLatestNodeMeasurements("", "", 10)).thenReturn(influxDatas);
        Group group = this.jobAndNodeClassification.classifyThroughput("", "", 1);
        Assertions.assertEquals(Group.MEDIUM, group);
    }

    @Test
    public void testJobClassificationLargeNode() {
        List<InfluxData> influxDatas = this.influxDataSet(100000000000.0, 100000000000.0);
        Mockito.when(this.metaDataService.getLatestNodeMeasurements("", "", 10)).thenReturn(influxDatas);
        Group group = this.jobAndNodeClassification.classifyThroughput("", "", 10);
        Assertions.assertEquals(Group.LARGE, group);
    }

    public List<InfluxData> influxDataSet(double readThrpt, double writeThrpt) {
        ArrayList<InfluxData> influxDatas = new ArrayList<>();
        InfluxData influxData = new InfluxData();
        influxData.setReadThroughput(readThrpt);
        influxData.setWriteThroughput(writeThrpt);
        influxDatas.add(influxData);
        return influxDatas;
    }


}