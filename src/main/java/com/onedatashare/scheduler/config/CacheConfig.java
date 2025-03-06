package com.onedatashare.scheduler.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.transport.jersey.TransportClientFactories;
import com.netflix.discovery.shared.transport.jersey3.Jersey3TransportClientFactories;
import com.onedatashare.scheduler.model.CarbonIntensityMapKey;
import com.onedatashare.scheduler.model.carbon.CarbonIpEntry;
import com.onedatashare.scheduler.services.VaultSSLService;
import com.onedatashare.scheduler.services.listeners.FileTransferNodeEventListener;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;


@Configuration
public class CacheConfig {

    private final Environment env;
    private final Logger logger;

    public CacheConfig(Environment environment) {
        this.env = environment;
        this.logger = LoggerFactory.getLogger(CacheConfig.class);
    }

    @Value("${hazelcast.enterprise.license}")
    String hazelcastLicenseKey;

    @SneakyThrows
    @Bean(name = "hazelcastInstance")
    @Profile("prod")
    public HazelcastInstance prodHazelcastInstance(EurekaClient eurekaClient, SSLConfig sslConfig) {
        while (eurekaClient.getApplications().getRegisteredApplications().isEmpty()) {
            Thread.sleep(1000); // Wait until Eureka has registered nodes
        }
        Config config = new Config();
        config.setClusterName("prod-scheduler-cluster");
        config.setLicenseKey(this.hazelcastLicenseKey);
        config.getNetworkConfig().setPortAutoIncrement(true);
        config.getNetworkConfig().setSSLConfig(sslConfig);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);


        logger.info(this.env.getProperty("eureka.client.serviceUrl.defaultZone"));
        config.getNetworkConfig().getJoin().getEurekaConfig()
                .setEnabled(true)
                .setProperty("self-registration", "true")
                .setProperty("namespace", "hazelcast")
                .setProperty("name", "hazelcast-prod")
                .setProperty("shouldUseDns", "false")
                .setProperty("serviceUrl.default", this.env.getProperty("eureka.client.serviceUrl.defaultZone"))
                .setProperty("use-classpath-eureka-client-props", "false");

        return Hazelcast.newHazelcastInstance(config);
    }

    @Bean(name = "hazelcastInstance")
    @Profile("dev")
    public HazelcastInstance devHazelcastInstance() {
        Config config = new Config();
        config.setClusterName("dev-scheduler-cluster");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().setPortAutoIncrement(true);
        return Hazelcast.newHazelcastInstance(config);
    }

    @Bean
    @Profile("prod")
    public SSLConfig sslConfig(VaultSSLService vaultSslService) {
        Properties properties = new Properties();
        properties.setProperty("protocol", "TLSv1.2");
        properties.setProperty("mutualAuthentication", "OPTIONAL");
        properties.setProperty("keyMaterialDuration", vaultSslService.getStoreDuration().toString());
        properties.setProperty("validateIdentity", "false");

        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setEnabled(true);
        sslConfig.setProperties(properties);
        sslConfig.setFactoryImplementation(vaultSslService);

        return sslConfig;
    }

    @Bean
    public IMap<CarbonIntensityMapKey, List<CarbonIpEntry>> historicalTransferJobMeasurements(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap("historical-transfer-job-measurements");
    }

    @Bean
    public IMap<String, HazelcastJsonValue> fileTransferNodeMap(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance, ObjectMapper objectMapper) {
        IMap<String, HazelcastJsonValue> fileNodeMap = hazelcastInstance.getMap("file-transfer-node-map");
        Map<String, LocalIndexStats> index = fileNodeMap.getLocalMapStats().getIndexStats();
        if (!index.containsKey("odsOwner")) fileNodeMap.addIndex(IndexType.HASH, "odsOwner");
        if (!index.containsKey("nodeName")) fileNodeMap.addIndex(IndexType.HASH, "nodeName");
        if (!index.containsKey("runningJob")) fileNodeMap.addIndex(IndexType.HASH, "runningJob");
        if (!index.containsKey("online")) fileNodeMap.addIndex(IndexType.HASH, "online");
        fileNodeMap.addEntryListener(new FileTransferNodeEventListener(hazelcastInstance, objectMapper), true);
        return fileNodeMap;
    }

    @Bean
    public IMap<UUID, HazelcastJsonValue> carbonIntensityMap(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        IMap<UUID, HazelcastJsonValue> carbonMap = hazelcastInstance.getMap("carbon-intensity-map");
        Map<String, LocalIndexStats> indexMap = carbonMap.getLocalMapStats().getIndexStats();
        if (!indexMap.containsKey("ownerId")) carbonMap.addIndex(IndexType.HASH, "ownerId");
        if (!indexMap.containsKey("transferNodeName")) carbonMap.addIndex(IndexType.HASH, "transferNodeName");
        if (!indexMap.containsKey("jobUuid")) carbonMap.addIndex(IndexType.HASH, "jobUuid");
        return carbonMap;
    }

    @Bean
    public IScheduledExecutorService jobSchedulerExecutorService(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getScheduledExecutorService("job-scheduler-executor-service");
    }

    @Bean
    public TransportClientFactories transportClientFactories() {
        return Jersey3TransportClientFactories.getInstance();
    }

    @Profile("prod")
    @LoadBalanced
    @Bean("restTemplate")
    public RestTemplate prodRestTemplate() {
        return new RestTemplate();
    }

    @Profile("dev")
    @Bean("restTemplate")
    public RestTemplate devRestTemplate() {
        return new RestTemplate();
    }

}
