package com.onedatashare.scheduler.services;

import com.onedatashare.scheduler.model.InfluxData;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;

@Service
public class MetaDataService {

    private final RestTemplate restTemplate;
    private final String MetaDataURL = "http://MetaDataService/api/v1/";

    public MetaDataService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public List<InfluxData> getLatestNodeMeasurements(String userEmail, String nodeName, int limit) {
        String url = UriComponentsBuilder.fromHttpUrl(MetaDataURL + "/stats/influx/transfer/node/latest")
                .queryParam("userEmail", userEmail)
                .queryParam("nodeName", nodeName)
                .queryParam("limit", limit)
                .toUriString();
        ResponseEntity<List<InfluxData>> resp = this.restTemplate.exchange(url,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<InfluxData>>() {
                });
        return resp.getBody();
    }

}
