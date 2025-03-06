package com.onedatashare.scheduler.model;

import lombok.Data;

@Data
public class InfluxData {

    private String networkInterface;

    private String odsUser;

    private String transferNodeName;

    private Long coreCount;

    private Double cpu_frequency_max;

    private Double cpu_frequency_current;

    private Double cpu_frequency_min;

    private String cpuArchitecture;

    private Double packetLossRate;
    //NIC values
    private Long bytesSent;

    private Long bytesReceived;

    private Long packetSent;

    private Long packetReceived;

    private Long dropin;

    private Long dropout;
    private Long nicMtu;

    private Double latency;

    private Double rtt;

    private Double sourceRtt;

    private Double sourceLatency;

    private Double destinationRtt;

    private Double destLatency;

    private Long errin;

    private Long errout;

    //Job Values
    private String jobId;
    private Double readThroughput;
    private Double writeThroughput;
    private Long bytesWritten;
    private Long bytesRead;
    private Long concurrency;

    private Long parallelism;
    private Long pipelining;
    private Long memory;
    private Long maxMemory;
    private Long freeMemory;
    private Long allocatedMemory;
    private Long jobSize;
    private Long avgFileSize;

    private String sourceType;
    private String sourceCredId;

    private String destType;
    private String destCredId;
}
