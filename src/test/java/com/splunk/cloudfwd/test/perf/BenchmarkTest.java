package com.splunk.cloudfwd.test.perf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.test.mock.ThroughputCalculatorCallback;
import org.json.simple.JSONObject;
import com.sun.management.OperatingSystemMXBean;

import org.junit.Test;

import java.io.IOException;

import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;


/**
 * Created by mhora on 1/3/18.
 */

/*
    Pre-requisites before running this test:
    1.) Make sure AWS Kinesis Add-On is installed on all nodes in cluster
    2.) Create 3 tokens - one for each sourcetype. Create 3 indexes, and associate one with each token.
    3.) Call BenchmarkTest and pass 3 tokens as cloudtrail_token, cloudwatchevents_token, and vpcflowlog_token
 */
public class BenchmarkTest extends MultiThreadedVolumeTest {
    
    // Configurable options
    private SourcetypeEnum sourcetype;
    private int batchSizeMB = 5; // TODO: make configurable?
    private enum SourcetypeEnum {
        GENERIC_SINGLELINE_EVENTS,
        CLOUDTRAIL_UNPROCESSED,
        CLOUDWATCH_EVENTS_NO_VERSIONID,
        CLOUDWATCH_EVENTS_VERSIONID_MIXED,
        CLOUDWATCH_EVENTS_VERSIONID_SHORT,
        CLOUDWATCH_EVENTS_VERSIONID_LONG,
        VPCFLOWLOG
    }
//    private static final String CLOUDTRAIL_TOKEN_KEY = "cloudtrail_token";
//    private static final String CLOUDWATCHEVENTS_TOKEN_KEY = "cloudwatchevents_token";
//    private static final String VPCFLOWLOG_TOKEN_KEY = "vpcflowlog_token";
//    private static final String GENERICSINGLELINE_TOKEN_KEY = "genericsingleline_token";
    private String token;
    private JSONObject jsonReport;

    private Float ackedThroughputMin = null;
    private float ackedThroughputMax = 0;
    private float ackedThroughputTotal = 0;

    private Long memoryMin = null;
    private long memoryMax = 0;
    private long memoryTotal = 0;

    private Long threadMin = null;
    private long threadMax = 0;
    private long threadTotal = 0;

    private Float failedMin = null;
    private float failedMax = 0;
    private float failedTotal = 0;

    private Double latencyMin = null;
    private double latencyMax = 0;

    private long runtimeSecs;
    private long metricsStartTimeMillis = testStartTimeMillis - warmUpTimeMillis;

    private Double cpuUsageMin = null;
    private double cpuUsageMax = 0;
    private double cpuUsageTotal = 0;

    private int numofLines = numOfLines();


    private long eventsSent = 0;

    private long callCount = 0;
/*
    static {
//        cliProperties.put("num_senders", "40"); // Low default sender count due to java.lang.OutOfMemoryError: GC overhead limit exceeded on local.
        cliProperties.put(GENERICSINGLELINE_TOKEN_KEY, null);
        cliProperties.put(CLOUDTRAIL_TOKEN_KEY, null);
        cliProperties.put(CLOUDWATCHEVENTS_TOKEN_KEY, null);
        cliProperties.put(VPCFLOWLOG_TOKEN_KEY, null);
    }

    private HashMap<SourcetypeEnum, Sourcetype> sourcetypes;
    
    private static final int MIN_MBPS = 50; //FIXME placeholder - collect baseline metric from initial test run
    private static final int MAX_MEMORY_MB = 1024; //FIXME placeholder - collect baseline metric from initial test run
    
    class Sourcetype {
        String filepath;
        String token;
        int minMbps;
        int minMemoryMb;
        
        private Sourcetype(String filepath, String token, int minMbps, int minMemoryMb) {
            this.filepath = filepath;
            this.token = token;
            this.minMbps = minMbps;
            this.minMemoryMb = minMemoryMb;
        }
    }
*/

    @Test
    public void testGenericEvents() throws InterruptedException {
        sourcetype = SourcetypeEnum.GENERIC_SINGLELINE_EVENTS;
//        token = cliProperties.get(GENERICSINGLELINE_TOKEN_KEY);
        eventsFilename = "./events_from_aws_5MB_batch.sample";
        sendTextToRaw();
        printReport(sourcetype);
    }

    @Test
    public void testCloudTrail() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDTRAIL_UNPROCESSED;
//        token = cliProperties.get(CLOUDTRAIL_TOKEN_KEY);
        eventsFilename = "./cloudtrail_via_cloudwatchevents_unprocessed.sample";
        sendTextToRaw();
        printReport(sourcetype);
    }


    @Test
    public void testCloudWatch1() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDWATCH_EVENTS_NO_VERSIONID;
//        token = cliProperties.get(CLOUDWATCHEVENTS_TOKEN_KEY);
        eventsFilename = "./cloudwatchevents_awstrustedadvisor.sample";
        sendTextToRaw();
        printReport(sourcetype);
    }

    @Test
    public void testCloudWatch2() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_MIXED;
//        token = cliProperties.get(CLOUDWATCHEVENTS_TOKEN_KEY);
        eventsFilename = "./cloudwatchevents_ec2autoscale.sample";
        sendTextToRaw();
        printReport(sourcetype);
    }

    @Test
    public void testCloudWatch3() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_SHORT;
//        token = cliProperties.get(CLOUDWATCHEVENTS_TOKEN_KEY);
        eventsFilename = "./cloudwatchevents_codebuild.sample";
        sendTextToRaw();
        printReport(sourcetype);
    }

    @Test
    public void testCloudWatch4() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_LONG;
//        token = cliProperties.get(CLOUDWATCHEVENTS_TOKEN_KEY);
        eventsFilename = "./cloudwatchevents_macie.sample";
        sendTextToRaw();
        printReport(sourcetype);
    }

    @Test
    public void testVpcFlowLog() throws InterruptedException {
        sourcetype = SourcetypeEnum.VPCFLOWLOG;
//        token = cliProperties.get(VPCFLOWLOG_TOKEN_KEY);
        eventsFilename = "./cloudwatchlogs_vpcflowlog_lambdaprocessed.sample";
        sendTextToRaw();
        printReport(sourcetype);
    }
    
    @Override
    public void runTest() throws InterruptedException {
        //No-op - override MultiThreadedVolumeTest
    }


//    @Override
    protected void setSenderToken(ConnectionSettings connectionSettings) {
        connectionSettings.setToken(token);
    }

    @Override
    protected void checkAndLogPerformance(boolean shouldAssert) {
        super.checkAndLogPerformance(shouldAssert);
       if (shouldAssert) {
            callCount++;

            // Throughput
            int numAckedBatches = callbacks.getAcknowledgedBatches().size();
            long elapsedSeconds = (System.currentTimeMillis() - testStartTimeMillis) / 1000;
           float mbps = showThroughput(System.currentTimeMillis(),testStartTimeMillis)/8;
//            float mbps = (float) batchSizeMB * (float) numAckedBatches / (float) elapsedSeconds;
            if (mbps != Float.NaN) {
//                System.out.println("Sourcetype " + sourcetype + " - MBps: " + (mbps / 8F) + " - at time(seconds):" + ((System.currentTimeMillis() - testStartTimeMillis) / 1000));
//            Assert.assertTrue("Throughput must be above minimum value of " + sourcetypes.get(sourcetype).minMbps,
//                    mbps > sourcetypes.get(sourcetype).minMbps);
                if (ackedThroughputMin == null || ackedThroughputMin.floatValue() > mbps) {
                    ackedThroughputMin = mbps;
                }
                if (ackedThroughputMax < mbps) {
                    ackedThroughputMax = mbps;
                }
                ackedThroughputTotal += mbps;
            }
            // Memory Used
            long memoryUsed = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1000000; // MB
//            System.out.println("Memory(MB): " + memoryUsed);
//          Assert.assertTrue("Memory usage must be below maximum value of " + sourcetypes.get(sourcetype).minMemory + " MB",
//                memoryUsed < sourcetypes.get(sourcetype).minMemory);
            if (memoryMin == null || memoryMin.longValue() > memoryUsed) {
                memoryMin = memoryUsed;
            }
            if (memoryMax < memoryUsed) {
                memoryMax = memoryUsed;
            }
            memoryTotal += memoryUsed;

            // Threads
            long threadCount = Thread.activeCount() - numSenderThreads;
//            System.out.println("Sender Thread Count: " + threadCount);
//            Assert.assertTrue("Thread count must be below maximum value of " + cliProperties.get(MAX_THREADS_KEY),
//                    threadCount < Long.parseLong(cliProperties.get(MAX_THREADS_KEY)));
            if (threadMin == null || threadMin.longValue() > threadCount) {
                threadMin = threadCount;
            }
            if (threadMax < threadCount) {
                threadMax = threadCount;
            }
            threadTotal += threadCount;

            // Failures
            Integer numFailed = callbacks.getFailedCount();
            Integer numSent = batchCounter.get();
            float percentFailed = ( (float) numFailed / (float) numSent ) * 100F;
//            System.out.println("Percentage failed: " + percentFailed);
//            Assert.assertTrue("Percentage failed must be below 2%", percentFailed < 2F);
            if (failedMin == null || failedMin.floatValue() > percentFailed) {
                failedMin = percentFailed;
            }
            if (failedMax < percentFailed) {
                failedMax = percentFailed;
            }
            failedTotal += percentFailed;

            // Ack Latency
            double lastLatencySec = ((ThroughputCalculatorCallback) callbacks).getLastLatency() / 1000;
//            System.out.println("Last ack Latency: " + lastLatencySec + " seconds");
            if (latencyMin == null || latencyMin.doubleValue() > lastLatencySec) {
                latencyMin = lastLatencySec;
            }
            if (latencyMax < lastLatencySec) {
                latencyMax = lastLatencySec;
            }

            OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
            double lastCpuUsage = operatingSystemMXBean.getProcessCpuLoad();

            if(cpuUsageMin == null || cpuUsageMin.doubleValue() > lastCpuUsage) {
                cpuUsageMin = lastCpuUsage;
            }
            if(cpuUsageMax < lastCpuUsage){
                cpuUsageMax = lastCpuUsage;
            }
            cpuUsageTotal += lastCpuUsage;


            double avgLatencySec = ((ThroughputCalculatorCallback) callbacks).getAvgLatency() / 1000;
//            System.out.println("Average ack Latency: " + avgLatencySec + " seconds");

       }
    }

    private int numOfLines(){
        URL resource = getClass().getClassLoader().getResource(eventsFilename); // to use a file on classpath in resources folder.
        try{
            List<String> lines = Files.readAllLines(Paths.get(resource.getFile()));
            numofLines = lines.size();
        }catch (Exception e){
            e.printStackTrace();
        }
        return numofLines;
    }

    private void createReport(SourcetypeEnum sourcetype) {
        runtimeSecs = ((System.currentTimeMillis() - metricsStartTimeMillis) / 1000);
        jsonReport = new JSONObject();
        jsonReport.put("runtimeMins", (runtimeSecs / 60));
        jsonReport.put("ackedThroughoutMBps", createAckedThroughputStats());
        jsonReport.put("memoryMB", createMemoryStats());
        jsonReport.put("threadCount", createThreadStats());
        jsonReport.put("percentageFailed", createFailedStats());
        jsonReport.put("ackLatencySecs", createLatencyStats());
        jsonReport.put("cpuUsage %",createCpuUsageStats());
        jsonReport.put("Events per batch", numOfLines());
        jsonReport.put("Number of events sent",((batchCounter.get()) * numofLines) / ((System.currentTimeMillis() - testStartTimeMillis)/1000));
    }

    private HashMap createAckedThroughputStats() {
        HashMap<String, Float> statsObject = new HashMap<>();
        statsObject.put("min", ackedThroughputMin);
        statsObject.put("max", ackedThroughputMax);
        statsObject.put("avg", ackedThroughputTotal / callCount);
        return statsObject;
    }

    private HashMap createMemoryStats() {
        HashMap<String, Long> statsObject = new HashMap<>();
        statsObject.put("min", memoryMin);
        statsObject.put("max", memoryMax);
        statsObject.put("avg", memoryTotal / callCount);
        return statsObject;
    }

    private HashMap createThreadStats() {
        HashMap<String, Long> statsObject = new HashMap<>();
        statsObject.put("min", threadMin);
        statsObject.put("max", threadMax);
        statsObject.put("avg", threadTotal / callCount);
        return statsObject;
    }

    private HashMap createFailedStats() {
        HashMap<String, Float> statsObject = new HashMap<>();
        statsObject.put("min", failedMin);
        statsObject.put("max", failedMax);
        statsObject.put("avg", failedTotal / callCount);
        return statsObject;
    }

    private HashMap createLatencyStats() {
        HashMap<String, Double> statsObject = new HashMap<>();
        statsObject.put("min", latencyMin);
        statsObject.put("max", latencyMax);
        statsObject.put("avg", ((ThroughputCalculatorCallback) callbacks).getLastLatency() / 1000);
        return statsObject;
    }

    private HashMap createCpuUsageStats(){
        HashMap<String, Double> statsObject = new HashMap<>();
        statsObject.put("min",cpuUsageMin);
        statsObject.put("max",cpuUsageMax);
        statsObject.put("avg",cpuUsageTotal / callCount);
        return statsObject;
    }


    private void printReport(SourcetypeEnum sourcetype) {
        createReport(sourcetype);

        System.out.println("\nSOURCETYPE: " + sourcetype);

        // Pretty print JSON
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonReport));
        } catch (IOException e) {
            System.out.println("Error pretty printing metrics JSON");
        }

    }
/*
    @Override
    protected void updateTimestampsOnBatch() {
        String byte_str = new String(buffer.array());
        // Convert time stamp based on source type
        if (sourcetype.equals(SourcetypeEnum.CLOUDTRAIL_UNPROCESSED)) {
            byte_str = byte_str.replaceAll("\"eventTime\":\\s?\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z","\"eventTime\" : \"" +
                    new SimpleDateFormat("YYYY-MM-DD'T'hh:mm:ss'Z'").format(new Date()));
        } else if( sourcetype.equals(SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_SHORT)||
                sourcetype.equals(SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_LONG) ||
                sourcetype.equals(SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_MIXED)) {
            byte_str = byte_str.replaceAll("\"time\":\\s?\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z", "\"time\" : \"" +
                    new SimpleDateFormat("YYYY-MM-DD'T'hh:mm:ss'Z'").format(new Date()));
        }
        // Repack buffer
        byte[] bytes = byte_str.getBytes();
        buffer = ByteBuffer.wrap(bytes);

    }
*/
/*
    @Override
    protected Connection createAndConfigureConnection(){
        ConnectionSettings settings = getTestProps();
        configureProps(settings);
        if(token != null)
            settings.setToken(token);
        connection = createConnection(callbacks, settings);
        if(null == connection){
            return null;
        }
        configureConnection(connection);
        return connection;
    }



    @Override
    protected void configureProps(ConnectionSettings settings) {
        super.configureProps(settings);
        if(token != null)
            setSenderToken(settings);
        String url = System.getProperty(PropertyKeys.COLLECTOR_URI);
        if (System.getProperty(PropertyKeys.TOKEN) != null) {
            settings.setToken(token);
        }
        if (System.getProperty(PropertyKeys.COLLECTOR_URI) != null) {
            settings.setUrls(url);
        }
        settings.setMockHttp(false);
        settings.setTestPropertiesEnabled(false);
    }
*/
}
