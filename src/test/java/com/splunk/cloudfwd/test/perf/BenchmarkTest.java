package com.splunk.cloudfwd.test.perf;

import com.splunk.cloudfwd.ConnectionSettings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

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
    private static final String CLOUDTRAIL_TOKEN_KEY = "cloudtrail_token";
    private static final String CLOUDWATCHEVENTS_TOKEN_KEY = "cloudwatchevents_token";
    private static final String VPCFLOWLOG_TOKEN_KEY = "vpcflowlog_token";
    private static final String GENERICSINGLELINE_TOKEN_KEY = "genericsingleline_token";
    private String token;

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
    
    @Test
    public void testGenericEvents() throws InterruptedException {
        sourcetype = SourcetypeEnum.GENERIC_SINGLELINE_EVENTS;
        token = cliProperties.get(GENERICSINGLELINE_TOKEN_KEY);
        eventsFilename = "./1KB_event_5MB_batch.sample";
        sendTextToRaw();
    }

    @Test
    public void testCloudTrail() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDTRAIL_UNPROCESSED;
        token = cliProperties.get(CLOUDTRAIL_TOKEN_KEY);
        eventsFilename = "./cloudtrail_via_cloudwatchevents_unprocessed.sample";
        sendTextToRaw();
    }

    @Test
    public void testCloudWatch1() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDWATCH_EVENTS_NO_VERSIONID;
        token = cliProperties.get(CLOUDWATCHEVENTS_TOKEN_KEY);
        eventsFilename = "./cloudwatchevents_awstrustedadvisor.sample";
        sendTextToRaw();
    }

    @Test
    public void testCloudWatch2() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_MIXED;
        token = cliProperties.get(CLOUDWATCHEVENTS_TOKEN_KEY);
        eventsFilename = "./cloudwatchevents_ec2autoscale.sample";
        sendTextToRaw();
    }

    @Test
    public void testCloudWatch3() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_SHORT;
        token = cliProperties.get(CLOUDWATCHEVENTS_TOKEN_KEY);
        eventsFilename = "./cloudwatchevents_codebuild.sample";
        sendTextToRaw();
    }

    @Test
    public void testCloudWatch4() throws InterruptedException {
        sourcetype = SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_LONG;
        token = cliProperties.get(CLOUDWATCHEVENTS_TOKEN_KEY);
        eventsFilename = "./cloudwatchevents_macie.sample";
        sendTextToRaw();
    }

    @Test
    public void testVpcFlowLog() throws InterruptedException {
        sourcetype = SourcetypeEnum.VPCFLOWLOG;
        token = cliProperties.get(VPCFLOWLOG_TOKEN_KEY);
        eventsFilename = "./cloudwatchlogs_vpcflowlog_lambdaprocessed.sample";
        sendTextToRaw();
    }
    
    @Override
    public void runTest() throws InterruptedException {
        //No-op - override MultiThreadedVolumeTest
    }
    
    @Override
    protected void setSenderToken(ConnectionSettings connectionSettings) {
        connectionSettings.setToken(token);
    }
    
    @Override
    protected void checkAndLogPerformance(boolean shouldAssert) {
        super.checkAndLogPerformance(shouldAssert);
        if (shouldAssert) {
            // Throughput
            float mbps = showThroughput(System.currentTimeMillis(), testStartTimeMillis);
            if (mbps != Float.NaN) {
                //TODO: MB (/8F)
                System.out.println("Sourcetype " + sourcetype + " - mbps: " + mbps + " - at time(seconds):" + ((System.currentTimeMillis() - testStartTimeMillis) / 1000));
//            Assert.assertTrue("Throughput must be above minimum value of " + sourcetypes.get(sourcetype).minMbps,
//                    mbps > sourcetypes.get(sourcetype).minMbps);
            }
            // Memory Used
            long memoryUsed = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1000000; // MB
            System.out.println("Memory(MB): " + memoryUsed);
//          Assert.assertTrue("Memory usage must be below maximum value of " + sourcetypes.get(sourcetype).minMemory + " MB",
//                memoryUsed < sourcetypes.get(sourcetype).minMemory);

            // Threads
            long threadCount = Thread.activeCount() - numSenderThreads;
            System.out.println("Sender Thread Count: " + threadCount);
//            Assert.assertTrue("Thread count must be below maximum value of " + cliProperties.get(MAX_THREADS_KEY),
//                    threadCount < Long.parseLong(cliProperties.get(MAX_THREADS_KEY)));

            // Failures
            Integer numFailed = callbacks.getFailedCount();
            Integer numSent = batchCounter.get();
            float percentFailed = ( (float) numFailed / (float) numSent ) * 100F;
            System.out.println("Percentage failed: " + percentFailed);
//            Assert.assertTrue("Percentage failed must be below 2%", percentFailed < 2F);

            LOG.info("{ }");
            /*
                [{
                    sourcetype: _,
                    runtimeMins: _,
                    ackedThroughputMBps: {
                        min: _,
                        max: _,
                        avg: _
                    },
                    memoryMB: {
                        min: _,
                        max: _,
                        avg: _
                    },
                    threadCount: {
                        min: _,
                        max: _,
                        avg: _
                    },
                    percentageFailed: {
                        min: _,
                        max: _,
                        avg: _
                    },
                    ackLatency: {
                        min: _,
                        max: _,
                        avg: _//                    }
                }, {...}, ...]
             */
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

}
