package com.splunk.cloudfwd.test.perf;

import com.splunk.cloudfwd.ConnectionSettings;
import org.junit.Assert;
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
    
    // Configurable options //TODO: make this configurable through CLI
    private SourcetypeEnum sourcetype;
    private int batchSizeMB = 5;
    private enum SourcetypeEnum {
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
    private String cloudTrailToken;
    private String cloudWatchEventsToken;
    private String vpcFlowLogToken;

    static {
//        cliProperties.put("num_senders", "40"); // Low default sender count due to java.lang.OutOfMemoryError: GC overhead limit exceeded on local.
        cliProperties.put(CLOUDTRAIL_TOKEN_KEY, null);
        cliProperties.put(CLOUDWATCHEVENTS_TOKEN_KEY, null);
        cliProperties.put(VPCFLOWLOG_TOKEN_KEY, null);
    }
    
    HashMap<SourcetypeEnum, Sourcetype> sourcetypes = new HashMap();
    
    private static final int MIN_MBPS = 50; //FIXME placeholder - collect baseline metric from initial test run
    private static final int MAX_MEMORY_MB = 2000; //FIXME placeholder - collect baseline metric from initial test run
    
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
    
    private void setupSourcetypes() {
        cloudTrailToken = "2F2D2DE9-E023-42D0-80A0-ED5A58B4DC49";//cliProperties.get(CLOUDTRAIL_TOKEN_KEY);
        cloudWatchEventsToken = "A6A3E414-CBBA-498B-BBED-9A5A720E79EE";//cliProperties.get(CLOUDWATCHEVENTS_TOKEN_KEY);
        vpcFlowLogToken = "18ABC6A4-0BCE-4FC5-ACD0-3ADF6997F50A";//cliProperties.get(VPCFLOWLOG_TOKEN_KEY);
        
        sourcetypes.put(SourcetypeEnum.CLOUDTRAIL_UNPROCESSED, new Sourcetype(
            "./cloudtrail_via_cloudwatchevents_unprocessed.sample",
            cloudTrailToken,
                MIN_MBPS, //Perf peaks at 4 minutes (51mbps), starts degrading at 7 minutes down to 33 at 15 minutes
                MAX_MEMORY_MB)
        );
        sourcetypes.put(SourcetypeEnum.CLOUDWATCH_EVENTS_NO_VERSIONID, new Sourcetype(
            "./cloudwatchevents_awstrustedadvisor.sample",
            cloudWatchEventsToken,
                MIN_MBPS,
                MAX_MEMORY_MB)
        );
        sourcetypes.put(SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_MIXED, new Sourcetype(
            "./cloudwatchevents_ec2autoscale.sample",
            cloudWatchEventsToken,
            MIN_MBPS,
            MAX_MEMORY_MB)
        );
        sourcetypes.put(SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_SHORT, new Sourcetype(
            "./cloudwatchevents_codebuild.sample",
            cloudWatchEventsToken,
            MIN_MBPS,
            MAX_MEMORY_MB)
        );
        sourcetypes.put(SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_LONG, new Sourcetype(
            "./cloudwatchevents_macie.sample",
            cloudWatchEventsToken,
            MIN_MBPS,
            MAX_MEMORY_MB)
        );
        sourcetypes.put(SourcetypeEnum.VPCFLOWLOG, new Sourcetype(
            "./cloudwatchlogs_vpcflowlog_lambdaprocessed.sample",
            vpcFlowLogToken,
            MIN_MBPS,
            MAX_MEMORY_MB)
        );
}
    
    @Test
    @Override
    public void runTest() throws InterruptedException {
        setupSourcetypes();
        
        // For each sourcetype, send batches for 15 minutes
        for (SourcetypeEnum s : SourcetypeEnum.values()) {
            sourcetype = s;

            System.out.println("Sending to URL: " + connection.getSettings().getUrlString());
            System.out.println("With Token: " + connection.getSettings().getToken());
            // Read events from file once, then build a batch from it that we can reuse
            sendTextToRaw();
        }
    }
    
    @Override
    protected void setSenderToken(ConnectionSettings connectionSettings) {
        connectionSettings.setToken(sourcetypes.get(sourcetype).token);
    }
    
    @Override
    protected void checkAndLogPerformance(boolean shouldAssert) {
        super.checkAndLogPerformance(shouldAssert);
        if (shouldAssert) {
            // Throughput
            float mbps = showThroughput(System.currentTimeMillis(), testStartTimeMillis);
            if (mbps != Float.NaN) {
                System.out.println("Sourcetype " + sourcetype + " - mbps: " + mbps + " - at time(seconds):" + ((System.currentTimeMillis() - testStartTimeMillis) / 1000));
//            Assert.assertTrue("Throughput must be above minimum value of " + sourcetypes.get(sourcetype).minMbps,
//                    mbps > sourcetypes.get(sourcetype).minMbps);
            }
            // Memory Used
            long memoryUsed = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1000000; // MB
            System.out.println("Memory(MB): " + memoryUsed);
//        Assert.assertTrue("Memory usage must be below maximum value of " + sourcetypes.get(sourcetype).minMemory + " MB",
//                memoryUsed < sourcetypes.get(sourcetype).minMemory);

            // Failures
//            Integer numFailed = callbacks.getFailedCount();
//            Integer numSent = batchCounter.get();
//            float percentFailed = ( (float) numFailed / (float) numSent ) * 100F;
//            Assert.assertTrue("Percentage failed must be below 2%", percentFailed < 2F);

            // Threads
//            long threadCount = Thread.activeCount() - numSenderThreads;
//            LOG.info("Thread count: " + threadCount);
//            Assert.assertTrue("Thread count must be below maximum value of " + cliProperties.get(MAX_THREADS_KEY),
//                    threadCount < Long.parseLong(cliProperties.get(MAX_THREADS_KEY)));
        }
    }
    
    @Override
    protected void updateTimestampsOnBatch() {
        String byte_str = new String(buffer.array());
        // Convert time stamp based on source type
        if (sourcetype.equals(SourcetypeEnum.CLOUDTRAIL_UNPROCESSED) ||
                sourcetype.equals(SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_LONG) ||
                sourcetype.equals(SourcetypeEnum.CLOUDWATCH_EVENTS_VERSIONID_MIXED)) {
            byte_str = byte_str.replaceAll("\"time\":\\s?\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z","\"time\":\"" +
                    new SimpleDateFormat("YYYY-MM-DD'T'hh:mm:ss'Z'").format(new Date()));
        } else {
            byte_str = byte_str.replaceAll("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z",
                    new SimpleDateFormat("YYYY-MM-DD'T'hh:mm:ss'Z'").format(new Date()));
        }
        // Repack buffer
        byte[] bytes = byte_str.getBytes();
        buffer = ByteBuffer.wrap(bytes);
        
    }

    @Override
    protected void readEventsFile() {
        byte[] bytes = new byte[0];
        try {
            URL resource = getClass().getClassLoader().getResource(sourcetypes.get(sourcetype).filepath); // to use a file on classpath in resources folder.
            bytes = Files.readAllBytes(Paths.get(resource.getFile()));
        } catch (Exception ex) {
            Assert.fail("Problem reading file " + sourcetypes.get(sourcetype).filepath + ": " + ex.getMessage());
        }
        int origByteSize = bytes.length;
        buffer = ByteBuffer.allocate(batchSizeMB * 1024 * 1024 + 3000);

        // Make sure we send ~5MB batches, regardless of the size of the sample log file 
        while (buffer.position() <= batchSizeMB * 1024 * 1024) {
            try {
                buffer.put(bytes);
            } catch (BufferOverflowException e ) {
                System.out.println("buffer overflowed - could not put bytes");
                return;
            }
        }
        System.out.println("FINISHED BUILDING BATCH OF SIZE: " + buffer.position());
    }
}
