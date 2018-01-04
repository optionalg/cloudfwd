package com.splunk.cloudfwd.test.perf;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by mhora on 1/3/18.
 */
public class BenchmarkTest extends MultiThreadedVolumeTest {
    private ByteBuffer buffer;
    
    // Configurable options //TODO: make this configurable through CLI
    private Sourcetype sourcetype;
    private int batchSizeMB = 5;
    private enum Sourcetype {
        CLOUDTRAIL_UNPROCESSED,
        CLOUDTRAIL_PROCESSED,
        CLOUDWATCH_EVENTS_NO_VERSIONID,
        CLOUDWATCH_EVENTS_VERSIONID_MIXED,
        CLOUDWATCH_EVENTS_VERSIONID_SHORT,
        CLOUDWATCH_EVENTS_VERSIONID_LONG,
        VPCFLOWLOG
    }
    
    // # nodes in cluster 

    private String getEventsFilename() {
        switch(sourcetype) {
            case CLOUDTRAIL_UNPROCESSED:
                return "cloudtrail_via_cloudwatchevents_unprocessed.sample";
            case CLOUDTRAIL_PROCESSED:
                return "cloudtrail_modinputprocessed.sample";
            case CLOUDWATCH_EVENTS_NO_VERSIONID:
                // Events do not contain either version or id 
                return "cloudwatchevents_awstrustedadvisor.sample";
            case CLOUDWATCH_EVENTS_VERSIONID_MIXED:
                // Some events contain both version and id, while others just contain id
                return "cloudwatchevents_ec2autoscale.sample";
            case CLOUDWATCH_EVENTS_VERSIONID_SHORT:
                // Events contain both version and id, and are short in length
                return "cloudwatchevents_codebuild.sample";
            case CLOUDWATCH_EVENTS_VERSIONID_LONG:
                // Events contain both version and id, and are long in length
                return "cloudwatchevents_macie.sample";
            case VPCFLOWLOG:
                return "cloudwatchlogs_vpcflowlog_lambdaprocessed.sample";
            default:
                return "many_text_events_no_timestamp.sample";
        }
    }

    @Test
    public void runPerfTest() throws InterruptedException {
        // For each sourcetype, send batches for 15 minutes
        for (Sourcetype s : Sourcetype.values()) {
            sourcetype = s;
            // Read events from file once, then build a batch from it that we can reuse
            sendTextToRaw();
        }
        
    }
    
    @Override
    protected void updateTimestampsOnBatch() {
        // no-op - overridden in child class to do timestamp configuration on buffer variable
        String byte_str = new String(buffer.array());
        // TODO Convert time stamp based on source type
        if (sourcetype.equals(Sourcetype.CLOUDTRAIL_UNPROCESSED) ||
                sourcetype.equals(Sourcetype.CLOUDWATCH_EVENTS_VERSIONID_LONG) ||
                sourcetype.equals(Sourcetype.CLOUDWATCH_EVENTS_VERSIONID_MIXED)) {
            byte_str = byte_str.replaceAll("\"time\":\\s?\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z",
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
            URL resource = getClass().getClassLoader().getResource(getEventsFilename()); // to use a file on classpath in resources folder.
            bytes = Files.readAllBytes(Paths.get(resource.getFile()));
        } catch (Exception ex) {
            Assert.fail("Problem reading file " + getEventsFilename() + ": " + ex.getMessage());
        }
        int origByteSize = bytes.length;
        buffer = ByteBuffer.allocate(batchSizeMB * 1024 * 1024 + 3000);

        // Make sure we send ~5MB batches, regardless of the size of the sample log file 
        while (buffer.position() <= batchSizeMB * 1024 * 1024) {
            System.out.println("******** BATCH SIZE 1: going to add " + origByteSize + " bytes");
            try {
                buffer.put(bytes);
            } catch (BufferOverflowException e ) {
                System.out.println("buffer overflowed - could not put bytes");
                return;
            }
            System.out.println("******** BATCH SIZE 2: current buffer size: " + buffer.position());
        }
    }
}
