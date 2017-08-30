/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.awsLogTypes.writer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.samples.awsLogTypes.utils.ConfigurationUtils;
import com.amazonaws.services.kinesis.samples.awsLogTypes.utils.CredentialUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Continuously sends cloud watch events, vpc logs and sys logs from files to Kinesis
 *
 */
public class LogWriter {

    private static final Log LOG = LogFactory.getLog(LogWriter.class);
    private final static ObjectMapper JSON = new ObjectMapper();

    private static void checkUsage(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: " + LogWriter.class.getSimpleName()
                    + " <stream name> <region> <profile name> <log file name>");
            System.exit(1);
        }
    }

    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param streamName Name of stream
     */
    private static void validateStream(AmazonKinesis kinesisClient, String streamName) {
        try {
            DescribeStreamResult result = kinesisClient.describeStream(streamName);
            if(!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }
        } catch (ResourceNotFoundException e) {
            System.err.println("Stream " + streamName + " does not exist. Please create it in the console.");
            System.err.println(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error found while describing the stream " + streamName);
            System.err.println(e);
            System.exit(1);
        }
    }

    /**
     * Uses the Kinesis client to send the data from log files to the given stream.
     *
     * @param logs files representing cloud watch logs and events
     * @param kinesisClient Amazon Kinesis client
     * @param streamName Name of stream
     */
    private static void sendLogs(String logs, AmazonKinesis kinesisClient,
            String streamName, String logFileName) {
        byte[] bytes;
        bytes = logs.getBytes();
        LOG.info("Putting logs: " + logs);
        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamName);
        putRecord.setPartitionKey(logFileName);
        putRecord.setData(ByteBuffer.wrap(bytes));

        try {
            kinesisClient.putRecord(putRecord);
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }

    /*
     * Entry point to write dummy log data to a Kinesis stream.
     *
     * Run with three arguments:
     *      1) stream name
     *      2) AWS region name
     *      3) profile name
     *      4) log file name within '/logfiles/'
     *
     * Make sure credentials for "profile name" are provided in ~/.aws/credentials (e.g. 'default')
     * For more information, see http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
     */
    public static void main(String[] args) throws Exception {
        checkUsage(args);

        String streamName = args[0];
        String regionName = args[1];
        String profileName = args[2];
        String logFileName = args[3];
        Region region = RegionUtils.getRegion(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }

        AWSCredentials credentials = CredentialUtils.getCredentialsProvider(profileName).getCredentials();

        AmazonKinesis kinesisClient = new AmazonKinesisClient(credentials,
                ConfigurationUtils.getClientConfigWithUserAgent());
        kinesisClient.setRegion(region);

        // Validate that the stream exists and is active
        validateStream(kinesisClient, streamName);

        InputStream stream = LogWriter.class.getResourceAsStream("../logfiles/" + logFileName);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            String line = null;
            while ((line = reader.readLine()) != null) {
                sendLogs(line, kinesisClient, streamName, logFileName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
