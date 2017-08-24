package com.amazonaws.services.kinesis.samples.logtypes.processor;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.samples.logtypes.utils.ConfigurationUtils;
import com.amazonaws.services.kinesis.samples.logtypes.utils.CredentialUtils;

/**
 * Created by mhora on 8/21/17.
 */
public class LogsProcessor {

    private static final Log LOG = LogFactory.getLog(LogsProcessor.class);

    private static final Logger ROOT_LOGGER = Logger.getLogger("");
    private static final Logger PROCESSOR_LOGGER =
            Logger.getLogger("com.amazonaws.services.kinesis.samples.logtypes.processor");

    private static void checkUsage(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: " + LogsProcessor.class.getSimpleName()
                    + " <application name> <stream name> <region> <profile name>");
            System.exit(1);
        }
    }

    /**
     * Sets the global log level to WARNING and the log level for this package to INFO,
     * so that we only see INFO messages for this processor. This is just for the purpose
     * of this tutorial, and should not be considered as best practice.
     */
    private static void setLogLevels() {
        ROOT_LOGGER.setLevel(Level.WARNING);
        PROCESSOR_LOGGER.setLevel(Level.INFO);
    }

    /*
     * Entry point to read dummy stock trade data from a Kinesis stream and send it to Splunk.
     * Reads Splunk HEC configuration from examples/kinesis/resources/kinesis_example_lb.properties
     *
     * Run with four arguments:
     *      1) application name
     *      2) stream name
     *      3) AWS region name
     *      4) profile name
     *
     * Make sure credentials for "profile name" are provided in ~/.aws/credentials
     * For more information, see http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
     */
    public static void main(String[] args) throws Exception {
        checkUsage(args);

        String applicationName = args[0];
        String streamName = args[1];
        Region region = RegionUtils.getRegion(args[2]);
        if (region == null) {
            System.err.println(args[2] + " is not a valid AWS region.");
            System.exit(1);
        }
        String profileName = args[3];

        setLogLevels();

        AWSCredentialsProvider credentialsProvider = CredentialUtils.getCredentialsProvider(profileName);

        String workerId = String.valueOf(UUID.randomUUID());
        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration(applicationName, streamName, credentialsProvider, workerId)
                        .withRegionName(region.getName())
                        .withCommonClientConfig(ConfigurationUtils.getClientConfigWithUserAgent());

        IRecordProcessorFactory recordProcessorFactory = new LogRecordProcessorFactory();

        // Create the KCL worker with the log record processor factory
        Worker worker = new Worker(recordProcessorFactory, kclConfig);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);

    }
}
