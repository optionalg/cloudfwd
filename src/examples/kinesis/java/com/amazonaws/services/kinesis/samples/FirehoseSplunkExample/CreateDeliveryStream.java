package com.amazonaws.services.kinesis.samples.FirehoseSplunkExample;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.*;

public class CreateDeliveryStream {

    private static AmazonKinesisFirehose firehoseClient;
    private static FirehoseSplunkSettings settings = new FirehoseSplunkSettings();

    public static void createDeliveryStream(String streamName) {
        SplunkRetryOptions splunkRetryOptions = new SplunkRetryOptions()
                .withDurationInSeconds(Integer.valueOf(
                        settings.getPropertyFor("splunk_retry_timer")));

        BufferingHints bufferingHints = new BufferingHints()
                .withIntervalInSeconds(Integer.valueOf(settings.getPropertyFor("aws_s3_buffer_interval")))
                .withSizeInMBs(Integer.valueOf(settings.getPropertyFor("aws_s3_buffer_size")));

        CloudWatchLoggingOptions cloudWatchLoggingOptions = new CloudWatchLoggingOptions()
                .withEnabled(Boolean.valueOf(settings.getPropertyFor("aws_cw_log_enabled")))
                .withLogGroupName(settings.getPropertyFor("aws_cw_log_group_name"))
                .withLogStreamName(settings.getPropertyFor("aws_cw_log_stream_name"));

        EncryptionConfiguration encryptionConfiguration = new EncryptionConfiguration()
                .withNoEncryptionConfig(settings.getPropertyFor("aws_s3_encryption"));

        S3DestinationConfiguration s3DestinationConfiguration = new S3DestinationConfiguration()
                .withRoleARN(settings.getPropertyFor("aws_s3_iam_role"))
                .withBucketARN(settings.getPropertyFor("aws_s3_bucket"))
                .withBufferingHints(bufferingHints)
                .withEncryptionConfiguration(encryptionConfiguration);

        SplunkDestinationConfiguration splunkDestinationConfiguration = new SplunkDestinationConfiguration()
                .withHECEndpoint(settings.getPropertyFor("hec_endpoint"))
                .withHECEndpointType(HECEndpointType.valueOf(settings.getPropertyFor("hecendpoint_type")))
                .withHECToken(settings.getPropertyFor("hec_token"))
                .withRetryOptions(splunkRetryOptions)
                .withS3BackupMode(settings.getPropertyFor("aws_s3_backup"))
                .withCloudWatchLoggingOptions(cloudWatchLoggingOptions)
                .withS3Configuration(s3DestinationConfiguration);
        CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest()
                .withDeliveryStreamName(streamName)
                .withDeliveryStreamType(settings.getPropertyFor("aws_fh_stream_type"))
                .withSplunkDestinationConfiguration(splunkDestinationConfiguration);
        CreateDeliveryStreamResult createDeliveryStreamResult = firehoseClient.createDeliveryStream(createDeliveryStreamRequest);
        System.out.println(createDeliveryStreamResult.getDeliveryStreamARN().toString());
    }

    public static void main(String args[]) throws InterruptedException {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        String serviceEndpoint = settings.getPropertyFor("aws_fh_endpoint");
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                serviceEndpoint, null);
        AmazonKinesisFirehoseClient.builder().withClientConfiguration(clientConfiguration).build();
        firehoseClient = AmazonKinesisFirehoseClient.builder()
                .withClientConfiguration(clientConfiguration)
                .withRegion(settings.getPropertyFor("aws_fh_stream_region"))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
        createDeliveryStream(settings.getPropertyFor("aws_fh_stream_name"));
    }
}

