package com.amazonaws.services.kinesis.samples.FirehoseSplunkExample;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.*;

public class UpdateDeliveryStream {

    private static AmazonKinesisFirehose firehoseClient;
    private static FirehoseSplunkSettings settings = new FirehoseSplunkSettings();

    public static void updateDeliveryStream(String streamName) {
        SplunkRetryOptions splunkRetryOptions = new SplunkRetryOptions()
                .withDurationInSeconds(Integer.valueOf(
                        settings.getPropertyFor("splunk_retry_timer")));

        SplunkDestinationUpdate splunkDestinationUpdate = new SplunkDestinationUpdate()
                .withRetryOptions(splunkRetryOptions)
                .withHECEndpoint(settings.getPropertyFor("hec_endpoint"))
                .withHECEndpointType(HECEndpointType.valueOf(settings.getPropertyFor("hecendpoint_type")))
                .withHECToken(settings.getPropertyFor("hec_token"));

        UpdateDestinationRequest updateDestinationRequest = new UpdateDestinationRequest()
                .withDeliveryStreamName(streamName)
                .withCurrentDeliveryStreamVersionId(settings.getPropertyFor("aws_fh_stream_version"))
                .withDestinationId(settings.getPropertyFor("aws_fh_dest_id"))
                .withSplunkDestinationUpdate(splunkDestinationUpdate);

        UpdateDestinationResult updateDestinationResult = firehoseClient.updateDestination(updateDestinationRequest);
        System.out.println(updateDestinationResult.getSdkResponseMetadata().toString());
    }

    public static void main(String args[]) throws InterruptedException{
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
        updateDeliveryStream(settings.getPropertyFor("aws_fh_stream_name"));
    }
}
