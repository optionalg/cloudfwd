package com.amazonaws.services.kinesis.samples.FirehoseSplunkExample;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.*;

public class DeleteDeliveryStream {

    private static AmazonKinesisFirehose firehoseClient;
    private static FirehoseSplunkSettings settings = new FirehoseSplunkSettings();

    public static void deleteDeliveryStream(String streamName) {
        DeleteDeliveryStreamRequest deleteDeliveryStreamRequest = new DeleteDeliveryStreamRequest()
                .withDeliveryStreamName(streamName);
        DeleteDeliveryStreamResult deleteDeliveryStreamResult = firehoseClient.deleteDeliveryStream(deleteDeliveryStreamRequest);
        System.out.println(deleteDeliveryStreamResult.toString());
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
        deleteDeliveryStream(settings.getPropertyFor("aws_fh_stream_name"));
    }
}

