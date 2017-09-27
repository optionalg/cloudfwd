package com.amazonaws.services.kinesis.samples.FirehoseSplunkExample;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;



public class PutRecordFromFile {
    private static AmazonKinesisFirehose firehoseClient;
    private static FirehoseSplunkSettings firehoseSettings = new FirehoseSplunkSettings();
    private static final int BATCH_PUT_MAX_SIZE = 500;

    /**
     * Method to put records in the specified delivery stream by reading
     * contents from sample input file using PutRecordBatch API.
     *
     * @throws IOException
     */
    public static void putRecordBatchIntoDeliveryStream() throws IOException {
        try (InputStream inputStream = PutRecordFromFile.class.getResourceAsStream(
                firehoseSettings.getPropertyFor("data")))
        {

            if (inputStream == null) {
                throw new FileNotFoundException("Could not find data file");
            }

            List<Record> recordList = new ArrayList<Record>();
            int batchSize = 0;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    String data = line + "\n";
                    Record record = createRecord(data);
                    recordList.add(record);
                    batchSize++;

                    if (batchSize == BATCH_PUT_MAX_SIZE) {
                        putRecordBatch(recordList);

                        recordList.clear();
                        batchSize = 0;
                    }
                }

                if (batchSize > 0) {
                    putRecordBatch(recordList);
                }
            }
        }
    }

    public static Record createRecord(String data) {
        return new Record().withData(ByteBuffer.wrap(data.getBytes()));
    }

    private static PutRecordBatchResult putRecordBatch(List<Record> recordList) {
        PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
        putRecordBatchRequest.setDeliveryStreamName(firehoseSettings.getPropertyFor("aws_fh_stream_name"));
        putRecordBatchRequest.setRecords(recordList);

        // Put Record Batch records. Max No.Of Records we can put in a
        // single put record batch request is 500
        return firehoseClient.putRecordBatch(putRecordBatchRequest);
    }

    public static void main (String args[]) throws InterruptedException, IOException{
        firehoseSettings.validateProperties();
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        String serviceEndpoint = firehoseSettings.getPropertyFor("aws_fh_endpoint");
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                serviceEndpoint, null);
        AmazonKinesisFirehoseClient.builder().withClientConfiguration(clientConfiguration).build();
        firehoseClient = AmazonKinesisFirehoseClient.builder()
                .withClientConfiguration(clientConfiguration)
                .withRegion(firehoseSettings.getPropertyFor("aws_fh_stream_region"))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
        System.out.println("Putting records in deliveryStream");
        putRecordBatchIntoDeliveryStream();
        System.out.println("All records in the file are put");


    }
}