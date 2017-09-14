package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;

import com.splunk.cloudfwd.ConnectionCallbacks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.splunk.cloudfwd.impl.EventBatchImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Example implementation of ConnectionCallback interface for use in a Kinesis Streams
 * consumer application.
 */
public class StockTradeProcessorCallback implements ConnectionCallbacks {
    private static final Log LOG = LogFactory.getLog(StockTradeProcessorCallback.class);
    private final Map<String, IRecordProcessorCheckpointer> checkpointerMap = new ConcurrentSkipListMap<>(); // event batch highest seq. no -> checkpointer fn
    private final String shardId;

    public StockTradeProcessorCallback(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public void acknowledged(EventBatchImpl events) {
        LOG.info("Received ack for event batch with sequenceNumber="
                + events.getId()
                + " (shardId=" + shardId + ")");
    }

    @Override
    public void failed(EventBatchImpl events, Exception ex) {
        // TODO: show how to handle different types of exceptions
        LOG.warn("Sending failed for event batch with sequenceNumber="
                + events.getId()
                + " (shardId=" + shardId + "): "
                + ex.getMessage());
    }

    @Override
    public void checkpoint(EventBatchImpl events) {
        String sequenceNumber = (String)events.getId(); // highest sequence number in the event batch
        try {
            LOG.info("Checkpointing at sequenceNumber="
                    + sequenceNumber
                    + "(shardId=" + shardId + ")");
            IRecordProcessorCheckpointer cp = checkpointerMap.get(sequenceNumber);
            cp.checkpoint(sequenceNumber);
            checkpointerMap.remove(sequenceNumber);
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

    /**
     * Stores a checkpointer function associated with a given sequence number so that the callback
     * can call the right checkpointer function when the batch is acknowledged by Splunk.
     * Should be called right before sending an event batch to Splunk.
     * @param sequenceNumber the highest (last) sequence number in the event batch to be sent
     * @param checkpointer the checkpointer function passed to ProcessRecords
     */
    public void addCheckpointer(String sequenceNumber, IRecordProcessorCheckpointer checkpointer) {
        checkpointerMap.put(sequenceNumber, checkpointer);
    }
}
