package com.amazonaws.services.kinesis.samples.awsLogTypes.processor;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.EventBatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;

/**
 * Created by eprokop on 8/8/17.
 */
public class LogProcessorCallback implements ConnectionCallbacks {
    private static final Log LOG = LogFactory.getLog(LogProcessorCallback.class);
    private final Map<String, IRecordProcessorCheckpointer> checkpointerMap = new ConcurrentSkipListMap<>(); // event batch highest seq. no -> checkpointer fn
    private final String shardId;

    public LogProcessorCallback(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public void acknowledged(EventBatch events) {
        LOG.info("Received ack for event batch with sequenceNumber="
                + events.getId()
                + " (shardId=" + shardId + ")");
    }

    @Override
    public void failed(EventBatch events, Exception ex) {
        // TODO: show how to handle different types of exceptions
        LOG.warn("Sending failed for event batch with sequenceNumber="
                + events.getId()
                + " (shardId=" + shardId + "): "
                + ex.getMessage());
    }
    
    @Override
    public void systemWarning(Exception e) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }    
    
    @Override
    public void systemError(Exception e) {
        LOG.error("system error: {}", e);        
    }    

    @Override
    public void checkpoint(EventBatch events) {
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

    public void addCheckpointer(String sequenceNumber, IRecordProcessorCheckpointer checkpointer) {
        checkpointerMap.put(sequenceNumber, checkpointer);
    }

}
