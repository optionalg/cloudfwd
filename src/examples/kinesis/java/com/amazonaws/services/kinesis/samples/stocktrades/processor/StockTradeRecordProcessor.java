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

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import java.util.List;

import com.splunk.cloudfwd.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processes records retrieved from stock trades stream.
 *
 */
public class StockTradeRecordProcessor implements IRecordProcessor {
    private static final Log LOG = LogFactory.getLog(StockTradeRecordProcessor.class);
    private String kinesisShardId;

    private final int BATCH_SIZE = 10;
    private EventBatch eventBatch = new EventBatch();
    private IConnection splunk;
    StockTradeProcessorCallback callback;

    /**
     * {@inheritDoc}
     */
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
        callback = new StockTradeProcessorCallback(shardId);
        try {
            splunk = Connection.createConnection(callback);
            splunk.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        } catch (RuntimeException e) {
            LOG.error("Unable to connect to Splunk.", e);
            System.exit(1);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        for (Record record : records) {
            processRecord(record, checkpointer);
        }
    }

    private void processRecord(Record record, IRecordProcessorCheckpointer checkpointer) {
        StockTrade trade = StockTrade.fromJsonAsBytes(record.getData().array());
        if (trade == null) {
            LOG.warn("Could not deserialize record into StockTrade object.");
            return;
        }

        eventBatch.add(new EventWithMetadata(trade.toString(), record.getSequenceNumber()));
        if (eventBatch.getNumEvents()  >= BATCH_SIZE) {
            LOG.info("Sending event batch with sequenceNumber=" + eventBatch.getId());
            callback.addCheckpointer((String)eventBatch.getId(), checkpointer);
          try {
            splunk.sendBatch(eventBatch);
          } catch (HecConnectionTimeoutException ex) {
            Logger.getLogger(StockTradeRecordProcessor.class.getName()).
                    log(Level.SEVERE, null, ex);
          }
            eventBatch = new EventBatch();
        }
    }

    /**
     * {@inheritDoc}
     */
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            try {
                checkpointer.checkpoint();
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
            }
        }
    }
}
