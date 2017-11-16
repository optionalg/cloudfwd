package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.util.ThreadScheduler;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * Created by eprokop on 11/15/17.
 */
public class MetricsConnection extends ConnectionImpl {
//    private long flushTimer = System.currentTimeMillis();
//    private final long flushTimeout = 60000; //1min
    
    public MetricsConnection(ConnectionCallbacks callbacks) {
        super(callbacks);
    }

    public MetricsConnection(ConnectionCallbacks callbacks, Properties settings) {
        super(callbacks, settings);
    }

    @Override
    public synchronized int send(Event event) throws HecConnectionTimeoutException, HecNoValidChannelsException {
        if (isClosed()) {
            throw new HecConnectionStateException("Attempt to send on closed connection.", HecConnectionStateException.Type.SEND_ON_CLOSED_CONNECTION);
        }
        if (null == this.events) {
            this.events = new EventBatchImpl();
        }
        this.events.add(event);
        if (this.events.isFlushable(propertiesFileHelper.getEventBatchSize())) {
            asyncSendBatch();
            // TODO: send metrics after some interval also
        }
        return 0;
    }

    @Override
    public synchronized void flush() throws HecConnectionTimeoutException, HecNoValidChannelsException {
        if (null != events && events.getNumEvents() != 0) {
            asyncSendBatch();
        }
    }
    
    private synchronized void asyncSendBatch() {
        ExecutorService e = ThreadScheduler.getSharedExecutorInstance("metric_sender");
        Runnable r = ()->{
          sendBatch(events);  
        };
        e.execute(r);
    }
    
//    private boolean shouldFlush() {
//        return this.events.isFlushable(propertiesFileHelper.getEventBatchSize()) || 
//                flushTimer - System.currentTimeMillis() > flushTimeout;
//    }
}
