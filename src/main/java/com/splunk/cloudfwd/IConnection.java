package com.splunk.cloudfwd;

import java.io.Closeable;
import java.util.Properties;

/**
 * Created by eprokop on 9/5/17.
 */
public interface IConnection extends Closeable {
    @Override
    public void close();

    public void closeNow();

    public int send(Event event) throws HecConnectionTimeoutException;

    public int sendBatch(EventBatch events) throws HecConnectionTimeoutException;

    public void flush() throws HecConnectionTimeoutException;

    /**
     * @return the callbacks
     */
    public ConnectionCallbacks getCallbacks();

    /**
     * @return the closed
     */
    public boolean isClosed();

    /**
     * @return the hecEndpointType
     */
    public Connection.HecEndpoint getHecEndpointType();

    /**
     * @param hecEndpointType the hecEndpointType to set
     */
    public void setHecEndpointType(
            Connection.HecEndpoint hecEndpointType);

    /**
     * @return the desired size of an EventBatch, in characters (not bytes)
     */
    public int getEventBatchSize();

    /**
     * @param numChars the size of the EventBatch in characters (not bytes)
     */
    public void setEventBatchSize(int numChars);

    public long getBlockingTimeoutMS();

    public void setEventAcknowledgementTimeoutMS(long ms);

    public void setProperties(Properties settings);
}
