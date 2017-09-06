package com.splunk.cloudfwd;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.util.LoadBalancer;
import com.splunk.cloudfwd.util.PropertiesFileHelper;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by eprokop on 9/5/17.
 */
public class ConnectionWrapper implements Closeable {
    private Connection activeConnection;
    private ConnectionCallbacks callbacks;

    public ConnectionWrapper(ConnectionCallbacks callbacks) {
        this.activeConnection = new Connection(callbacks, new Properties());
        this.callbacks = callbacks;
    }

    public ConnectionWrapper(ConnectionCallbacks callbacks, Properties settings) {
        this.activeConnection = new Connection(callbacks, settings);
        this.callbacks = callbacks;
    }

    public void setProperties(Properties settings) {
        activeConnection.close();
        this.activeConnection = new Connection(callbacks, settings);
    }

    @Override
    public void close() {
        this.activeConnection.close();
    }

    public void closeNow() {
        this.activeConnection.closeNow();
    }

    public synchronized int send(Event event) throws HecConnectionTimeoutException {
        return this.activeConnection.send(event);
    }

    public synchronized int sendBatch(EventBatch events) throws HecConnectionTimeoutException {
        return this.activeConnection.sendBatch(events);
    }

    public synchronized void flush() throws HecConnectionTimeoutException {
        this.activeConnection.flush();
    }

    /**
     * @return the callbacks
     */
    public ConnectionCallbacks getCallbacks() {
        return callbacks;
    }

    /**
     * @return the closed
     */
    public boolean isClosed() {
        return this.activeConnection.isClosed();
    }

    /**
     * @return the hecEndpointType
     */
    public Connection.HecEndpoint getHecEndpointType() {
        return this.activeConnection.getHecEndpointType();
    }

    /**
     * @param hecEndpointType the hecEndpointType to set
     */
    public void setHecEndpointType(Connection.HecEndpoint hecEndpointType) {
        this.activeConnection.setHecEndpointType(hecEndpointType);
    }

    /**
     * @return the desired size of an EventBatch, in characters (not bytes)
     */
    public int getEventBatchSize() {
        return this.activeConnection.getEventBatchSize();
    }

    /**
     * @param numChars the size of the EventBatch in characters (not bytes)
     */
    public void setEventBatchSize(int numChars) {
        this.activeConnection.setEventBatchSize(numChars);
    }

    public long getBlockingTimeoutMS(){
        return this.activeConnection.getBlockingTimeoutMS();
    }


}
