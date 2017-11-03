package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class SplunkEventFieldsIT extends AbstractReconciliationTest {

    @Override
    protected int getNumEventsToSend() {
        return 10;
    }

    @Override
    protected void setProps(PropertiesFileHelper settings) {
        super.setProps(settings);
        settings.setToken(createTestToken(null));
    }

    @Override
    protected String getSearchString() {
        String searchString = "search";
        if (!connection.getSettings().getHost().isEmpty()) {
            searchString = searchString + " host=" +  connection.getSettings().getHost();
        }
        if (!connection.getSettings().getIndex().isEmpty()) {
            searchString = searchString + " index=" + connection.getSettings().getIndex();
        } else {
            searchString = searchString + " index=" + INDEX_NAME;
        }
        if (!connection.getSettings().getSource().isEmpty()) {
            searchString = searchString + " source=" +  connection.getSettings().getSource();
        }
        if (!connection.getSettings().getSourcetype().isEmpty()) {
            searchString = searchString + " sourcetype=" +  connection.getSettings().getSourcetype();
        }
        return searchString;
    }

    private String getLocalHost() {
        String host = null;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            //e.printStackTrace();
            LOG.error("{}", e.getMessage());
        }
        return host;
    }

    private String getSource() {
        return getClass().getName();
    }

    @Test
    public void sendEventsWithDefaultFieldsToRaw() throws InterruptedException, TimeoutException, HecConnectionTimeoutException, UnknownHostException {
        LOG.info("test: sendEventsWithDefaultFieldsToRaw");
        super.sendEvents();
        LOG.warn("SEARCH STRING: " + getSearchString());
        Set<String> results = getEventsFromSplunk();
        verifyResults(getSentEvents(), results);
    }

    @Test
    public void sendEventsWithNullValueFieldsToRaw() throws InterruptedException, TimeoutException, HecConnectionTimeoutException, UnknownHostException {
        LOG.info("test: sendEventsWithNullValueFieldsToRaw");
        connection.getSettings().setHost(null);
        connection.getSettings().setIndex(null);
        connection.getSettings().setSource(null);
        connection.getSettings().setSourcetype(null);
        super.sendEvents();
        LOG.warn("SEARCH STRING: " + getSearchString());
        Set<String> results = getEventsFromSplunk();
        verifyResults(getSentEvents(), results);
    }

    @Test
    public void sendEventsWithCustomFieldsToRaw() throws InterruptedException, TimeoutException, HecConnectionTimeoutException, UnknownHostException {
        LOG.info("test: sendEventsWithCustomFieldsToRaw");
        connection.getSettings().setIndex(INDEX_NAME);
        connection.getSettings().setHost(getLocalHost());
        connection.getSettings().setSource(getSource());
        connection.getSettings().setSourcetype(getSource());
        super.sendEvents();
        LOG.warn("SEARCH STRING: " + getSearchString());
        Set<String> results = getEventsFromSplunk();
        verifyResults(getSentEvents(), results);
    }

    @Test
    public void sendEventsWithDefaultFieldsToEvent() throws InterruptedException, TimeoutException, HecConnectionTimeoutException, UnknownHostException {
        LOG.info("test: sendEventsWithDefaultFieldsToEvent");
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.sendEvents();
        LOG.warn("SEARCH STRING: " + getSearchString());
        Set<String> results = getEventsFromSplunk();
        verifyResults(getSentEvents(), results);
    }

    @Test
    public void sendEventsWithNullValueFieldsToEvent() throws InterruptedException, TimeoutException, HecConnectionTimeoutException, UnknownHostException {
        LOG.info("test: sendEventsWithNullValueFieldsToEvent");
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        connection.getSettings().setHost(null);
        connection.getSettings().setIndex(null);
        connection.getSettings().setSource(null);
        connection.getSettings().setSourcetype(null);
        super.sendEvents();
        LOG.warn("SEARCH STRING: " + getSearchString());
        Set<String> results = getEventsFromSplunk();
        verifyResults(getSentEvents(), results);
    }

    @Test
    public void sendEventsWithCustomFieldsToEvent() throws InterruptedException, TimeoutException, HecConnectionTimeoutException, UnknownHostException {
        LOG.info("test: sendEventsWithCustomFieldsToEvent");
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        connection.getSettings().setIndex(INDEX_NAME);
        connection.getSettings().setHost(getLocalHost());
        connection.getSettings().setSource(getSource());
        connection.getSettings().setSourcetype(getSource());
        super.sendEvents();
        LOG.warn("SEARCH STRING: " + getSearchString());
        Set<String> results = getEventsFromSplunk();
        verifyResults(getSentEvents(), results);
    }
}