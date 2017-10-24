package com.splunk.cloudfwd.test.integration;

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
            e.printStackTrace();
        }
        return host;
    }

    private String getSource() {
        return getClass().getName();
    }

    @Test
    public void sendEventsWithDefaultFields() throws InterruptedException, TimeoutException, HecConnectionTimeoutException, UnknownHostException {
        super.sendEvents();
        LOG.warn("SEARCH STRING: " + getSearchString());
        Set<String> results = getEventsFromSplunk();
        verifyResults(getSentEvents(), results);
    }
    @Test
    public void sendEventsWithCustomFields() throws InterruptedException, TimeoutException, HecConnectionTimeoutException, UnknownHostException {
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