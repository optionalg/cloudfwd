package com.splunk.cloudfwd.test.mock;/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.net.UnknownHostException;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Connections;
import java.io.IOException;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import javafx.beans.property.Property;
import jdk.nashorn.internal.ir.PropertyKey;
import org.junit.Test;
import org.junit.Assert;

/**
 * Created by kchen on 9/27/17.
 */
public class ConnectionSettingsTest extends AbstractConnectionTest{

    /*
    Test:
    getUrls, urlStringToList, getUrlWithAutoAssignedPorts, getSimulatedEndpoints
     */
    @Test
    public void urlTest() {
        // Test initial properties set by test.properties and ConnectionSettings defaults (test.properties takes precedence over defaults)
        ConnectionSettings settings = connection.getSettings();
        
        // From test.properties
        Assert.assertEquals("https://127.0.0.1:8088", settings.getUrlString());

        settings.setUrl("https://127.0.0.1:8089");
        Assert.assertEquals("https://127.0.0.1:8089", settings.getUrlString());
    }
    
    @Test
    public void channelsPerDestTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(4, settings.getChannelsPerDestination());

        settings.setChannelsPerDestination(7);
        Assert.assertEquals(7, settings.getChannelsPerDestination());
        
        settings.setChannelsPerDestination(0);
        Assert.assertEquals(PropertyKeys.DEFAULT_CHANNELS_PER_DESTINATION, settings.getChannelsPerDestination());
    }
    
    @Test
    public void unresponsiveMSTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(-1, settings.getUnresponsiveMS());
        
        settings.setUnresponsiveMS(1000);
        Assert.assertEquals(1000, settings.getUnresponsiveMS());
        
        settings.setUnresponsiveMS(0);
        Assert.assertEquals(PropertyKeys.DEFAULT_UNRESPONSIVE_MS, settings.getUnresponsiveMS());
    }
    
    @Test
    public void ackPollMSTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(250, settings.getAckPollMS());
        
        settings.setAckPollMS(5000);
        Assert.assertEquals(5000, settings.getAckPollMS());

        settings.setAckPollMS(0);
        Assert.assertEquals(PropertyKeys.MIN_ACK_POLL_MS, settings.getAckPollMS());
    }
    
    @Test
    public void healthPollMSTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(5000, settings.getHealthPollMS());

        settings.setHealthPollMS(1000);
        Assert.assertEquals(1000, settings.getHealthPollMS());
        
        settings.setHealthPollMS(0);
        Assert.assertEquals(PropertyKeys.MIN_HEALTH_POLL_MS, settings.getHealthPollMS());
    }
    
    @Test
    public void maxTotalChannelsTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(8, settings.getMaxTotalChannels());
        
        settings.setMaxTotalChannels(1);
        Assert.assertEquals(1, settings.getMaxTotalChannels());
        
        settings.setMaxTotalChannels(0);
        Assert.assertEquals(Integer.MAX_VALUE, settings.getMaxTotalChannels());

    }
    
    @Test
    public void maxUnackedEventBatchPerChannelTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(10000, settings.getMaxUnackedEventBatchPerChannel());

        settings.setMaxUnackedEventBatchPerChannel(20000);
        Assert.assertEquals(20000, settings.getMaxUnackedEventBatchPerChannel());

        settings.setMaxUnackedEventBatchPerChannel(0);
        Assert.assertEquals(10000, settings.getMaxUnackedEventBatchPerChannel()); //no limit
    }
    
    @Test
    public void eventBatchSizeTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(0, settings.getEventBatchSize());

        settings.setEventBatchSize(100);
        Assert.assertEquals(100, settings.getEventBatchSize());
        
        settings.setEventBatchSize(0);
        Assert.assertEquals(PropertyKeys.MIN_EVENT_BATCH_SIZE, settings.getEventBatchSize());
    }
    
    @Test
    public void channelDecomMSTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals(PropertyKeys.DEFAULT_DECOM_MS, settings.getChannelDecomMS());

        settings.setChannelDecomMS(80000);
        Assert.assertEquals(80000, settings.getChannelDecomMS());

        settings.setChannelDecomMS(1);
        Assert.assertEquals(-1, settings.getChannelDecomMS()); //No limit

        Assert.assertEquals(true, settings.isMockHttp());
        settings.setChannelDecomMS(100);
        Assert.assertEquals(100, settings.getChannelDecomMS()); //Mock Http can set low DecomMS

        settings.setMockHttp(false);
        settings.setChannelDecomMS(10);
        Assert.assertEquals(PropertyKeys.MIN_DECOM_MS, settings.getChannelDecomMS()); //Non-mock Http cannot set low DecomMS
    }
    
    @Test 
    public void channelQuiesceTimeoutMSTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals(PropertyKeys.DEFAULT_CHANNEL_QUIESCE_TIMEOUT_MS, settings.getChannelQuiesceTimeoutMS());
        
        settings.setChannelQuiesceTimeoutMS(200000);
        Assert.assertEquals(200000, settings.getChannelQuiesceTimeoutMS());
        
        Assert.assertEquals(true, settings.isMockHttp());
        settings.setChannelQuiesceTimeoutMS(100);
        Assert.assertEquals(100, settings.getChannelQuiesceTimeoutMS());
        
        settings.setMockHttp(false);
        settings.setChannelQuiesceTimeoutMS(10);
        Assert.assertEquals(PropertyKeys.MIN_CHANNEL_QUIESCE_TIMEOUT_MS, settings.getChannelQuiesceTimeoutMS());
    }
    
    @Test
    public void ackTimeoutMSTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(60000, settings.getAckTimeoutMS());

        settings.setAckTimeoutMS(100000);
        Assert.assertEquals(100000, settings.getAckTimeoutMS());
        
        settings.setAckTimeoutMS(0);
        Assert.assertEquals(Long.MAX_VALUE, settings.getAckTimeoutMS()); // no limit

        settings.setAckTimeoutMS(10000);
        Assert.assertEquals(PropertyKeys.MIN_ACK_TIMEOUT_MS, settings.getAckTimeoutMS());
    }
    
    @Test
    public void blockingTimeoutMSTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals(PropertyKeys.DEFAULT_BLOCKING_TIMEOUT_MS, settings.getBlockingTimeoutMS());

        settings.setBlockingTimeoutMS(1000);
        Assert.assertEquals(1000, settings.getBlockingTimeoutMS());

        settings.setBlockingTimeoutMS(1000);
        Assert.assertEquals(1000, settings.getBlockingTimeoutMS());
    }
    
    @Test
    public void mockHttpClassnameTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals("com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints", settings.getMockHttpClassname()); //default in ConnectionSettings

        settings.setMockHttpClassname("oh.hai.there");
        Assert.assertEquals("oh.hai.there", settings.getMockHttpClassname());
    }
    
    @Test
    public void certValidationEnabledTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(true, settings.isCertValidationDisabled());

        settings.setCertValidationEnabled(false);
        Assert.assertEquals(false, settings.isCertValidationDisabled());
    }
    
    @Test
    public void httpDebugEnabledTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(false, settings.isHttpDebugEnabled());

        settings.setHttpDebugEnabled(true);
        Assert.assertEquals(true, settings.isHttpDebugEnabled());
    }
    
    @Test
    public void checkpointEnabledTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(false, settings.isCheckpointEnabled());

        settings.setCheckpointEnabled(true);
        Assert.assertEquals(true, settings.isCheckpointEnabled());
    }

    @Test
    public void getSSLCertContentForCloudInstance() throws IOException {
        ConnectionSettings settings = connection.getSettings();
        settings.setUrl("https://customer.cloud.splunk.com:8088");
        Connection c = Connections.create(settings);

        // For cloud instance, if we didn't set CLOUD_SSL_CERT_CONTENT in overrides,
        // it will pick up from cloudfwd.properties
        // Non-exist ssl content
        String defaultCloudCertContent = "-----BEGIN CERTIFICATE-----" +
                "MIIB/DCCAaGgAwIBAgIBADAKBggqhkjOPQQDAjB+MSswKQYDVQQDEyJTcGx1bmsg" +
                "Q2xvdWQgQ2VydGlmaWNhdGUgQXV0aG9yaXR5MRYwFAYDVQQHEw1TYW4gRnJhbmNp" +
                "c2NvMRMwEQYDVQQKEwpTcGx1bmsgSW5jMQswCQYDVQQIEwJDQTEVMBMGA1UECxMM" +
                "U3BsdW5rIENsb3VkMB4XDTE0MTExMDA3MDAxOFoXDTM0MTEwNTA3MDAxOFowfjEr" +
                "MCkGA1UEAxMiU3BsdW5rIENsb3VkIENlcnRpZmljYXRlIEF1dGhvcml0eTEWMBQG" +
                "A1UEBxMNU2FuIEZyYW5jaXNjbzETMBEGA1UEChMKU3BsdW5rIEluYzELMAkGA1UE" +
                "CBMCQ0ExFTATBgNVBAsTDFNwbHVuayBDbG91ZDBZMBMGByqGSM49AgEGCCqGSM49" +
                "AwEHA0IABPRRy9i3yQcxgMpvCSsI7Qe6YZMimUHOecPZWaGz5jEfB4+p5wT7dF3e" +
                "QrgjDWshVJZvK6KGO7nDh97GnbVXrTCjEDAOMAwGA1UdEwQFMAMBAf8wCgYIKoZI" +
                "zj0EAwIDSQAwRgIhALMUgLYPtICN9ci/ZOoXeZxUhn3i4wIo2mPKEWX0IcfpAiEA" +
                "8Jid6bzwUqAdDZPSOtaEBXV9uRIrNua0Qxl1S55TlWY=" +
                "-----END CERTIFICATE-----";
        String sslCert = settings.getSSLCertContent();
        if (!sslCert.equals(defaultCloudCertContent)) {
            Assert.fail("Expect: " + defaultCloudCertContent + "\nbut got: " + sslCert);
        }

    }

    @Test
    public void getSSLCertContentForNonCloudInstance() throws UnknownHostException {
        ConnectionSettings settings = connection.getSettings();
        settings.setUrl("https://localhost:8088");

        // Non-empty ssl content
        String expectedSSLCert = "testing ssl cert";
        settings.setSSLCertContent(expectedSSLCert);
        String sslCert = settings.getSSLCertContent();
        if (!sslCert.equals(expectedSSLCert)) {
            Assert.fail("Expect: " + expectedSSLCert + "\nbut got: " + sslCert);
        }
    }

    @Test
    public void cloudInstanceTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(false, settings.isCloudInstance());

        settings.setUrl("https://foo.cloud.splunk.com");
        Assert.assertEquals(true, settings.isCloudInstance());
    }
    
    @Test 
    public void maxRetriesTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals(PropertyKeys.DEFAULT_RETRIES, settings.getMaxRetries());
        
        settings.setMaxRetries(5);
        Assert.assertEquals(5, settings.getMaxRetries());

        settings.setMaxRetries(0);
        Assert.assertEquals(Integer.MAX_VALUE, settings.getMaxRetries());
    }
    
    @Test
    public void maxPreflightRetriesTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals(PropertyKeys.DEFAULT_PREFLIGHT_RETRIES, settings.getMaxPreflightRetries());

        settings.setMaxPreflightRetries(5);
        Assert.assertEquals(5, settings.getMaxPreflightRetries());

        settings.setMaxPreflightRetries(0);
        Assert.assertEquals(Integer.MAX_VALUE, settings.getMaxPreflightRetries());
    }
    
    @Test
    public void hecEndpointTypeTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals(ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT, settings.getHecEndpointType());

        settings.setHecEndpointType(ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        Assert.assertEquals(ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT, settings.getHecEndpointType());

        settings.setHecEndpointType(ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT);
        Assert.assertEquals(ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT, settings.getHecEndpointType());
    }
    
    @Test
    public void tokenTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals("", settings.getToken());

        settings.setToken("woot");
        Assert.assertEquals("woot", settings.getToken());
    }
    
    @Test
    public void testPropertiesEnabledTest() {
        ConnectionSettings settings = connection.getSettings();

        // From test.properties
        Assert.assertEquals(true, settings.getTestPropertiesEnabled());

        settings.setTestPropertiesEnabled(false);
        Assert.assertEquals(false, settings.getTestPropertiesEnabled());
    }
    
    @Test
    public void hostTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals("localhost:8088", settings.getHost()); // Set by PropertiesFileHelper > createSender()

        settings.setHost("woot");
        Assert.assertEquals("woot", settings.getHost());

        settings.setHost("");
        Assert.assertEquals("woot", settings.getHost()); //Ignore empty string sets
    }
    
    @Test
    public void indexTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals("", settings.getIndex());

        settings.setIndex("woot");
        Assert.assertEquals("woot", settings.getIndex());

        settings.setIndex("");
        Assert.assertEquals("woot", settings.getIndex()); //Ignore empty string sets
    }
    
    @Test 
    public void sourceTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals("", settings.getSource());

        settings.setSource("woot");
        Assert.assertEquals("woot", settings.getSource());

        settings.setSource("");
        Assert.assertEquals("woot", settings.getSource()); //Ignore empty string sets
    }
    
    @Test 
    public void sourcetypeTest() {
        ConnectionSettings settings = connection.getSettings();

        Assert.assertEquals("", settings.getSourcetype());

        settings.setSourcetype("woot");
        Assert.assertEquals("woot", settings.getSourcetype());

        settings.setSourcetype("");
        Assert.assertEquals("woot", settings.getSourcetype()); //Ignore empty string sets
    }
    
    @Override
    protected int getNumEventsToSend() {
        return 1;
    }
}