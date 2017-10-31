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
    public void gettersTest() {
        // Test initial properties set by test.properties and ConnectionSettings defaults (test.properties takes precedence over defaults)
        ConnectionSettings settings = connection.getSettings();
        
        // From test.properties
        Assert.assertEquals("https://127.0.0.1:8088", settings.getUrlString());
        Assert.assertEquals(false, settings.isCloudInstance());
        Assert.assertEquals(4, settings.getChannelsPerDestination());
        Assert.assertEquals(-1, settings.getUnresponsiveMS());
        Assert.assertEquals(250, settings.getAckPollMS());
        Assert.assertEquals(5000, settings.getHealthPollMS());
        Assert.assertEquals(8, settings.getMaxTotalChannels());
        Assert.assertEquals(10000, settings.getMaxUnackedEventBatchPerChannel());
        Assert.assertEquals(0, settings.getEventBatchSize());
        Assert.assertEquals(60000, settings.getAckTimeoutMS());
        Assert.assertEquals(true, settings.isCertValidationDisabled());
        Assert.assertEquals(false, settings.isHttpDebugEnabled());
        Assert.assertEquals(false, settings.isCheckpointEnabled());
        Assert.assertEquals(true, settings.getTestPropertiesEnabled());

        // From PropertyKeys
        Assert.assertEquals(PropertyKeys.DEFAULT_DECOM_MS, settings.getChannelDecomMS());
        Assert.assertEquals(PropertyKeys.DEFAULT_CHANNEL_QUIESCE_TIMEOUT_MS, settings.getChannelQuiesceTimeoutMS());
        Assert.assertEquals(PropertyKeys.DEFAULT_BLOCKING_TIMEOUT_MS, settings.getBlockingTimeoutMS()); 
        Assert.assertEquals(PropertyKeys.DEFAULT_RETRIES, settings.getMaxRetries());
        Assert.assertEquals(PropertyKeys.DEFAULT_PREFLIGHT_RETRIES, settings.getMaxPreflightRetries());
        Assert.assertEquals(ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT, settings.getHecEndpointType());
        
        Assert.assertEquals("", settings.getToken());
        Assert.assertEquals("", settings.getSource());
        Assert.assertEquals("", settings.getSourcetype());
        Assert.assertEquals("", settings.getIndex());

        Assert.assertEquals("com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints", settings.getMockHttpClassname()); //default in ConnectionSettings
        
        Assert.assertEquals("localhost:8088", settings.getHost()); // Set by PropertiesFileHelper > createSender()
    }
    
    @Test
    public void settersTest() {
        ConnectionSettings settings = connection.getSettings(); // Initial values set by test.properties and ConnectionSettings defaults

        settings.setUrl("https://127.0.0.1:8089");
        Assert.assertEquals("https://127.0.0.1:8089", settings.getUrlString());
        
        settings.setChannelsPerDestination(7);
        Assert.assertEquals(7, settings.getChannelsPerDestination());

        settings.setChannelsPerDestination(0);
        Assert.assertEquals(PropertyKeys.DEFAULT_CHANNELS_PER_DESTINATION, settings.getChannelsPerDestination());
        
    }
    /*
    Test:
    getUrls, urlStringToList, getUrlWithAutoAssignedPorts, getSimulatedEndpoints
     */

    @Override
    protected int getNumEventsToSend() {
        return 1;
    }
}