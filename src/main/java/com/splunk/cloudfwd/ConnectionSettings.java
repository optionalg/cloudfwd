/*
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
package com.splunk.cloudfwd;

import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecMissingPropertiesException;
import com.splunk.cloudfwd.error.HecIllegalStateException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.http.Endpoints;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Enumeration;
import org.slf4j.Logger;

import static com.splunk.cloudfwd.PropertyKeys.*;

/**
 *
 * @author ghendrey
 */
public class ConnectionSettings {
    protected final Logger LOG;
    protected Properties defaultProps = new Properties();
    protected Properties overrides;
    protected ConnectionImpl connection;

    public ConnectionSettings(Connection c, Properties overrides) {
        this.overrides = overrides;
        this.connection = (ConnectionImpl)c;
        this.LOG = this.connection.getLogger(ConnectionSettings.class.getName());
        this.populateProperties();
    }

    public ConnectionSettings(Connection c) {
        this.connection = (ConnectionImpl)c;
        this.LOG = this.connection.getLogger(ConnectionSettings.class.getName());
        this.populateProperties();
    }

    /* ***************************** UTIL ******************************* */

    private int parsePropertyInt(String key) {
        return Integer.parseInt(defaultProps.getProperty(key).trim());
    }

    private long parsePropertyLong(String key) {
        return Long.parseLong(defaultProps.getProperty(key).trim());
    }

    private boolean parsePropertyBoolean(String key) {
        return Boolean.parseBoolean(defaultProps.getProperty(key).trim());
    }

    //TODO: do we want to return a default if no value is set? Or do we return null and enforce that user sets the value?
    private int parsePropertyInt(String key, String defaultVal) {
        return Integer.parseInt(defaultProps.getProperty(key, defaultVal).trim());
    }

    private long parsePropertyLong(String key, String defaultVal) {
        return Long.parseLong(defaultProps.getProperty(key, defaultVal).trim());
    }

    private boolean parsePropertyBoolean(String key, String defaultVal) {
        return Boolean.parseBoolean(defaultProps.getProperty(key, defaultVal).trim());
    }


    /* ***************************** GETTERS ******************************* */
    /*
       If property value is undefined, then return the default. Otherwise, return property value.
       If set, the property value has already been validated by the property setter at init of ConnectionImpl
       when the cloudfwd.properties file or overrides object have been loaded into the defaultProps object.
     */


    public Properties getDiff(Properties props) {
        Properties diff = new Properties();
        diff.putAll(defaultProps);
        diff.putAll(props);
        diff.entrySet().removeAll(defaultProps.entrySet());
        return diff;
    }

    public List<URL> getUrls() {
        return urlsStringToList(defaultProps.getProperty(
                PropertyKeys.COLLECTOR_URI));
    }

    protected List<URL> urlsStringToList(String urlsListAsString) {
        List<URL> urlList = new ArrayList<>();
        String[] splits = urlsListAsString.split(",");
        URL url = null;
        String urlString = null;
        for (int i=0;i<splits.length;i++) {
            urlString = splits[i];
            try {
                url =getUrlWithAutoAssignedPorts(urlString);
                urlList.add(url);
            } catch (MalformedURLException ex) {
                String msg = "url:'"+urlString+"',  "+ ex.getLocalizedMessage() ;
                HecConnectionStateException e = new HecConnectionStateException(msg,
                        HecConnectionStateException.Type.CONFIGURATION_EXCEPTION, ex);
                connection.getCallbacks().systemError(e);
                LOG.error(e.getMessage(), e);
                throw e;
            }
        }
        urlList.sort(Comparator.comparing(URL::toString));
        return urlList;
    }

    private URL getUrlWithAutoAssignedPorts(String urlString) throws MalformedURLException {
        URL url = new URL(urlString.trim());
        if(!url.getProtocol().equals("https")){
            throw new HecConnectionStateException("protocol '"+url.getProtocol()+ "' is not supported. Use 'https'.",
                    HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
        if (url.getPort() == -1) {
            int port = 443;
            LOG.warn("No port provided for url: " + urlString.trim()
                    + ". Defaulting to port " + port);
            return new URL(url.getProtocol(), url.getHost(), port, url.getFile());
        }
        return url;
    }

    // Compares if the first URL matches Cloud>Trail domain (cloud.splunk.com)
    public boolean isCloudInstance() {
        return getUrls().get(0).toString().trim().
                matches("^.+\\.cloud\\.splunk\\.com.*$");
    }

    public int getChannelsPerDestination() {
        return parsePropertyInt(CHANNELS_PER_DESTINATION, DEFAULT_CHANNELS_PER_DESTINATION);
    }

    public long getUnresponsiveChannelDecomMS() {
        return parsePropertyLong(UNRESPONSIVE_MS, DEFAULT_UNRESPONSIVE_MS);
    }

    public long getAckPollMS() {
        return parsePropertyLong(ACK_POLL_MS, DEFAULT_ACK_POLL_MS);
    }

    public long getHealthPollMS() {
        return parsePropertyLong(HEALTH_POLL_MS, DEFAULT_HEALTH_POLL_MS);
    }

    public int getMaxTotalChannels() {
        return parsePropertyInt(MAX_TOTAL_CHANNELS, DEFAULT_MAX_TOTAL_CHANNELS);
    }

    public int getMaxUnackedEventBatchPerChannel() {
        return parsePropertyInt(MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL);
    }

    public int getEventBatchSize() {
        return parsePropertyInt(EVENT_BATCH_SIZE, DEFAULT_EVENT_BATCH_SIZE);
    }

    public long getChannelDecomMS() {
        return parsePropertyLong(CHANNEL_DECOM_MS, DEFAULT_DECOM_MS);
    }

    public long getAckTimeoutMS() {
        return parsePropertyLong(ACK_TIMEOUT_MS, DEFAULT_ACK_TIMEOUT_MS);
    }

    public long getBlockingTimeoutMS() {
        return parsePropertyLong(BLOCKING_TIMEOUT_MS, DEFAULT_BLOCKING_TIMEOUT_MS);
    }

    public Endpoints getSimulatedEndpoints() {
        String classname = this.defaultProps.getProperty(MOCK_HTTP_CLASSNAME);
        try {
            return (Endpoints) Class.forName(classname).newInstance();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public boolean isCertValidationDisabled() {
        return parsePropertyBoolean(DISABLE_CERT_VALIDATION, "false");
    }

    public boolean enabledHttpDebug() {
        return parsePropertyBoolean(ENABLE_HTTP_DEBUG, "false");
    }

    public boolean isCheckpointEnabled() {
        return parsePropertyBoolean(ENABLE_CHECKPOINTS, DEFAULT_ENABLE_CHECKPOINTS);
    }

    public String getSSLCertContent() {
        String certKey = SSL_CERT_CONTENT;
        if (isCloudInstance()) {
            certKey = CLOUD_SSL_CERT_CONTENT;
        }

        String sslCertContent = defaultProps.getProperty(certKey);
        if (sslCertContent != null) {
            return sslCertContent.trim();
        }
        return "";
    }

    public int getMaxRetries() {
        return parsePropertyInt(RETRIES, DEFAULT_RETRIES);
    }
    
    public int getMaxPreflightRetries() {
        return parsePropertyInt(PREFLIGHT_RETRIES, DEFAULT_PREFLIGHT_RETRIES);
    }    

    public ConnectionImpl.HecEndpoint getHecEndpointType() {
        ConnectionImpl.HecEndpoint endpoint;
        String type = defaultProps.getProperty(HEC_ENDPOINT_TYPE,
                DEFAULT_HEC_ENDPOINT_TYPE).trim();
        if (type.equals("raw")) {
            endpoint = ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT;
        } else if (type.equals("event")) {
            endpoint = ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT;
        } else {
            LOG.warn(
                    "Unrecognized HEC Endpoint type. Defaulting to " + PropertyKeys.DEFAULT_HEC_ENDPOINT_TYPE + ". See PropertyKeys.HEC_ENDPOINT_TYPE.");
            endpoint = ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT;
        }
        return endpoint;
    }

    public String getToken() {
        if (defaultProps.getProperty(TOKEN) == null) {
            throw new HecConnectionStateException(
                    "HEC token missing from Connection configuration. " + "See PropertyKeys.TOKEN",
                    HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
        return defaultProps.getProperty(TOKEN);
    }

    protected boolean isMockHttp() {
        return parsePropertyBoolean(MOCK_HTTP_KEY, "false");
    }




    /* ***************************** SETTERS ******************************* */

    private void putProperty(String k, String v) {
        //TODO: validate property key is part of PropertyKeys AND if available, call that key's setter method that validates value
        this.defaultProps.put(k, v);
    }

    /**
     * Use this method to change multiple settings on the connection. See
     * PropertyKeys class for more information.
     *
     * @param props
     */
    public void setProperties(Properties props) throws UnknownHostException {
        Properties diffs = getDiff(props);
        boolean refreshChannels = false;
        boolean dnsLookup = false;

        for (String key : diffs.stringPropertyNames()) {
            switch (key) {
                case ACK_TIMEOUT_MS:
                    setAckTimeoutMS(Long.parseLong(diffs.getProperty(key)));
                    break;
                case COLLECTOR_URI:
                    putProperty(COLLECTOR_URI,
                            diffs.getProperty(key));
                    dnsLookup = true;
                    refreshChannels = true;
                    break;
                case TOKEN:
                    putProperty(TOKEN,
                            diffs.getProperty(key));
                    refreshChannels = true;
                    break;
                case HEC_ENDPOINT_TYPE:
                    putProperty(HEC_ENDPOINT_TYPE,
                            diffs.getProperty(key));
                    break;
                default:
                    LOG.warn("Attempt to change property not supported: " + key);
            }
        }
        if (refreshChannels) {
            connection.getLoadBalancer().refreshChannels(dnsLookup, true);
        }
    }

    //TODO: write unit tests for each setter

    //TODO: String args into all setters?
    public void setChannelsPerDestination(int numChannels) {
        if (numChannels < 1) {
            int was = numChannels;
            numChannels = Integer.parseInt(DEFAULT_CHANNELS_PER_DESTINATION);
            LOG.debug("{}, defaulting {} to {}", was, CHANNELS_PER_DESTINATION, numChannels);
        }
        putProperty(CHANNELS_PER_DESTINATION, String.valueOf(numChannels));
    }

    public void setUnresponsiveChannelDemonMS(long decomMS) {
        if (decomMS < 1) {
            long was = decomMS;
            decomMS = Long.parseLong(DEFAULT_UNRESPONSIVE_MS);
            LOG.debug("{}, defaulting {} to {}", was, UNRESPONSIVE_MS, decomMS);
        }
        putProperty(UNRESPONSIVE_MS, String.valueOf(decomMS));
    }

    public void setAckPollMS(long pollMS) {
        if (pollMS <= 0) {
            long was = pollMS;
            pollMS = MIN_ACK_POLL_MS;
            LOG.debug("{}, defaulting {} to smallest allowed value of {}", was, ACK_POLL_MS, pollMS);
        }
        putProperty(ACK_POLL_MS, String.valueOf(pollMS));
    }

    public void setHealthPollMS(long pollMS) {
        if (pollMS <= 0) {
            long was = pollMS;
            pollMS = MIN_HEALTH_POLL_MS;
            LOG.debug("{}, defaulting {} to smallest allowed value {}", was, HEALTH_POLL_MS, pollMS);
        }
        putProperty(HEALTH_POLL_MS, String.valueOf(pollMS));
    }

    public void setMaxTotalChannels(int totalChannels) {
        if (totalChannels < 1) {
            int was = totalChannels;
            totalChannels = Integer.MAX_VALUE; //effectively no limit by default
            LOG.debug("{}, defaulting {} to no-limit {}", was, MAX_TOTAL_CHANNELS, totalChannels);
        }
        putProperty(MAX_TOTAL_CHANNELS, String.valueOf(totalChannels));
    }

    public void setMaxUnackedEventBatchPerChannel(int batchesPerChannel) {
        int minimum = 10000;
        if (batchesPerChannel < MIN_UNACKED_EVENT_BATCHES_PER_CHANNEL) {
            int was = batchesPerChannel;
            batchesPerChannel = minimum;
            LOG.debug("{}, defaulting {} to smallest allowed value {}", was, MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, batchesPerChannel);
        }
        putProperty(MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, String.valueOf(batchesPerChannel));
    }

    /**
     * @param numChars the size of the EventBatchImpl in characters (not bytes)
     */
    public void setEventBatchSize(int numChars) {
        if (numChars < 1) {
            int was = numChars;
            numChars = MIN_EVENT_BATCH_SIZE;
            LOG.debug("{}, defaulting {} to smallest allowed value {}", was, EVENT_BATCH_SIZE, numChars);
        }
        putProperty(EVENT_BATCH_SIZE, String.valueOf(numChars));
    }

    public void setChannelDecomMS(long decomMS) {
        if (decomMS <= 1) {
            long was = decomMS;
            decomMS = -1;
            LOG.debug("{}, defaulting {} to no-limit {}", was, EVENT_BATCH_SIZE, decomMS);
        }
        if (decomMS < MIN_DECOM_MS && !isMockHttp()) {
            LOG.warn(
                    "Ignoring setting for " + CHANNEL_DECOM_MS + " because it is less than minimum acceptable value: " + MIN_DECOM_MS);
            decomMS = MIN_DECOM_MS;
        }
        putProperty(CHANNEL_DECOM_MS, String.valueOf(decomMS));
    }

    public void setAckTimeoutsMS(long timeoutMS) {
        if (timeoutMS <= 0) {
            long was = timeoutMS;
            timeoutMS = Long.MAX_VALUE;
            LOG.debug("{}, defaulting {} to maximum allowed value {}", was, ACK_TIMEOUT_MS, timeoutMS);
        } else if (timeoutMS < MIN_ACK_TIMEOUT_MS) {
            LOG.warn(
                    ACK_TIMEOUT_MS + " was set to a potentially too-low value: " + timeoutMS);
        }
        putProperty(ACK_TIMEOUT_MS, String.valueOf(timeoutMS));
    }

    public void setBlockingTimeoutMS(long timeoutMS) {
        if (timeoutMS < 0) {
            throw new IllegalArgumentException(BLOCKING_TIMEOUT_MS + " must be positive.");
        }
        putProperty(BLOCKING_TIMEOUT_MS, String.valueOf(timeoutMS));
    }

    public void setSimulatedEndpoints(String endpoints) {
        putProperty(MOCK_HTTP_CLASSNAME, endpoints);
    }

    public void enableCertValidation() {
        putProperty(DISABLE_CERT_VALIDATION, "false");
    }

    public void disableCertValidation() {
        putProperty(DISABLE_CERT_VALIDATION, "true");
    }

    public void enableHttpDebug() {
        System.setProperty("org.apache.commons.logging.Log",
                "org.apache.commons.logging.impl.SimpleLog");
        System.setProperty("org.apache.commons.logging.simplelog.showdatetime",
                "true");
        System.setProperty(
                "org.apache.commons.logging.simplelog.log.httpclient.wire.header",
                "debug");
        System.setProperty(
                "org.apache.commons.logging.simplelog.log.org.apache.http",
                "debug");
        putProperty(ENABLE_HTTP_DEBUG, "true");
    }

    public void disableHttpDebug() {
        putProperty(ENABLE_HTTP_DEBUG, "false");
    }

    public void enableCheckpoint() {
        putProperty(ENABLE_CHECKPOINTS, "true");
    }

    public void disableCheckpoint() {
        putProperty(ENABLE_CHECKPOINTS, "false");
    }

    public void setSSLCertContent(String cert) {
        String certKey = SSL_CERT_CONTENT;
        if (isCloudInstance()) {
            certKey = CLOUD_SSL_CERT_CONTENT;
        }
        putProperty(certKey, cert);
    }

    public void setMaxRetries(int retries) {
        if (retries < 1) {
            int was = retries;
            retries = Integer.MAX_VALUE;
            LOG.debug("{}, defaulting {} to maximum allowed value {}", was, RETRIES, retries);
        }
        putProperty(RETRIES, String.valueOf(retries));
    }

    public void setMaxPreflightRetries(int retries) {
        if (retries < 1) {
            int was = retries;
            retries = Integer.MAX_VALUE;
            LOG.debug("{}, defaulting {} to maximum allowed value {}", was, PREFLIGHT_RETRIES, retries);
        }
    }

    public void setHecEndpointType(ConnectionImpl.HecEndpoint type) {
        String endpoint;
        if (type == ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT) {
            endpoint = "event";
        } else if (type == ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT) {
            endpoint = "raw";
        } else {
            LOG.warn(
                    "Unrecognized HEC Endpoint type. Defaulting to " + DEFAULT_HEC_ENDPOINT_TYPE + ". See PropertyKeys.HEC_ENDPOINT_TYPE.");
            endpoint = DEFAULT_HEC_ENDPOINT_TYPE;
        }
        putProperty(HEC_ENDPOINT_TYPE, endpoint);
    }

    /**
     * Set Http Event Collector token to use.
     * May take up to PropertyKeys.CHANNEL_DECOM_MS milliseconds
     * to go into effect.
     * @param token
     */
    public void setToken(String token) {
        if (!getToken().equals(token)) {
            putProperty(PropertyKeys.TOKEN, token);
            connection.getLoadBalancer().refreshChannels(false);
        }
    }

    public void enableMockHttp() {
        putProperty(MOCK_HTTP_KEY, "true");
    }

    public void disableMockHttp() {
        putProperty(MOCK_HTTP_KEY, "false");
    }

    /**
     * Set event acknowledgement timeout. See PropertyKeys.ACK_TIMEOUT_MS for
     * more information.
     *
     * @param ms
     */
    public synchronized void setAckTimeoutMS(long ms) {
        if (ms != getAckTimeoutMS()) {
            putProperty(ACK_TIMEOUT_MS, String.valueOf(ms));
            connection.getTimeoutChecker().setTimeout(ms);
        }
    }

  /**
   * Set urls to send to. See PropertyKeys.COLLECTOR_URI
   * for more information.
   * @param urls comma-separated list of urls
   */
  public void setUrls(String urls) throws UnknownHostException {
    if (!urlsStringToList(urls).equals(
            getUrls())) {
      // a single url or a list of comma separated urls
      putProperty(PropertyKeys.COLLECTOR_URI, urls);
      connection.getLoadBalancer().refreshChannels(true, true);
    }
  }



  
    // All properties are populated by following order of precedence: 1) overrides, 2) cloudfwd.properties, then 3) defaults.
    private void populateProperties() {
        try {
            InputStream is = getClass().getResourceAsStream("/cloudfwd.properties");
            if (is != null) {
                defaultProps.load(is); //TODO: confirm - does this parse all values into strings?
            }

            if (overrides != null) {
//                defaultProps.putAll(overrides); //TODO: should call setters instead of putProperty() to make sure we validate it
                Enumeration en = overrides.propertyNames();
                while(en.hasMoreElements()) {
                    String key = (String)en.nextElement();
                    putProperty(key, String.valueOf(overrides.getProperty(key))); // Enforce string type of property values
                }
            }

            // If required properties are missing from cloudfwd.properties, overrides, and defaults, then throw exception.
            for (String key : REQUIRED_KEYS) {
                if (defaultProps.getProperty(key) == null) {
                    throw new HecMissingPropertiesException(
                            "Missing required key: " + key);
                }
            }

            // For any non-required properties, we allow them to remain null if they are not present in overrides
            // or cloudfwd.properties, because the property getters below will return the default values.
        } catch (IOException ex) {
            throw new HecIllegalStateException("Problem loading cloudfwd.properties",
                    HecIllegalStateException.Type.CANNOT_LOAD_PROPERTIES);
        }

    }

/*
User should be able to init Connection using ConnectionSettings
 */

}
