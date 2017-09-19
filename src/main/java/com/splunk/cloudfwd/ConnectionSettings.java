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

import static com.splunk.cloudfwd.PropertyKeys.ACK_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.REQUIRED_KEYS;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.http.Endpoints;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class ConnectionSettings {
    private static final Logger LOG = ConnectionImpl.getLogger(ConnectionSettings.class.getName());
    protected Properties defaultProps = new Properties();
    protected Properties overrides;
    protected ConnectionImpl connection;

    public ConnectionSettings(Connection c, Properties overrides) {
        this.overrides = overrides;
        this.connection = (ConnectionImpl)c;
        this.parsePropertiesFile();       
    }

    public ConnectionSettings(Connection c) {
        this.connection = (ConnectionImpl)c;
        this.parsePropertiesFile();
    }

    public void putProperty(String k, String v) {
        this.defaultProps.put(k, v);
    }

    public List<URL> getUrls() {
        return urlsStringToList(defaultProps.getProperty(
                PropertyKeys.COLLECTOR_URI));
    }

    // Compares if the first URL matches Cloud>Trail domain (cloud.splunk.com)
    public boolean isCloudInstance() {
        return getUrls().get(0).toString().trim().
                matches("^.+\\.cloud\\.splunk\\.com.*$");
    }

    public int getChannelsPerDestination() {
        int n = Integer.parseInt(defaultProps.getProperty(
                PropertyKeys.CHANNELS_PER_DESTINATION,
                PropertyKeys.DEFAULT_CHANNELS_PER_DESTINATION).trim());
        if (n < 1) {
            // FIXME: EP: Do we actually want to allow creating 2,147,483,647 channels PER destination ?!
            n = Integer.MAX_VALUE; //effectively no limit by default
        }
        return n;
    }

    public long getUnresponsiveChannelDecomMS() {
        long t = Long.parseLong(defaultProps.getProperty(
                PropertyKeys.UNRESPONSIVE_MS,
                PropertyKeys.DEFAULT_UNRESPONSIVE_MS).trim());
        if (t < 1) {
            LOG.debug(PropertyKeys.UNRESPONSIVE_MS + ": unlimited");
        }
        return t;
    }

    public long getAckPollMS() {
        long interval = Long.parseLong(defaultProps.getProperty(
                PropertyKeys.ACK_POLL_MS,
                PropertyKeys.DEFAULT_ACK_POLL_MS).trim());
        if (interval <= 0) {
            interval = PropertyKeys.MIN_ACK_POLL_MS;
        }
        return interval;
    }

    public long getHealthPollMS() {
        long interval = Long.parseLong(defaultProps.getProperty(
                PropertyKeys.HEALTH_POLL_MS,
                PropertyKeys.DEFAULT_HEALTH_POLL_MS).trim());
        if (interval <= 0) {
            interval = PropertyKeys.MIN_HEALTH_POLL_MS;
        }
        return interval;
    }

    public int getMaxTotalChannels() {
        int max = Integer.parseInt(defaultProps.getProperty(
                PropertyKeys.MAX_TOTAL_CHANNELS,
                PropertyKeys.DEFAULT_MAX_TOTAL_CHANNELS).trim()); //default no limit
        if (max < 1) {
            max = Integer.MAX_VALUE; //effectively no limit by default
        }
        return max;
    }

    public int getMaxUnackedEventBatchPerChannel() {
        int max = Integer.parseInt(defaultProps.getProperty(
                PropertyKeys.MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL,
                PropertyKeys.DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL).
                trim());
        if (max < PropertyKeys.MIN_UNACKED_EVENT_BATCHES_PER_CHANNEL) {
            max = 10000;
        }
        return max;
    }

    public int getEventBatchSize() {
        int max = Integer.parseInt(defaultProps.getProperty(
                PropertyKeys.EVENT_BATCH_SIZE,
                PropertyKeys.DEFAULT_EVENT_BATCH_SIZE).trim());
        if (max < 1) {
            max = PropertyKeys.MIN_EVENT_BATCH_SIZE;
        }
        return max;
    }

    public long getChannelDecomMS() {
        long decomMs = Long.parseLong(defaultProps.getProperty(
                PropertyKeys.CHANNEL_DECOM_MS,
                PropertyKeys.DEFAULT_DECOM_MS).trim());
        if (decomMs <= 1) {
            return -1;
        }
        if (decomMs < PropertyKeys.MIN_DECOM_MS && !isMockHttp()) {
            LOG.warn(
                    "Ignoring setting for " + PropertyKeys.CHANNEL_DECOM_MS + " because it is less than minimum acceptable value: " + PropertyKeys.MIN_DECOM_MS);
            decomMs = PropertyKeys.MIN_DECOM_MS;
        }
        return decomMs;
    }

    public long getAckTimeoutMS() {
        long timeout = Long.parseLong(defaultProps.getProperty(
                PropertyKeys.ACK_TIMEOUT_MS,
                PropertyKeys.DEFAULT_ACK_TIMEOUT_MS).trim());
        if (timeout <= 0) {
            timeout = Long.MAX_VALUE;
        } else if (timeout < PropertyKeys.MIN_ACK_TIMEOUT_MS) {
            LOG.warn(
                    PropertyKeys.ACK_TIMEOUT_MS + " was set to a potentially too-low value: " + timeout);
        }
        return timeout;
    }

    public long getBlockingTimeoutMS() {
        long timeout = Long.parseLong(defaultProps.getProperty(
                PropertyKeys.BLOCKING_TIMEOUT_MS,
                PropertyKeys.DEFAULT_BLOCKING_TIMEOUT_MS).trim());
        if (timeout < 0) {
            throw new IllegalArgumentException(
                    PropertyKeys.BLOCKING_TIMEOUT_MS + " must be positive.");
        }
        return timeout;
    }

    public Endpoints getSimulatedEndpoints() {
        String classname = this.defaultProps.getProperty(
                PropertyKeys.MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints");
        try {
            return (Endpoints) Class.forName(classname).newInstance();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public boolean isCertValidationDisabled() {
        return Boolean.parseBoolean(this.defaultProps.getProperty(
                PropertyKeys.DISABLE_CERT_VALIDATION,
                "false").trim());
    }

    public boolean enabledHttpDebug() {
        return Boolean.parseBoolean(this.defaultProps.getProperty(
                PropertyKeys.ENABLE_HTTP_DEBUG,
                "false").trim());
    }

    /**
     *
     * @return
     */
    public String getSSLCertContent() {
        if (isCloudInstance()) {
            return defaultProps.getProperty(PropertyKeys.CLOUD_SSL_CERT_CONTENT).
                    trim();
        }
        return defaultProps.getProperty(PropertyKeys.SSL_CERT_CONTENT).trim();
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
    }

    public int getMaxRetries() {
        int max = Integer.parseInt(defaultProps.
                getProperty(PropertyKeys.RETRIES,
                        PropertyKeys.DEFAULT_RETRIES).trim());
        if (max < 1) {
            LOG.debug(PropertyKeys.RETRIES + ": unlimited");
            max = Integer.MAX_VALUE;
        }
        return max;
    }

    public boolean isCheckpointEnabled() {
        return Boolean.parseBoolean(this.defaultProps.getProperty(
                PropertyKeys.ENABLE_CHECKPOINTS,
                PropertyKeys.DEFAULT_ENABLE_CHECKPOINTS).trim());
    }

    public ConnectionImpl.HecEndpoint getHecEndpointType() {
        ConnectionImpl.HecEndpoint endpoint;
        String type = defaultProps.getProperty(PropertyKeys.HEC_ENDPOINT_TYPE,
                PropertyKeys.DEFAULT_HEC_ENDPOINT_TYPE).trim();
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

    public void setHecEndpointType(
            ConnectionImpl.HecEndpoint type) {
        if (type == ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT) {
            defaultProps.put(PropertyKeys.HEC_ENDPOINT_TYPE, "event");
        } else {
            defaultProps.put(PropertyKeys.HEC_ENDPOINT_TYPE, "raw");
        }
    }

    public Properties getDiff(Properties props) {
        Properties diff = new Properties();
        diff.putAll(defaultProps);
        diff.putAll(props);
        diff.entrySet().removeAll(defaultProps.entrySet());
        return diff;
    }

    public String getToken() {
        if (defaultProps.getProperty(PropertyKeys.TOKEN) == null) {
            throw new HecConnectionStateException(
                    "HEC token missing from Connection configuration. " + "See PropertyKeys.TOKEN",
                    HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
        return defaultProps.getProperty(PropertyKeys.TOKEN);
    }

    public List<URL> urlsStringToList(String urlsListAsString) {
        List<URL> urlList = new ArrayList<>();
        String[] splits = urlsListAsString.split(",");
        for (String urlString : splits) {
            try {
                URL url = new URL(urlString.trim());
                urlList.add(url);
            } catch (MalformedURLException ex) {
                LOG.error(ex.getMessage(), ex);
                throw new RuntimeException(ex);
            }
        }
        urlList.sort(Comparator.comparing(URL::toString));
        return urlList;
    }

    protected boolean isMockHttp() {
        return Boolean.parseBoolean(this.defaultProps.getProperty(
                PropertyKeys.MOCK_HTTP_KEY,
                "false").trim());
    }

    /**
     * @param numChars the size of the EventBatchImpl in characters (not bytes)
     */
    public void setEventBatchSize(int numChars) {
        putProperty(PropertyKeys.EVENT_BATCH_SIZE, String.
                valueOf(numChars));
    }

    /**
     * Use this method to change multiple settings on the connection. See
     * PropertyKeys class for more information.
     *
     * @param props
     */
    public void setProperties(Properties props) {
        Properties diffs = getDiff(props);
        boolean refreshChannels = false;
        boolean dnsLookup = false;

        for (String key : diffs.stringPropertyNames()) {
            switch (key) {
                case PropertyKeys.ACK_TIMEOUT_MS:
                    setAckTimeoutMS(Long.parseLong(diffs.getProperty(key)));
                    break;
                case PropertyKeys.COLLECTOR_URI:
                    putProperty(PropertyKeys.COLLECTOR_URI,
                            diffs.getProperty(key));
                    dnsLookup = true;
                    refreshChannels = true;
                    break;
                case PropertyKeys.TOKEN:
                    putProperty(PropertyKeys.TOKEN,
                            diffs.getProperty(key));
                    refreshChannels = true;
                    break;
                case PropertyKeys.HEC_ENDPOINT_TYPE:
                    putProperty(PropertyKeys.HEC_ENDPOINT_TYPE,
                            diffs.getProperty(key));
                    break;
                default:
                    LOG.warn("Attempt to change property not supported: " + key);
            }
        }
        if (refreshChannels) {
            connection.getLoadBalancer().refreshChannels(dnsLookup);
        }
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

  /**
   * Set urls to send to. See PropertyKeys.COLLECTOR_URI
   * for more information.
   * @param urls comma-separated list of urls
   */
  public void setUrls(String urls) {
    if (!urlsStringToList(urls).equals(
            getUrls())) {
      // a single url or a list of comma separated urls
      putProperty(PropertyKeys.COLLECTOR_URI, urls);
      connection.getLoadBalancer().refreshChannels(true);
    }
  }

  
    // All properties are populated by following order of precedence: 1) overrides, 2) lb.properties, then 3) defaults.
    private void parsePropertiesFile() {
        try {
            InputStream is = getClass().getResourceAsStream("/lb.properties");
            if (is != null) {
                defaultProps.load(is);
            }

            if (overrides != null) {
                defaultProps.putAll(overrides);
            }

            // If required properties are missing from lb.properties, overrides, and defaults, then throw exception.
            for (String key : REQUIRED_KEYS) {
                if (this.defaultProps.getProperty(key) == null) {
                    throw new HecMissingPropertiesException(
                            "Missing required key: " + key);
                }
            }

            // For any non-required properties, we allow them to remain null if they are not present in overrides
            // or lb.properties, because the property getters below will return the default values.
        } catch (IOException ex) {
            throw new HecIllegalStateException("Problem loading lb.properties",
                    HecIllegalStateException.Type.CANNOT_LOAD_PROPERTIES);
        }

    }

}
