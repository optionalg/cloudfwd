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

import static com.splunk.cloudfwd.PropertyKeys.ACK_POLL_MS;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.splunk.cloudfwd.PropertyKeys.*;

/**
 *
 * @author ghendrey
 */
public class ConnectionSettings {
    protected Logger LOG;
    protected Logger GENERIC_LOG;
    protected ConnectionSettings overrides;
    protected ConnectionImpl connection;

    @JsonProperty("url")
    private String url;

    @JsonProperty("splunk_hec_token")
    private String splunkHecToken;

    @JsonProperty("splunk_hec_host")
    private String splunkHecHost;

    @JsonProperty("splunk_hec_index")
    private String splunkHecIndex;

    @JsonProperty("splunk_hec_source")
    private String splunkHecSource;
    
    @JsonProperty("splunk_hec_sourcetype")
    private String splunkHecSourcetype;
    
    @JsonProperty("ssl_cert_hostname_regex")
    private String sslCertHostnameRegex;

    @JsonProperty("hec_endpoint_type")
    private String hecEndpointType = DEFAULT_HEC_ENDPOINT_TYPE;
    
    @JsonProperty("enable_checkpoints")
    private Boolean enableCheckpoints = DEFAULT_ENABLE_CHECKPOINTS;

    @JsonProperty("disable_certificate_validation")
    private Boolean disableCertificateValidation = false;

    @JsonProperty("channels_per_dest")
    private int channelsPerDest = DEFAULT_CHANNELS_PER_DESTINATION;

    @JsonProperty("mock_http")
    private Boolean mockHttp = false;

    @JsonProperty("mock_force_url_map_to_one")
    private Boolean mockForceUrlMapToOne;

    @JsonProperty("enabled")
    private Boolean testPropertiesEnabled;

    @JsonProperty("unresponsive_channel_decom_ms")
    private long unresponsiveChannelDecomMS = DEFAULT_UNRESPONSIVE_MS;

    @JsonProperty("max_total_channels")
    private int maxTotalChannels = DEFAULT_MAX_TOTAL_CHANNELS;

    @JsonProperty("max_unacked_per_channel")
    private int maxUnackedPerChannel = DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL;

    @JsonProperty("event_batch_size")
    private int eventBatchSize = DEFAULT_EVENT_BATCH_SIZE;

    @JsonProperty("ack_poll_ms")
    private long ackPollMS = DEFAULT_ACK_POLL_MS;

    @JsonProperty("health_poll_ms")
    private long healthPollMS = DEFAULT_HEALTH_POLL_MS;

    @JsonProperty("channel_decom_ms")
    private long channelDecomMS = DEFAULT_DECOM_MS;
    
    @JsonProperty("channel_quiesce_timeout_ms")
    private long channelQuiesceTimeoutMS = DEFAULT_CHANNEL_QUIESCE_TIMEOUT_MS;

    @JsonProperty("ack_timeout_ms")
    private long ackTimeoutMS = DEFAULT_ACK_TIMEOUT_MS;

    @JsonProperty("blocking_timeout_ms")
    private long blockingTimeoutMS = DEFAULT_BLOCKING_TIMEOUT_MS;

    @JsonProperty("mock_http_classname")
    private String mockHttpClassname;
    
    @JsonProperty("preflight_timeout")
    private long preFlightTimeoutMS = DEFAULT_PREFLIGHT_TIMEOUT_MS;

    @JsonProperty("ssl_cert_content")
    private String sslCertContent;

    @JsonProperty("cloud_ssl_cert_content")
    private String cloudSslCertContent;

    @JsonProperty("enable_http_debug")
    private Boolean enableHttpDebug = false;

    @JsonProperty("max_retries")
    private int maxRetries = DEFAULT_RETRIES;

    @JsonProperty("max_preflight_retries")
    private int maxPreflightTries = DEFAULT_PREFLIGHT_RETRIES;
    
    @JsonProperty("event_batch_flush_timeout_ms")
    private long eventBatchFlushTimeout = DEFAULT_EVENT_BATCH_FLUSH_TIMEOUT_MS;

    @Deprecated
    /*
        Only used by deprecated Connections.create(ConnectionCallbacks, Properties) method
     */
    public static ConnectionSettings fromProps(Properties props) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.valueToTree(props);
        try {
            ConnectionSettings settings = mapper.readValue(node.toString(), ConnectionSettings.class);
            return settings;
        } catch (IOException e) {
            throw new RuntimeException("Could not map Properties object to ConnectionSettings object - please check Properties object.", e);
        }
    }
    
    public static ConnectionSettings fromPropsFile(String pathToFile) {
        // use Jackson to populate this ConnectionSettings instance from file
        JavaPropsMapper mapper = new JavaPropsMapper();
        try {
            InputStream inputStream = ConnectionSettings.class.getResourceAsStream(pathToFile);
            if (inputStream != null) {
                ConnectionSettings connectionSettings = mapper.readValue(inputStream, ConnectionSettings.class);
                
                return connectionSettings;
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not map Properties file to Java object - please check file path.", e);
        }

        return null;
    }

    public void setConnection(Connection c) {
        this.connection = (ConnectionImpl)c;
        this.LOG = this.connection.getLogger(ConnectionSettings.class.getName());
    }

    /* ***************************** UTIL ******************************* */

    protected <T> T applyDefaultIfNull(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    /* ***************************** GETTERS ******************************* */
    /*
       If property value is undefined, then return the default. Otherwise, return property value.
       If set, the property value has already been validated by the property setter at init of ConnectionImpl
       when the cloudfwd.properties file or overrides object have been loaded into the defaultProps object.
     */


    public List<URL> getUrls() {
        return urlsStringToList(this.url);
    }

    public String getUrlString() {
        return this.url;
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
                String msg = "url:'"+urlString+"',  "+ ex.getLocalizedMessage();
                HecConnectionStateException e = new HecConnectionStateException(msg,
                        HecConnectionStateException.Type.CONFIGURATION_EXCEPTION, ex);
                if (connection != null) {
                    connection.getCallbacks().systemError(e);
                }
                getLog().error(e.getMessage(), e);
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
            getLog().warn("No port provided for url: " + urlString.trim()
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
        return applyDefaultIfNull(this.channelsPerDest, DEFAULT_CHANNELS_PER_DESTINATION);
    }

    public long getUnresponsiveChannelDecomMS() {
        return applyDefaultIfNull(this.unresponsiveChannelDecomMS, DEFAULT_UNRESPONSIVE_MS);
    }

    public long getAckPollMS() {
        return applyDefaultIfNull(this.ackPollMS, DEFAULT_ACK_POLL_MS);
    }

    public long getHealthPollMS() {
        return applyDefaultIfNull(this.healthPollMS, DEFAULT_HEALTH_POLL_MS);
    }

    public int getMaxTotalChannels() {
        return applyDefaultIfNull(this.maxTotalChannels, DEFAULT_MAX_TOTAL_CHANNELS);
    }

    public int getMaxUnackedEventBatchPerChannel() {
        return applyDefaultIfNull(this.maxUnackedPerChannel, DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL);
    }

    public int getEventBatchSize() {
        return applyDefaultIfNull(this.eventBatchSize, DEFAULT_EVENT_BATCH_SIZE);
    }

    public long getChannelDecomMS() {
        return applyDefaultIfNull(this.channelDecomMS, DEFAULT_DECOM_MS);
    }
    
    public long getChannelQuiesceTimeoutMS() {
        return applyDefaultIfNull(this.channelQuiesceTimeoutMS, DEFAULT_CHANNEL_QUIESCE_TIMEOUT_MS);
    }

    public long getAckTimeoutMS() {
        return applyDefaultIfNull(this.ackTimeoutMS, DEFAULT_ACK_TIMEOUT_MS);
    }

    public long getBlockingTimeoutMS() {
        return applyDefaultIfNull(this.blockingTimeoutMS, DEFAULT_BLOCKING_TIMEOUT_MS);
    }
    
    public String getMockHttpClassname() {
        return applyDefaultIfNull(this.mockHttpClassname, "com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints");
    }

    public boolean isForcedUrlMapToSingleAddr() {
        return applyDefaultIfNull(this.mockForceUrlMapToOne, false);
    }


    public Endpoints getSimulatedEndpoints() {
        try {
            return (Endpoints) Class.forName(getMockHttpClassname()).newInstance();
        } catch (Exception ex) {
            getLog().error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public long getPreFlightTimeoutMS() {
        return applyDefaultIfNull(this.preFlightTimeoutMS, DEFAULT_PREFLIGHT_TIMEOUT_MS);
    }

    public boolean isCertValidationDisabled() {
        return applyDefaultIfNull(this.disableCertificateValidation, false);
    }

    public boolean isHttpDebugEnabled() {
        return applyDefaultIfNull(this.enableHttpDebug, false);
    }

    public boolean isCheckpointEnabled() {
        return applyDefaultIfNull(this.enableCheckpoints, DEFAULT_ENABLE_CHECKPOINTS);
    }

    public String getSSLCertContent() {
        String certKey = this.sslCertContent;
        if (isCloudInstance()) {
            certKey = this.cloudSslCertContent;
        }

        if (certKey != null) {
            return certKey.trim();
        }
        return "";
    }

    public int getMaxRetries() {
        return applyDefaultIfNull(this.maxRetries, DEFAULT_RETRIES);
    }

    public int getMaxPreflightRetries() {
        return applyDefaultIfNull(this.maxPreflightTries, DEFAULT_PREFLIGHT_RETRIES);
    }
    
    public long getEventBatchFlushTimeout() {
        return applyDefaultIfNull(this.eventBatchFlushTimeout, DEFAULT_EVENT_BATCH_FLUSH_TIMEOUT_MS);
    }

    public ConnectionImpl.HecEndpoint getHecEndpointType() {
        ConnectionImpl.HecEndpoint endpoint;
        String type = applyDefaultIfNull(this.hecEndpointType, DEFAULT_HEC_ENDPOINT_TYPE);
        if (type.equals("raw")) {
            endpoint = ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT;
        } else if (type.equals("event")) {
            endpoint = ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT;
        } else {
            getLog().warn(
                    "Unrecognized HEC Endpoint type. Defaulting to " + PropertyKeys.DEFAULT_HEC_ENDPOINT_TYPE + ". See PropertyKeys.HEC_ENDPOINT_TYPE.");
            endpoint = ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT;
        }
        return endpoint;
    }

    public String getToken() {
        if (this.splunkHecToken == null) {
            throw new HecConnectionStateException(
                    "HEC token missing from Connection configuration. " + "See PropertyKeys.TOKEN",
                    HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
        return this.splunkHecToken;
    }

    public boolean isMockHttp() {
        return applyDefaultIfNull(this.mockHttp, false);
    }


    public Boolean getTestPropertiesEnabled() {
        return applyDefaultIfNull(this.testPropertiesEnabled, false);
    }

    public String getHost() {
        return this.splunkHecHost;
    }

    public String getSource() {
        return this.splunkHecSource;
    }

    public String getSourcetype() {
        return this.splunkHecSourcetype;
    }

    public String getIndex() {
        return this.splunkHecIndex;
    }

    /* ***************************** SETTERS ******************************* */
    // Setters should not throw Exceptions or call callbacks on Connection instance
    // because they may be called before the Connection is instantiated.
    // Any setter behavior that should happen on the Connection instance should be defined
    // in ConnectionSettings > setConnection() method (or in Connections > create())

    public void setChannelsPerDestination(int numChannels) {
        if (numChannels < 1) {
            int was = numChannels;
            numChannels = DEFAULT_CHANNELS_PER_DESTINATION;
            getLog().debug("{}, defaulting {} to {}", was, CHANNELS_PER_DESTINATION, numChannels);
            this.channelsPerDest = DEFAULT_CHANNELS_PER_DESTINATION;
        }
        this.channelsPerDest = numChannels;
    }

    public void setUnresponsiveMS(long decomMS) {
        if (decomMS < 1) {
            long was = decomMS;
            decomMS = DEFAULT_UNRESPONSIVE_MS;
            getLog().debug("{}, defaulting {} to {}", was, UNRESPONSIVE_MS, decomMS);
            this.unresponsiveChannelDecomMS = DEFAULT_UNRESPONSIVE_MS;
        }
        this.unresponsiveChannelDecomMS = decomMS;
    }

    public void setAckPollMS(long pollMS) {
        if (pollMS <= 0) {
            long was = pollMS;
            pollMS = MIN_ACK_POLL_MS;
            getLog().debug("{}, defaulting {} to smallest allowed value of {}", was, ACK_POLL_MS, pollMS);
        }
        this.ackPollMS = pollMS;
    }

    public void setHealthPollMS(long pollMS) {
        if (pollMS <= 0) {
            long was = pollMS;
            pollMS = MIN_HEALTH_POLL_MS;
            getLog().debug("{}, defaulting {} to smallest allowed value {}", was, HEALTH_POLL_MS, pollMS);
        }
        this.healthPollMS = pollMS;
    }

    public void setMaxTotalChannels(int totalChannels) {
        if (totalChannels < 1) {
            int was = totalChannels;
            totalChannels = Integer.MAX_VALUE; //effectively no limit by default
            getLog().debug("{}, defaulting {} to no-limit {}", was, MAX_TOTAL_CHANNELS, totalChannels);
        }
        this.maxTotalChannels = totalChannels;
    }

    public void setMaxUnackedEventBatchPerChannel(int batchesPerChannel) {
        int max = 10000;
        if (batchesPerChannel < MIN_UNACKED_EVENT_BATCHES_PER_CHANNEL) {
            int was = batchesPerChannel;
            batchesPerChannel = max;
            getLog().debug("{}, defaulting {} to no limit {}", was, MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, batchesPerChannel);
        }
        this.maxUnackedPerChannel = batchesPerChannel;
    }

    /**
     * @param numChars the size of the EventBatchImpl in characters (not bytes)
     */
    public void setEventBatchSize(int numChars) {
        if (numChars < 1) {
            int was = numChars;
            numChars = MIN_EVENT_BATCH_SIZE;
            getLog().debug("{}, defaulting {} to smallest allowed value {}", was, EVENT_BATCH_SIZE, numChars);
        }
        this.eventBatchSize = numChars;
    }

    public void setChannelDecomMS(long decomMS) {
        if (decomMS <= 1) {
            long was = decomMS;
            decomMS = -1;
            getLog().debug("{}, defaulting {} to no-limit {}", was, CHANNEL_DECOM_MS, decomMS);
        } else if (decomMS < MIN_DECOM_MS && !isMockHttp()) {
            getLog().warn(
                    "Ignoring setting for " + CHANNEL_DECOM_MS + " because it is less than minimum acceptable value: " + MIN_DECOM_MS);
            decomMS = MIN_DECOM_MS;
        }
        this.channelDecomMS = decomMS;
    }
    
    public void setChannelQuiesceTimeoutMS(long timeoutMS) {
        if (timeoutMS < PropertyKeys.MIN_CHANNEL_QUIESCE_TIMEOUT_MS && !isMockHttp()) {
            LOG.warn(PropertyKeys.CHANNEL_QUIESCE_TIMEOUT_MS +
                    " was set to a potentially too-low value, reset to min value: " + timeoutMS);
            this.channelQuiesceTimeoutMS = MIN_CHANNEL_QUIESCE_TIMEOUT_MS;
        } else {
            this.channelQuiesceTimeoutMS = timeoutMS;
        }
    }

    public void setAckTimeoutMS(long timeoutMS) {
        if (timeoutMS != getAckTimeoutMS()) {
            if (timeoutMS <= 0) {
                long was = timeoutMS;
                timeoutMS = Long.MAX_VALUE;
                getLog().debug("{}, defaulting {} to maximum allowed value {}", was, ACK_TIMEOUT_MS, timeoutMS);
            } else if (timeoutMS < MIN_ACK_TIMEOUT_MS && !isMockHttp()) {
                getLog().warn(ACK_TIMEOUT_MS + " was set to a potentially too-low value, reset to min value: " + timeoutMS);
                timeoutMS = MIN_ACK_TIMEOUT_MS;
            }
            this.ackTimeoutMS = timeoutMS;
            if (connection != null) {
                connection.getTimeoutChecker().setTimeout();
            }
        }
    }

    public void setBlockingTimeoutMS(long timeoutMS) {
        if (timeoutMS < 0) {
            throw new IllegalArgumentException(BLOCKING_TIMEOUT_MS + " must be positive.");
        }
        this.blockingTimeoutMS = timeoutMS;
    }

    public void setMockHttpClassname(String endpoints) {
        this.mockHttpClassname = endpoints;
    }

    public void setMockForceUrlMapToOne(Boolean force) {
        this.mockForceUrlMapToOne = force;
    }
    
    public void setPreFlightTimeoutMS(long timeoutMS) {
        if (timeoutMS <= 0) {
            throw new IllegalArgumentException(
                    PropertyKeys.PREFLIGHT_TIMEOUT_MS + " must be greater than zero.");
        }
        this.preFlightTimeoutMS = timeoutMS;
    }

    public void disableCertValidation() {
        this.disableCertificateValidation = true;
    }
    
    public void enableCertValidation() {
        this.disableCertificateValidation = false;
    }

    public void setHttpDebugEnabled(Boolean mode) {
        if (mode == true) {
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
        this.enableHttpDebug = mode;
    }

    public void setCheckpointEnabled(Boolean mode) {
        this.enableCheckpoints = mode;
    }

    public void setSSLCertContent(String cert) {
        if (isCloudInstance()) {
            this.cloudSslCertContent = cert;
        } else {
            this.sslCertContent = cert;
        }
    }

    public void setMaxRetries(int retries) {
        if (retries < 1) {
            int was = retries;
            retries = Integer.MAX_VALUE;
            getLog().debug("{}, defaulting {} to maximum allowed value {}", was, RETRIES, retries);
        }
        this.maxRetries = retries;
    }

    public void setMaxPreflightRetries(int retries) {
        if (retries < 1) {
            int was = retries;
            retries = Integer.MAX_VALUE;
            getLog().debug("{}, defaulting {} to maximum allowed value (unlimited) {}", was, PREFLIGHT_RETRIES, retries);
        }
        this.maxPreflightTries = retries;
    }
    
    public void setEventBatchFlushTimeout(long timeoutMS) {
        if (timeoutMS <= 0) {
            timeoutMS = DEFAULT_EVENT_BATCH_FLUSH_TIMEOUT_MS;
            getLog().warn("Property {} must be greater than 0. Using default value of {}",
                    EVENT_BATCH_FLUSH_TIMEOUT_MS, DEFAULT_EVENT_BATCH_FLUSH_TIMEOUT_MS);
        }
        this.eventBatchFlushTimeout = timeoutMS;
    }

    @Deprecated
    /**
     * Allows serializated hecEndpointType to be set by mapper in ConnectionSettings.fromProps()
     */
    public void setHecEndpointType(String type) {
        String endpoint;
        if (type != this.hecEndpointType) {
            if ("event".equals(type) || "raw".equals(type)) {
                endpoint = type;
            } else {
                getLog().warn(
                        "Unrecognized HEC Endpoint type. Defaulting to " + DEFAULT_HEC_ENDPOINT_TYPE + ". See PropertyKeys.HEC_ENDPOINT_TYPE.");
                endpoint = DEFAULT_HEC_ENDPOINT_TYPE;
            }
            this.hecEndpointType = endpoint;
        }
    }

    public void setHecEndpointType(ConnectionImpl.HecEndpoint type) {
        String endpoint;
        if (type != getHecEndpointType()) {
            if (type == ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT) {
                endpoint = "event";
            } else if (type == ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT) {
                endpoint = "raw";
            } else {
                getLog().warn(
                        "Unrecognized HEC Endpoint type. Defaulting to " + DEFAULT_HEC_ENDPOINT_TYPE + ". See PropertyKeys.HEC_ENDPOINT_TYPE.");
                endpoint = DEFAULT_HEC_ENDPOINT_TYPE;
            }
            this.hecEndpointType = endpoint;
        }
    }

    /**
     * Set Http Event Collector token to use.
     * May take up to PropertyKeys.CHANNEL_DECOM_MS milliseconds
     * to go into effect.
     * @param token
     */
    public void setToken(String token) {
        if (!token.equals(this.splunkHecToken)) {
            this.splunkHecToken = token;
            checkAndRefreshChannels();
        }
    }

    public void setMockHttp(Boolean mode) {
        this.mockHttp = mode;
    }

  /**
   * Set urls to send to. See PropertyKeys.COLLECTOR_URI
   * for more information.
   * @param urls comma-separated list of urls
   */
  public void setUrls(String urls) {
      if (connection != null && !urlsStringToList(urls).equals(getUrls())) {
          this.url = urls;
          checkAndRefreshChannels();
      } else {
          this.url = urls;
      }
  }

  public void setTestPropertiesEnabled(Boolean enabled) {
      this.testPropertiesEnabled = enabled;
  }
  
    /**
     * Set Host value for the data feed
     * @param host Host value for the data feed
     */
  public void setHost(String host) {
      if (!StringUtils.isEmpty(host) && !host.equals(getHost())) {
          this.splunkHecHost = host;
          checkAndRefreshChannels();
      }
  }

    /**
     * Set Splunk index in which the data feed is stored
     * @param index The Splunk index in which the data feed is stored
     */
  public void setIndex(String index) {
      if (!StringUtils.isEmpty(index) && !index.equals(getIndex())) {
          this.splunkHecIndex = index;
          checkAndRefreshChannels();
      }
  }

    /**
     * Set the source of the data feed
     * @param source The source of the data feed
     */
  public void setSource(String source) {
      if (!StringUtils.isEmpty(source) && !source.equals(getSource())) {
          this.splunkHecSource = source;
          checkAndRefreshChannels();
      }
  }

    /**
     * Set the source type of events of data feed
     * @param sourcetype The source type of events of data feed
     */
  public void setSourcetype(String sourcetype) {
      if (!StringUtils.isEmpty(sourcetype) && !sourcetype.equals(getSourcetype())) {
          this.splunkHecSourcetype = sourcetype;
          checkAndRefreshChannels(); 
      }
  }
  
    /**
     * Checking if LoadBalancer exists before refreshing channels
     */
    private void checkAndRefreshChannels() {
        if (connection != null && connection.getLoadBalancer() != null) {
            connection.getLoadBalancer().refreshChannels();
        }
    }

    protected Logger getLog() {
      if (this.connection != null) {
          return LOG;
      } else {
          if (GENERIC_LOG == null) {
              return LoggerFactory.getLogger(EventBatchImpl.class.getName());
          }
          return GENERIC_LOG;
      }
    }

    @Deprecated
    /**
     * Should use individual setter methods to update properties instead.
     * @param props
     * @throws UnknownHostException
     */
    public void setProperties(Properties props) throws UnknownHostException {
        for (String key : props.stringPropertyNames()) {
            String val = props.getProperty(key);
            switch (key) {
                case PropertyKeys.ACK_TIMEOUT_MS:
                    setAckTimeoutMS(Long.parseLong(val));
                    break;
                case PropertyKeys.COLLECTOR_URI:
                    setUrls(val);
                    break;
                case PropertyKeys.TOKEN:
                    setToken(val);
                    break;
                case PropertyKeys.HEC_ENDPOINT_TYPE:
                    if (val == "event") {
                        setHecEndpointType(ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
                    } else if (val == "raw") {
                        setHecEndpointType(ConnectionImpl.HecEndpoint.RAW_EVENTS_ENDPOINT);
                    }
                    break;
                case PropertyKeys.HOST:
                    setHost(val);
                    break;
                case PropertyKeys.INDEX:
                    setIndex(val);
                    break;
                case PropertyKeys.SOURCE:
                    setSource(val);
                    break;
                case PropertyKeys.SOURCETYPE:
                    setSourcetype(val);
                    break;
                default:
                    LOG.error("Attempt to change property not supported: " + key);
            }
        }
        
        /* TODO: Investigate whether code below can be made to call each attribute's setter
           instead of just setting the property directly, so that validation and  
           checkAndRefreshChannels have the chance of being called. 
           
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            props.store(bos, null);
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            JavaPropsMapper mapper = new JavaPropsMapper();
            mapper.readerForUpdating(this).readValue(bis);
        } catch (IOException e) {
            throw new RuntimeException("Could update Properties - please check Properties object.", e);
        }
        */
    }
}
