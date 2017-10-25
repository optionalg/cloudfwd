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
import java.util.*;

import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
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

    @JsonProperty("splunk_hec_url")
    private String splunkHecUrl;

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
    private Boolean enableCheckpoints = Boolean.parseBoolean(DEFAULT_ENABLE_CHECKPOINTS);

    @JsonProperty("disable_certificate_validation")
    private Boolean disableCertificateValidation = false;

    @JsonProperty("channels_per_dest")
    private int channelsPerDest = Integer.parseInt(DEFAULT_CHANNELS_PER_DESTINATION);

    @JsonProperty("mock_http")
    private Boolean mockHttp = false;

    @JsonProperty("enabled")
    private Boolean testPropertiesEnabled;

    @JsonProperty("unresponsive_channel_decom_ms")
    private long unresponsiveChannelDecomMS = Long.parseLong(DEFAULT_UNRESPONSIVE_MS);

    @JsonProperty("max_total_channels")
    private int maxTotalChannels = Integer.parseInt(DEFAULT_MAX_TOTAL_CHANNELS);

    @JsonProperty("max_unacked_per_channel")
    private int maxUnackedPerChannel = Integer.parseInt(DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL);

    @JsonProperty("event_batch_size")
    private int eventBatchSize = Integer.parseInt(DEFAULT_EVENT_BATCH_SIZE);

    @JsonProperty("ack_poll_ms")
    private long ackPollMS = Long.parseLong(DEFAULT_ACK_POLL_MS);

    @JsonProperty("health_poll_ms")
    private long healthPollMS = Long.parseLong(DEFAULT_HEALTH_POLL_MS);

    @JsonProperty("channel_decom_ms")
    private long channelDecomMS = Long.parseLong(DEFAULT_DECOM_MS);

    @JsonProperty("ack_timeout_ms")
    private long ackTimeoutMS = Long.parseLong(DEFAULT_ACK_TIMEOUT_MS);

    @JsonProperty("blocking_timeout_ms")
    private long blockingTimeoutMS = Long.parseLong(DEFAULT_BLOCKING_TIMEOUT_MS);

    @JsonProperty("mock_http_classname")
    private String mockHttpClassname;

    @JsonProperty("ssl_cert_content")
    private String sslCertContent;

    @JsonProperty("cloud_ssl_cert_content")
    private String cloudSslCertContent;

    @JsonProperty("enable_http_debug")
    private Boolean enableHttpDebug = false;

    @JsonProperty("max_retries")
    private int maxRetries = Integer.parseInt(DEFAULT_RETRIES);

    @JsonProperty("max_preflight_retries")
    private int maxPreflightTries = Integer.parseInt(DEFAULT_PREFLIGHT_RETRIES);

    public static PropertiesFileHelper fromPropsFile(String pathToFile) {
        // use Jackson to populate this ConnectionSettings instance from file
        JavaPropsMapper mapper = new JavaPropsMapper();
        try {
            InputStream inputStream = ConnectionSettings.class.getResourceAsStream(pathToFile);
            if (inputStream != null) {
                PropertiesFileHelper propertiesFileHelper = mapper.readValue(inputStream, PropertiesFileHelper.class);
                return propertiesFileHelper;
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not map Properties file to Java object - please check file path.", e);
        }

        //TODO!!
        // If required properties are missing from cloudfwd.properties, overrides, and defaults, then throw exception.
//        for (String key : REQUIRED_KEYS) {
//            if (keyGetter(key) == null) {
//                throw new HecMissingPropertiesException("Missing required key: " + key);
//            }
//        }
        return null;
    }

    public void setConnection(Connection c) {
        this.connection = (ConnectionImpl)c;
        this.LOG = this.connection.getLogger(ConnectionSettings.class.getName());

        // Perform any actions that throw any config exceptions here (aka. on Connections.create())
        //urlsStringToList(getUrlString()) --> validates urls and throws exception if invalid
        
        // Perform any actions that had required a Connection object in the property setter, 
        // where one may not have been available
        if (Long.valueOf(this.getAckTimeoutMS()) != null) {
            connection.getTimeoutChecker().setTimeout(this.getAckTimeoutMS());
        }
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
        return urlsStringToList(this.splunkHecUrl);
    }

    public String getUrlString() {
        return this.splunkHecUrl;
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
        return applyDefaultIfNull(this.channelsPerDest, Integer.parseInt(DEFAULT_CHANNELS_PER_DESTINATION));
    }

    public long getUnresponsiveMS() {
        return applyDefaultIfNull(this.unresponsiveChannelDecomMS, Long.parseLong(DEFAULT_UNRESPONSIVE_MS)); //TODO: can defaults be non-strings in properties file?
    }

    public long getAckPollMS() {
        return applyDefaultIfNull(this.ackPollMS, Long.parseLong(DEFAULT_ACK_POLL_MS));
    }

    public long getHealthPollMS() {
        return applyDefaultIfNull(this.healthPollMS, Long.parseLong(DEFAULT_HEALTH_POLL_MS));
    }

    public int getMaxTotalChannels() {
        return applyDefaultIfNull(this.maxTotalChannels, Integer.parseInt(DEFAULT_MAX_TOTAL_CHANNELS));
    }

    public int getMaxUnackedEventBatchPerChannel() {
        return applyDefaultIfNull(this.maxUnackedPerChannel, Integer.parseInt(DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL));
    }

    public int getEventBatchSize() {
        return applyDefaultIfNull(this.eventBatchSize, Integer.parseInt(DEFAULT_EVENT_BATCH_SIZE));
    }

    public long getChannelDecomMS() {
        return applyDefaultIfNull(this.channelDecomMS, Long.parseLong(DEFAULT_DECOM_MS));
    }

    public long getAckTimeoutMS() {
        return applyDefaultIfNull(this.ackTimeoutMS, Long.parseLong(DEFAULT_ACK_TIMEOUT_MS));
    }

    public long getBlockingTimeoutMS() {
        return applyDefaultIfNull(this.blockingTimeoutMS, Long.parseLong(DEFAULT_BLOCKING_TIMEOUT_MS));
    }
    
    public String getMockHttpClassname() {
        return applyDefaultIfNull(this.mockHttpClassname, "com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints");
    }

    public Endpoints getSimulatedEndpoints() {
        try {
            return (Endpoints) Class.forName(this.getMockHttpClassname()).newInstance();
        } catch (Exception ex) {
            getLog().error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public boolean isCertValidationDisabled() {
        return applyDefaultIfNull(this.disableCertificateValidation, false);
    }

    public boolean enabledHttpDebug() {
        return applyDefaultIfNull(this.enableHttpDebug, false);
    }

    public boolean isCheckpointEnabled() {
        return applyDefaultIfNull(this.enableCheckpoints, Boolean.parseBoolean(DEFAULT_ENABLE_CHECKPOINTS));
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
        return applyDefaultIfNull(this.maxRetries, Integer.parseInt(DEFAULT_RETRIES));
    }

    public int getMaxPreflightRetries() {
        return applyDefaultIfNull(this.maxPreflightTries, Integer.parseInt(DEFAULT_PREFLIGHT_RETRIES));
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

    public String getHecEndpointTypeString() {
        return applyDefaultIfNull(this.hecEndpointType, DEFAULT_HEC_ENDPOINT_TYPE);
    }

    public String getToken() {
        if (this.splunkHecToken == null) {
            throw new HecConnectionStateException(
                    "HEC token missing from Connection configuration. " + "See PropertyKeys.TOKEN",
                    HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
        return this.splunkHecToken;
    }

    protected boolean isMockHttp() {
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
    
    /**
     * Use this method to change multiple settings on the connection. See
     * PropertyKeys class for more information.
     *
     * @param props
     */
    // TODO: Requires connection instance
    public void setProperties(Properties props) throws UnknownHostException {
        boolean refreshChannels = false;
        boolean dnsLookup = false;
        Long newAckTimeout = Long.parseLong(props.getProperty(ACK_TIMEOUT_MS));
        String newUrls = props.getProperty(COLLECTOR_URI);
        String newToken = props.getProperty(TOKEN);
        String newHecEndpointType = props.getProperty(HEC_ENDPOINT_TYPE);
        String host = props.getProperty(HOST);
        String index = props.getProperty(INDEX);
        String source = props.getProperty(SOURCE);
        String sourcetype = props.getProperty(SOURCETYPE);
        Set keySet = props.keySet();

        // Ack Timeout
        if (this.getAckTimeoutMS() != newAckTimeout) {
            this.setAckTimeoutMS(newAckTimeout);
            keySet.remove(ACK_TIMEOUT_MS);
        }

        // Collector URI
        if (this.getUrlString() != newUrls) {
            this.setUrl(newUrls);
            dnsLookup = true;
            refreshChannels = true;
            keySet.remove(COLLECTOR_URI);
        }

        // Token
        if (this.getToken() != newToken) {
            this.setToken(newToken);
            refreshChannels = true;
            keySet.remove(TOKEN);
        }

        // HEC endpoint
        if (this.getHecEndpointTypeString() != newHecEndpointType) {
            this.setHecEndpointType(newHecEndpointType);
            keySet.remove(HEC_ENDPOINT_TYPE);
        }
        
        // Host
        if (this.getHost() != host) {
            this.setHost(host);
            refreshChannels = true;
        }
        
        // Index
        if (this.getIndex() != index) {
            this.setIndex(index);
            refreshChannels = true;
        }
        
        // Source
        if (this.getSource() != source) {
            this.setSource(source);
            refreshChannels = true;
        }
        
        // Sourcetype
        if (this.getSourcetype() != sourcetype) {
            this.setSourcetype(sourcetype);
            refreshChannels = true;
        }

        // Unsupported property change attempt
        for (Object key : keySet)
        {
            getLog().warn("Attempt to change property not supported: " + key);
        }

        if (refreshChannels) {
            checkAndRefreshChannels();
        }
    }

    //TODO: write unit tests for each setter

    public void setChannelsPerDestination(int numChannels) {
        if (numChannels < 1) {
            getLog().debug("{}, defaulting {} to {}", numChannels, CHANNELS_PER_DESTINATION, Integer.parseInt(DEFAULT_CHANNELS_PER_DESTINATION));
        }
        this.channelsPerDest = numChannels;
    }

    public void setUnresponsiveMS(long decomMS) {
        if (decomMS < 1) {
            getLog().debug("{}, defaulting {} to {}", decomMS, UNRESPONSIVE_MS, Long.parseLong(DEFAULT_UNRESPONSIVE_MS));
        }
        this.unresponsiveChannelDecomMS = decomMS;
    }

    public void setAckPollMS(long pollMS) {
        if (pollMS <= 0) {
            getLog().debug("{}, defaulting {} to smallest allowed value of {}", pollMS, ACK_POLL_MS, MIN_ACK_POLL_MS);
        }
        this.ackPollMS = pollMS;
    }

    public void setHealthPollMS(long pollMS) {
        if (pollMS <= 0) {
            getLog().debug("{}, defaulting {} to smallest allowed value {}", pollMS, HEALTH_POLL_MS, MIN_HEALTH_POLL_MS);
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
        int minimum = 10000;
        if (batchesPerChannel < MIN_UNACKED_EVENT_BATCHES_PER_CHANNEL) {
            int was = batchesPerChannel;
            batchesPerChannel = minimum;
            getLog().debug("{}, defaulting {} to smallest allowed value {}", was, MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, batchesPerChannel);
        }
        this.maxUnackedPerChannel = batchesPerChannel;
    }

    /**
     * @param numChars the size of the EventBatchImpl in characters (not bytes)
     */
    public void setEventBatchSize(int numChars) {
        if (numChars < 1) {
            numChars = MIN_EVENT_BATCH_SIZE;
            getLog().debug("{}, defaulting {} to smallest allowed value {}", numChars, EVENT_BATCH_SIZE, MIN_EVENT_BATCH_SIZE);
        }
        this.eventBatchSize = numChars;
    }

    public void setChannelDecomMS(long decomMS) {
        if (decomMS <= 1) {
            long was = decomMS;
            decomMS = -1;
            getLog().debug("{}, defaulting {} to no-limit {}", was, CHANNEL_DECOM_MS, decomMS);
        }
        if (decomMS < MIN_DECOM_MS && !isMockHttp()) {
            getLog().warn(
                    "Ignoring setting for " + CHANNEL_DECOM_MS + " because it is less than minimum acceptable value: " + MIN_DECOM_MS);
            decomMS = MIN_DECOM_MS;
        }
        this.channelDecomMS = decomMS;
    }

    public void setAckTimeoutMS(long timeoutMS) {
        if (timeoutMS != getAckTimeoutMS()) {
            if (timeoutMS <= 0) {
                long was = timeoutMS;
                timeoutMS = Long.MAX_VALUE;
                getLog().debug("{}, defaulting {} to maximum allowed value {}", was, ACK_TIMEOUT_MS, timeoutMS);
            } else if (timeoutMS < MIN_ACK_TIMEOUT_MS) {
                getLog().warn(ACK_TIMEOUT_MS + " was set to a potentially too-low value: " + timeoutMS);
            }
            this.ackTimeoutMS = timeoutMS;
            if (connection != null) {
                connection.getTimeoutChecker().setTimeout(timeoutMS);
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

    public void setCertValidationEnabled(Boolean mode) {
        this.disableCertificateValidation = mode;
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
            getLog().debug("{}, defaulting {} to maximum allowed value {}", was, PREFLIGHT_RETRIES, retries);
        }
        this.maxPreflightTries = retries;
    }

    public void setHecEndpointType(ConnectionImpl.HecEndpoint type) {
        String endpoint;
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

    public void setHecEndpointType(String type) {
        String endpoint;
        if (type == "event" || type == "raw") {
            endpoint = type;
        } else {
            getLog().warn(
                    "Unrecognized HEC Endpoint type. Defaulting to " + DEFAULT_HEC_ENDPOINT_TYPE + ". See PropertyKeys.HEC_ENDPOINT_TYPE.");
            endpoint = DEFAULT_HEC_ENDPOINT_TYPE;
        }
        this.hecEndpointType = endpoint;
    }

    /**
     * Set Http Event Collector token to use.
     * May take up to PropertyKeys.CHANNEL_DECOM_MS milliseconds
     * to go into effect.
     * @param token
     */
    public void setToken(String token) {
        this.splunkHecToken = token;
        
        if (!this.splunkHecToken.equals(token)) {
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
  public void setUrl(String urls) {
      this.splunkHecUrl = urls;

      if (connection != null && !urlsStringToList(urls).equals(getUrls())) {
          checkAndRefreshChannels();
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
      this.splunkHecHost = host;

      if (!getHost().equals(host)) {
          checkAndRefreshChannels();
      }
  }

    /**
     * Set Splunk index in which the data feed is stored
     * @param index The Splunk index in which the data feed is stored
     */
  public void setIndex(String index) {
      this.splunkHecIndex = index;
      
      if (!getIndex().equals(index)) {
          checkAndRefreshChannels();
      }
  }

    /**
     * Set the source of the data feed
     * @param source The source of the data feed
     */
  public void setSource(String source) {
      this.splunkHecSource = source;
      
      if (!getSource().equals(source)) {
          checkAndRefreshChannels();
      }
  }

    /**
     * Set the source type of events of data feed
     * @param sourcetype The source type of events of data feed
     */
  public void setSourcetype(String sourcetype) {
      this.splunkHecSourcetype = sourcetype;
      
      if (!getSourcetype().equals(sourcetype)) {
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
/*
User should be able to init Connection using ConnectionSettings
 */

}
