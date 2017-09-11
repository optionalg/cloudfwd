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
package com.splunk.cloudfwd.util;

import com.splunk.cloudfwd.http.Endpoints;
import com.splunk.cloudfwd.http.HttpSender;
import com.splunk.cloudfwd.exceptions.HecMissingPropertiesException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.splunk.cloudfwd.PropertyKeys.*;
/**
 *
 * @author ghendrey
 */
public class PropertiesFileHelper {

  private static final Logger LOG = LoggerFactory.getLogger(PropertiesFileHelper.class.getName());

  private Properties defaultProps = new Properties();
  private Properties overrides;

  public PropertiesFileHelper(Properties overrides) {
    this.overrides = overrides;
    this.parsePropertiesFile();
  }

  /**
   * create SenderFactory with default properties read from lb.properties file
   */
  public PropertiesFileHelper() {
    this.parsePropertiesFile();
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
      for(String key: REQUIRED_KEYS) {
        if (this.defaultProps.getProperty(key) == null) {
          throw new HecMissingPropertiesException("Missing required key: " + key);
        }
      }

      // For any non-required properties, we allow them to remain null if they are not present in overrides
      // or lb.properties, because the property getters below will return the default values.

    } catch (IOException ex) {
      LOG.error("problem loading lb.properties", ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }
  
  public void putProperty(String k, String v){
    this.defaultProps.put(k, v);
  }

  public List<URL> getUrls() {
    List<URL> urls = new ArrayList<>();
    String[] splits = defaultProps.getProperty(COLLECTOR_URI).split(",");
    for (String urlString : splits) {
      try {
        URL url = new URL(urlString.trim());
        urls.add(url);
      } catch (MalformedURLException ex) {
        LOG.error(ex.getMessage(), ex);
        throw new RuntimeException(ex);
      }
    }
    return urls;
  }
  


  // Compares if the first URL matches Cloud>Trail domain (cloud.splunk.com)
  public boolean isCloudInstance() {
    return getUrls().get(0).toString().trim().matches("^.+\\.cloud\\.splunk\\.com.*$");
  }

  public int getChannelsPerDestination() {
    int n = Integer.parseInt(defaultProps.getProperty(CHANNELS_PER_DESTINATION,
            DEFAULT_CHANNELS_PER_DESTINATION).trim());
    if (n < 1) {
      // FIXME: EP: Do we actually want to allow creating 2,147,483,647 channels PER destination ?!
      n = Integer.MAX_VALUE; //effectively no limit by default
    }
    return n;
  }

  public long getUnresponsiveChannelDecomMS() {
    long t =  Long.parseLong(defaultProps.getProperty(
            UNRESPONSIVE_MS, DEFAULT_UNRESPONSIVE_MS).trim());
    if (t < 1) {
      LOG.debug(UNRESPONSIVE_MS +  ": unlimited");
    }
    return t;
  }
  
  public long getAckPollMS() {
    long interval = Long.parseLong(defaultProps.getProperty(ACK_POLL_MS, DEFAULT_ACK_POLL_MS).trim());
    if (interval <= 0) {
      interval = MIN_ACK_POLL_MS;
    }
    return interval;
    
  }  
  
  public long getHealthPollMS() {
        long interval = Long.parseLong(defaultProps.getProperty(HEALTH_POLL_MS, DEFAULT_HEALTH_POLL_MS).trim());
    if (interval <= 0) {
      interval = MIN_HEALTH_POLL_MS;
    }
    return interval;
  }
  

  public int getMaxTotalChannels() {
    int max = Integer.parseInt(defaultProps.getProperty(
            MAX_TOTAL_CHANNELS, DEFAULT_MAX_TOTAL_CHANNELS).trim()); //default no limit
    if (max < 1) {
      max = Integer.MAX_VALUE; //effectively no limit by default
    }
    return max;
  }

  public int getMaxUnackedEventBatchPerChannel() {
    int max = Integer.parseInt(defaultProps.getProperty(
            MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL).trim());
    if (max < MIN_UNACKED_EVENT_BATCHES_PER_CHANNEL) {
      max = 10000;
    }
    return max;
  }
  
  public int getEventBatchSize() {
    int max = Integer.parseInt(defaultProps.getProperty(
            EVENT_BATCH_SIZE, DEFAULT_EVENT_BATCH_SIZE).trim());
    if (max < 1) {
      max = MIN_EVENT_BATCH_SIZE;
    }
    return max;
  }  
  
  public long getChannelDecomMS() {
    long decomMs = Long.parseLong(defaultProps.getProperty(
            CHANNEL_DECOM_MS, DEFAULT_DECOM_MS).trim());
    if(decomMs <= 1){
      return -1;
    }
    if (decomMs < MIN_DECOM_MS) {
      LOG.warn("Ignoring setting for " + CHANNEL_DECOM_MS + " because it is less than minimum acceptable value: " + MIN_DECOM_MS);
      decomMs = MIN_DECOM_MS;
    }
    return decomMs;
  }    
  
  public long getAckTimeoutMS() {
    long timeout = Long.parseLong(defaultProps.getProperty(
            ACK_TIMEOUT_MS, DEFAULT_ACK_TIMEOUT_MS).trim());
    if (timeout < MIN_ACK_TIMEOUT_MS) {
      LOG.warn(ACK_TIMEOUT_MS+ " was set to a potentially too-low value: " + timeout);
    }
    return timeout;
  }    
  
  public long getBlockingTimeoutMS() {
    long timeout = Long.parseLong(defaultProps.getProperty(
            BLOCKING_TIMEOUT_MS, DEFAULT_BLOCKING_TIMEOUT_MS).trim());
    if (timeout < 0) {
      throw new IllegalArgumentException(BLOCKING_TIMEOUT_MS + " must be positive.");
    }
    return timeout;
  }

  public String getToken() {
    return this.defaultProps.getProperty(TOKEN).trim();
  }

  public boolean isMockHttp() {
    return Boolean.parseBoolean(this.defaultProps.getProperty(MOCK_HTTP_KEY,
            "false").trim());
  }

  public boolean isForcedUrlMapToSingleAddr() {
    return Boolean.parseBoolean(this.defaultProps.getProperty(
            MOCK_FORCE_URL_MAP_TO_ONE, "false").trim());
  }

  public Endpoints getSimulatedEndpoints() {
    String classname = this.defaultProps.getProperty(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.sim.SimulatedHECEndpoints");

    try {
      return (Endpoints) Class.forName(classname).newInstance();
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }

  }

  public boolean isCertValidationDisabled() {
    return Boolean.parseBoolean(this.defaultProps.
            getProperty(DISABLE_CERT_VALIDATION, "false").trim());
  }

  public boolean enabledHttpDebug() {
    return Boolean.parseBoolean(this.defaultProps.
            getProperty(
                    ENABLE_HTTP_DEBUG, "false").trim());
  }

  /**
   *
   * @return
   */
  public String getSSLCertContent() {
    if (isCloudInstance()) {
      return defaultProps.getProperty(CLOUD_SSL_CERT_CONTENT).trim();
    }
    return defaultProps.getProperty(SSL_CERT_CONTENT).trim()  ;
  }

  public void enableHttpDebug() {
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
    System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
    System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire.header", "debug");
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "debug");
  }

  public HttpSender createSender(URL url, String host) {
    Properties props = new Properties(defaultProps);
    props.put(COLLECTOR_URI, url.toString());
    props.put(HOST, host.toString());
    return createSender(props);
  }

  private HttpSender createSender(Properties props) {
    try {
      // enable http client debugging
      if (enabledHttpDebug()) enableHttpDebug();
      String url = props.getProperty(COLLECTOR_URI).trim();
      String host = props.getProperty(HOST).trim();
      String token = props.getProperty(TOKEN).trim();
      String cert = getSSLCertContent();
      HttpSender sender = new HttpSender(url, token, isCertValidationDisabled(), cert, host);
      if(isMockHttp()){
        sender.setSimulatedEndpoints(getSimulatedEndpoints());
      }
      return sender;
    } catch (Exception ex) {
      LOG.error("Problem instantiating HTTP sender.", ex);
      throw new RuntimeException(
              "problem parsing lb.properties to create HttpEventCollectorSender",
              ex);
    }
  }

  public HttpSender createSender() {
    return createSender(this.defaultProps);
  }

  int getMaxRetries() {
    int max = Integer.parseInt(defaultProps.getProperty(
            RETRIES, DEFAULT_RETRIES).trim());
    if (max < 1) {
      LOG.debug(RETRIES +  ": unlimited");
      max = Integer.MAX_VALUE;
    }
    return max;
  }


}
