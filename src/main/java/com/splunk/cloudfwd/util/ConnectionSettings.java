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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import static com.splunk.cloudfwd.PropertyKeys.*;

/**
 *
 * @author ghendrey
 */
public class ConnectionSettings {

  private static final Logger LOG = Logger.getLogger(ConnectionSettings.class.
          getName());
  private Properties props = new Properties();

  /**
   * Create a new ConnectionSettings object that overrides
   * values from lb.properties.
   * @param overrides overrides settings in lb.properties
   */
  public ConnectionSettings(Properties overrides) {
    this(); //setup all defaults by calling SenderFactory() empty constr
    this.props.putAll(overrides);
  }

  /**
   * Create a new ConnectionSettings object that
   * reads values from lb.properties.
   */
  public ConnectionSettings() {
    try {
      InputStream is = getClass().getResourceAsStream("/lb.properties");
      if (null == is) {
        throw new RuntimeException("can't find /lb.properties");
      }
      props.load(is);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "problem loading lb.properties", ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  /**
   * Set a property in the Properties object that is
   * interpreted at runtime to configure the
   * Connection.
   * @param k key from PropertyKeys.java
   * @param v value of the property
   */
  public void putProperty(String k, String v){
    this.props.put(k, v);
  }

  /**
   * Overrides the settings in the Properties object
   * that is interpreted at runtime to configure the
   * Connection.
   * @param overrides the Properties object with keys from PropertyKeys.java
   */
  public void putAll(Properties overrides) {
    this.props.putAll(overrides);
  }

  /**
   * The Properties object that the software reads and interprets
   * to configure the Connection. Invalid values in this object
   * may be overridden by default values at runtime.
   * @return the Properties object
   */
  public Properties getPropertiesObject() {
    return props;
  }

  public List<URL> getUrls() {
    List<URL> urls = new ArrayList<>();
    String[] splits = props.getProperty(COLLECTOR_URI).split(",");
    for (String urlString : splits) {
      try {
        URL url = new URL(urlString.trim());
        urls.add(url);
      } catch (MalformedURLException ex) {
        LOG.severe(ex.getMessage());
        throw new RuntimeException(ex);
      }
    }
    return urls;
  }

  public String getToken() {
    if (props.getProperty(TOKEN) == null) {
      throw new RuntimeException("Connection settings must contain a token.");
    }
    return props.getProperty(TOKEN);
  }

  public boolean isCloudInstance() {
    // Compares if the first URL matches Cloud>Trail domain (cloud.splunk.com)
    return getUrls().get(0).toString().trim().matches("^.+\\.cloud\\.splunk\\.com.*$");
  }

  public int getChannelsPerDestination() {
    int n = Integer.parseInt(props.getProperty(CHANNELS_PER_DESTINATION, "8").trim());
    if (n < 1) {
      n = Integer.MAX_VALUE; //effectively no limit by default
    }
    return n;
  }

  public long getUnresponsiveChannelDecomMS() {
    long t =  Long.parseLong(props.getProperty(
            UNRESPONSIVE_MS, DEFAULT_UNRESPONSIVE_MS).trim());
    if (t < 1) {
      LOG.info(UNRESPONSIVE_MS +  ": unlimited");
      t = Integer.MAX_VALUE;
    }
    return t;
  }
  
  public long getAckPollMS() {
    long interval = Long.parseLong(props.getProperty(ACK_POLL_MS, DEFAULT_ACK_POLL_MS).trim());
    if (interval <= 0) {
      interval = MIN_ACK_POLL_MS;
    }
    return interval;
    
  }  
  
  public long getHealthPollMS() {
        long interval = Long.parseLong(props.getProperty(HEALTH_POLL_MS, DEFAULT_HEALTH_POLL_MS).trim());
    if (interval <= 0) {
      interval = MIN_HEALTH_POLL_MS;
    }
    return interval;
  }
  

  public int getMaxTotalChannels() {
    int max = Integer.parseInt(props.getProperty(
            MAX_TOTAL_CHANNELS, "-1").trim()); //default no limit
    if (max < 1) {
      max = Integer.MAX_VALUE; //effectively no limit by default
    }
    return max;
  }

  public int getMaxUnackedEventBatchPerChannel() {
    int max = Integer.parseInt(props.getProperty(
            MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL).trim());
    if (max < MIN_UNACKED_EVENT_BATCHES_PER_CHANNEL) {
      max = 10000;
    }
    return max;
  }
  
  public int getEventBatchSize() {
    int max = Integer.parseInt(props.getProperty(
            EVENT_BATCH_SIZE, DEFAULT_EVENT_BATCH_SIZE).trim());
    if (max < 1) {
      max = MIN_EVENT_BATCH_SIZE;
    }
    return max;
  }  
  
  public long getChannelDecomMS() {
    long decomMs = Long.parseLong(props.getProperty(
            CHANNEL_DECOM_MS, DEFAULT_DECOM_MS).trim());
    if(decomMs <= 1){
      return -1;
    }
    if (decomMs < MIN_DECOM_MS) {
      LOG.warning("Ignoring setting for " + CHANNEL_DECOM_MS
              + " because it is less than minimum acceptable value: " + MIN_DECOM_MS);
      decomMs = MIN_DECOM_MS;
    }
    return decomMs;
  }    
  
  public long getAckTimeoutMS() {
    long timeout = Long.parseLong(props.getProperty(
            ACK_TIMEOUT_MS, DEFAULT_ACK_TIMEOUT_MS).trim());
    if (timeout < MIN_ACK_TIMEOUT_MS) {
      LOG.warning(ACK_TIMEOUT_MS+ " was set to a potentially too-low value: " + timeout);
    }
    return timeout;
  }    
  
  public long getBlockingTimeoutMS() {
    long timeout = Long.parseLong(props.getProperty(
            BLOCKING_TIMEOUT_MS, DEFAULT_BLOCKING_TIMEOUT_MS).trim());
    if (timeout < 0) {
      throw new IllegalArgumentException(BLOCKING_TIMEOUT_MS + " must be positive.");
    }
    return timeout;
  }     

  public boolean isMockHttp() {
    return Boolean.parseBoolean(this.props.getProperty(MOCK_HTTP_KEY,
            "false").trim());
  }

  public boolean isForcedUrlMapToSingleAddr() {
    return Boolean.parseBoolean(this.props.getProperty(
            MOCK_FORCE_URL_MAP_TO_ONE, "false").trim());
  }

  public Endpoints getSimulatedEndpoints() {
    String classname = this.props.getProperty(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.sim.SimulatedHECEndpoints");

    try {
      return (Endpoints) Class.forName(classname).newInstance();
    } catch (Exception ex) {
      Logger.getLogger(ConnectionSettings.class.getName()).
              log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }

  }

  public boolean isCertValidationDisabled() {
    return Boolean.parseBoolean(this.props.
            getProperty(DISABLE_CERT_VALIDATION, "false").trim());
  }

  public boolean httpDebugEnabled() {
    return Boolean.parseBoolean(this.props.
            getProperty(
                    ENABLE_HTTP_DEBUG, "false").trim());
  }

  public String getSSLCertContent() {
    if (isCloudInstance()) {
      return props.getProperty(CLOUD_SSL_CERT_CONTENT).trim();
    }
    return props.getProperty(SSL_CERT_CONTENT).trim()  ;
  }

  public void enableHttpDebug() {
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
    System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
    System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire.header", "debug");
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "debug");
  }

  int getMaxRetries() {
    int max = Integer.parseInt(props.getProperty(
            RETRIES, DEFAULT_RETRIES).trim());
    if (max < 1) {
      LOG.info(RETRIES +  ": unlimited");
      max = Integer.MAX_VALUE;
    }
    return max;
  }
}
