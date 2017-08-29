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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.datatype.DatatypeConstants;

/**
 *
 * @author ghendrey
 */
public class PropertiesFileHelper {

  private static final Logger LOG = Logger.getLogger(PropertiesFileHelper.class.
          getName());

  public static final String TOKEN_KEY = "token";
  public static final String COLLECTOR_URI = "url";
  public static final String HOST = "host";
  public static final String DISABLE_CERT_VALIDATION_KEY = "disableCertificateValidation";
  public static final String CHANNELS_PER_DESTINATION_KEY = "channels_per_dest";
  public static final String MOCK_HTTP_KEY = "mock_http";
  public static final String MOCK_FORCE_URL_MAP_TO_ONE = "mock_force_url_map_to_one";
  public static final String UNRESPONSIVE_MS = "unresponsive_channel_decom_ms";
  public static final String MAX_TOTAL_CHANNELS = "max_total_channels";
  public static final String MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL = "max_unacked_per_channel";
  public static final String EVENT_BATCH_SIZE = "event_batch_size";
  public static final int MIN_EVENT_BATCH_SIZE = 0;
  public static final String DEFAULT_EVENT_BATCH_SIZE = "32768";
  public static final String ACK_POLL_MS = "ack_poll_ms";
  public static final long MIN_ACK_POLL_MS = 250;
  public static final String DEFAULT_ACK_POLL_MS = "1000";
  public static final String HEALTH_POLL_MS = "health_poll_ms";
  public static final long MIN_HEALTH_POLL_MS = 1000;
  public static final String DEFAULT_HEALTH_POLL_MS = "1000";  
  public static final String CHANNEL_DECOM_MS = "channel_decom_ms";
  public static final long MIN_DECOM_MS = 60000;
  public static final String DEFAULT_DECOM_MS = "600000";   //10 min
  public static final String ACK_TIMEOUT_MS = "ack_timeout_ms";
  public static final String DEFAULT_ACK_TIMEOUT_MS = "300000"; //5 min
  public static final long MIN_ACK_TIMEOUT_MS = 60000;  //60 sec
  public static final String MOCK_HTTP_CLASSNAME_KEY = "mock_http_classname";
  public static final String SSL_CERT_CONTENT_KEY = "ssl_cert_content";
  public static final String CLOUD_SSL_CERT_CONTENT_KEY = "cloud_ssl_cert_content";
  public static final String ENABLE_HTTP_DEBUG = "enable_http_debug";


  private Properties defaultProps = new Properties();

  public PropertiesFileHelper(Properties overrides) {
    this(); //setup all defaults by calling SenderFactory() empty constr
    this.defaultProps.putAll(overrides);
  }

  /**
   * create SenderFactory with default properties read from lb.properties file
   */
  public PropertiesFileHelper() {
    try {
      InputStream is = getClass().getResourceAsStream("/lb.properties");
      if (null == is) {
        throw new RuntimeException("can't find /lb.properties");
      }
      defaultProps.load(is);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "problem loading lb.properties", ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  public List<URL> getUrls() {
    List<URL> urls = new ArrayList<>();
    String[] splits = defaultProps.getProperty(COLLECTOR_URI).split(",");
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
  


  // Compares if the first URL matches Cloud>Trail domain (cloud.splunk.com)
  public boolean isCloudInstance() {
    return getUrls().get(0).toString().trim().matches("^.+\\.cloud\\.splunk\\.com.*$");
  }

  public int getChannelsPerDestination() {
    int n = Integer.parseInt(defaultProps.getProperty(
            CHANNELS_PER_DESTINATION_KEY, "8").trim());
    if (n < 1) {
      n = Integer.MAX_VALUE; //effectively no limit by default
    }
    return n;
  }

  public long getUnresponsiveChannelDecomMS() {
    return Long.parseLong(defaultProps.getProperty(
            UNRESPONSIVE_MS, "-1").trim());
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
            MAX_TOTAL_CHANNELS, "-1").trim()); //default no limit
    if (max < 1) {
      max = Integer.MAX_VALUE; //effectively no limit by default
    }
    return max;
  }

  public int getMaxUnackedEventBatchPerChannel() {
    int max = Integer.parseInt(defaultProps.getProperty(
            MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, "10000").trim());
    if (max < 1) {
      max = 10000;
    }
    return max;
  }
  
  public int getMinEventBatchSize() {
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
      LOG.warning("Ignoring setting for " + CHANNEL_DECOM_MS + " because it is less than minimum acceptable value: " + MIN_DECOM_MS);
      decomMs = MIN_DECOM_MS;
    }
    return decomMs;
  }    
  
  public long getAckTimeoutMS() {
    long timeout = Long.parseLong(defaultProps.getProperty(
            ACK_TIMEOUT_MS, DEFAULT_ACK_TIMEOUT_MS).trim());
    if (timeout < MIN_ACK_TIMEOUT_MS) {
      LOG.warning("Ignoring setting for " + MIN_ACK_TIMEOUT_MS + " because it is less than minimum acceptable value: " + MIN_ACK_TIMEOUT_MS);
    }
    return timeout;
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
    String classname = this.defaultProps.getProperty(MOCK_HTTP_CLASSNAME_KEY,
            "com.splunk.cloudfwd.sim.SimulatedHECEndpoints");

    try {
      return (Endpoints) Class.forName(classname).newInstance();
    } catch (Exception ex) {
      Logger.getLogger(PropertiesFileHelper.class.getName()).
              log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }

  }

  public boolean isCertValidationDisabled() {
    return Boolean.parseBoolean(this.defaultProps.
            getProperty(
                    DISABLE_CERT_VALIDATION_KEY, "false").trim());
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
      return defaultProps.getProperty(CLOUD_SSL_CERT_CONTENT_KEY).trim();
    }
    return defaultProps.getProperty(SSL_CERT_CONTENT_KEY).trim()  ;
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
      String token = props.getProperty(TOKEN_KEY).trim();
      String cert = getSSLCertContent();
      HttpSender sender = new HttpSender(url, token, isCertValidationDisabled(), cert, host);
      if(isMockHttp()){
        sender.setSimulatedEndpoints(getSimulatedEndpoints());
      }
      return sender;
    } catch (Exception ex) {
      LOG.log(Level.SEVERE, "Problem instantiating HTTP sender.", ex);
      throw new RuntimeException(
              "problem parsing lb.properties to create HttpEventCollectorSender",
              ex);
    }
  }

  public HttpSender createSender() {
    return createSender(this.defaultProps);
  }


}
