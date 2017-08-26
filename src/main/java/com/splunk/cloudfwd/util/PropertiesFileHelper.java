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

/**
 *
 * @author ghendrey
 */
public class PropertiesFileHelper {

  private static final Logger LOG = Logger.getLogger(PropertiesFileHelper.class.
          getName());

  public static final String TOKEN_KEY = "token";
  public static final String COLLECTOR_URI = "url";
  public static final String DISABLE_CERT_VALIDATION_KEY = "disableCertificateValidation";
  public static final String CHANNELS_PER_DESTINATION_KEY = "channels_per_dest";
  public static final String MOCK_HTTP_KEY = "mock_http";
  public static final String MOCK_HTTP_CLASSNAME_KEY = "mock_http_classname";
  public static final String MOCK_FORCE_URL_MAP_TO_ONE = "mock_force_url_map_to_one";
  public static final String UNRESPONSIVE_MS = "unresponsive_channel_decom_ms";
  public static final String MAX_TOTAL_CHANNELS = "max_total_channels";
  public static final String MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL = "max_unacked_per_channel";
  public static final String ACK_POLL_MS = "ack_poll_ms";
  public static final long MIN_ACK_POLL_MS = 250;
  public static final String DEFAULT_ACK_POLL_MS = "1000";
  public static final String HEALTH_POLL_MS = "health_poll_ms";
  public static final long MIN_HEALTH_POLL_MS = 1000;
  public static final String DEFAULT_HEALTH_POLL_MS = "1000";  

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
        Logger.getLogger(IndexDiscoverer.class.getName()).
                log(Level.SEVERE, "Malformed URL: '" + urlString + "'");
        Logger.getLogger(PropertiesFileHelper.class.getName()).log(
                Level.SEVERE, null,
                ex);
      }
    }
    return urls;
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

  public HttpSender createSender(URL url) {
    Properties props = new Properties(defaultProps);
    props.put("url", url.toString());
    return createSender(props);
  }

  private HttpSender createSender(Properties props) {
    try {
      String url = props.getProperty(COLLECTOR_URI).trim();
      String token = props.getProperty(TOKEN_KEY).trim();
      HttpSender sender = new HttpSender(url, token);
      if (isCertValidationDisabled()) {
        sender.disableCertificateValidation();
      }
      if (isMockHttp()) {
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
