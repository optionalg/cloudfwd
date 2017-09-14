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

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.HecConnectionStateException;
import com.splunk.cloudfwd.HecIllegalStateException;
import com.splunk.cloudfwd.http.Endpoints;
import com.splunk.cloudfwd.http.HttpSender;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Comparator;
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
        throw new RuntimeException("can't find /lb.properties"); // TODO: This will be removed
      }
      defaultProps.load(is);
    } catch (IOException ex) {
      throw new HecIllegalStateException("Problem loading lb.properties", HecIllegalStateException.Type.CANNOT_LOAD_PROPERTIES);
    }
  }
  
  public void putProperty(String k, String v){
    this.defaultProps.put(k, v);
  }

  public List<URL> getUrls() {
    return urlsStringToList(defaultProps.getProperty(COLLECTOR_URI));
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
    if (decomMs < MIN_DECOM_MS && !isMockHttp()) {
      LOG.warn("Ignoring setting for " + CHANNEL_DECOM_MS + " because it is less than minimum acceptable value: " + MIN_DECOM_MS);
      decomMs = MIN_DECOM_MS;
    }
    return decomMs;
  }    
  
  public long getAckTimeoutMS() {
    long timeout = Long.parseLong(defaultProps.getProperty(
            ACK_TIMEOUT_MS, DEFAULT_ACK_TIMEOUT_MS).trim());
    if(timeout <=0 ){
      timeout = Long.MAX_VALUE;
    }else if (timeout < MIN_ACK_TIMEOUT_MS) {
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

    //FIXME TODO. THis needs to get OUT of the public API
  public HttpSender createSender(URL url, String host) {
    Properties props = new Properties(defaultProps);
    props.put(COLLECTOR_URI, url.toString());
    props.put(HOST, host.toString());
    return createSender(props);
  }

  private HttpSender createSender(Properties props) {
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
  }

  //FIXME TODO. THis needs to get OUT of the public API
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
  
  public boolean isCheckpointEnabled(){
     return Boolean.parseBoolean(this.defaultProps.getProperty(ENABLE_CHECKPOINTS,
            DEFAULT_ENABLE_CHECKPOINTS).trim());
  }

  public Connection.HecEndpoint getHecEndpointType() {
    Connection.HecEndpoint endpoint;
    String type = defaultProps.getProperty(HEC_ENDPOINT_TYPE, DEFAULT_HEC_ENDPOINT_TYPE).trim();
    if (type.equals("raw")) {
      endpoint = Connection.HecEndpoint.RAW_EVENTS_ENDPOINT;
    } else if (type.equals("event")) {
      endpoint = Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT;
    } else {
      LOG.warn("Unrecognized HEC Endpoint type. Defaulting to " + DEFAULT_HEC_ENDPOINT_TYPE
        + ". See PropertyKeys.HEC_ENDPOINT_TYPE.");
      endpoint = Connection.HecEndpoint.RAW_EVENTS_ENDPOINT;
    }
    return endpoint;
  }

  public void setHecEndpointType(Connection.HecEndpoint type) {
    if (type == Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT) {
      defaultProps.put(PropertyKeys.HEC_ENDPOINT_TYPE, "event");
    } else {
      defaultProps.put(PropertyKeys.HEC_ENDPOINT_TYPE, "raw");
    }
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

  public Properties getDiff(Properties props) {
    Properties diff = new Properties();
    diff.putAll(defaultProps);
    diff.putAll(props);
    diff.entrySet().removeAll(defaultProps.entrySet());
    return diff;
  }

  public String getToken() {
    if (defaultProps.getProperty(TOKEN) == null) {
      throw new HecConnectionStateException("HEC token missing from Connection configuration. " +
              "See PropertyKeys.TOKEN", HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
    }
    return defaultProps.getProperty(TOKEN);
  }


}
