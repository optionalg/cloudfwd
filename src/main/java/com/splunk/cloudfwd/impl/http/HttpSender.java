package com.splunk.cloudfwd.impl.http;

/**
 * @copyright
 *
 * Copyright 2013-2015 Splunk, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"): you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecIllegalStateException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.impl.util.HecChannel;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class performs the actually HTTP send to HEC
 * event collector.
 */
public final class HttpSender implements Endpoints {

  // Default to SLF4J Logger, and set custom LoggerFactory when channel (and therefore Connection instance) is available
  private Logger LOG = LoggerFactory.getLogger(HttpSender.class.getName());

  private static final String AuthorizationHeaderTag = "Authorization";
  private static final String AuthorizationHeaderScheme = "Splunk %s";
  private static final String HttpContentType = "application/json; profile=urn:splunk:event:1.0; charset=utf-8"; //FIX ME application/json not all the time
  private static final String ChannelHeader = "X-Splunk-Request-Channel";
  private static final String Host = "Host";

  private final String eventUrl;
  private final String rawUrl;
  private final String token;
  private final String cert;
  private final String host;
  private CloseableHttpAsyncClient httpClient;
  private boolean disableCertificateValidation = false;
  private HecChannel channel = null;
  private final String ackUrl;
  private final String healthUrl;
  private Endpoints simulatedEndpoints;
  private final HecIOManager hecIOManager;
  private final String baseUrl; 


  /**
   * Initialize HttpEventCollectorSender
   *
   * @param url http event collector input server.
   * @param token application token
   * @param disableCertificateValidation disables Certificate Validation
   * @param cert SSL Certificate Authority Public key to verify TLS with
   * Self-Signed SSL Certificate chain
   * @param host Hostname to use in HTTP requests. It is needed when we use IP
   * addresses in url by RFC
   */
  public HttpSender(final String url, final String token,
          final boolean disableCertificateValidation,
          final String cert, final String host) {
    this.baseUrl = url;
    this.host = host;
    this.eventUrl = url.trim() + "/services/collector/event";
    this.rawUrl = url.trim() + "/services/collector/raw";
    this.ackUrl = url.trim() + "/services/collector/ack";
    this.healthUrl = url.trim() + "/services/collector/health";
    this.token = token;
    this.cert = cert;
    this.disableCertificateValidation = disableCertificateValidation;
    this.hecIOManager = new HecIOManager(this);
  }
  
    
  public void setChannel(HecChannel c) {
    this.channel = c;
    this.LOG = c.getConnection().getLogger(HttpSender.class.getName());
    // Channel is now available so Connection instance loggerFactory can be set
    this.hecIOManager.setLogger(c.getConnection());
  }
  
  public ConnectionImpl getConnection() {
     return this.channel.getConnection();
  }
  
  public HecChannel getChannel() {
    if (null == channel) {
      throw new HecIllegalStateException(
              "Channel is null",
              HecIllegalStateException.Type.NULL_CHANNEL);
    }
    return channel;
  } 
  
  public AcknowledgementTracker getAcknowledgementTracker() {
    return hecIOManager.getAcknowledgementTracker();
  }
  
  public HecIOManager getHecIOManager() {
    return this.hecIOManager;
  }

  /**
   * Immediately send the EventBatch
   *
   * 
   * @param eventsBatch
   */
  public synchronized void sendBatch(HttpPostable eventsBatch) {
    if (eventsBatch.isFlushed()) {
      throw new HecConnectionStateException(
              "Illegal attempt to send already-flushed batch. EventBatch is not reusable.",
              HecConnectionStateException.Type.ALREADY_FLUSHED);
    }

    eventsBatch.post(this.hecIOManager);

  }

  /**
   * @return the simulated
   */
  public boolean isSimulated() {
    return simulatedEndpoints != null;
  }

  /**
   * Close events sender
   */
  @Override
  public void close() {
//    if (null != eventsBatch) { //can happen if no msgs sent on this sender
//      eventsBatch.close();
//    }
    this.hecIOManager.close();
    if (null != simulatedEndpoints) {
      simulatedEndpoints.close();
    }
    stopHttpClient();
  }

  /**
   * Disable https certificate validation of the splunk server. This
   * functionality is for development purpose only.
   */
  public void disableCertificateValidation() {
    disableCertificateValidation = true;
  }
  
  public ChannelMetrics getChannelMetrics() {
    return this.channel.getChannelMetrics();
  }

  /**
   * Validate if http client started, call failed callback in case of an error
   *
   * @return true if started, false otherwise
   */
  public synchronized boolean started() {
    if (isSimulated()) {
      simulatedEndpoints.start();
      return true;
    }
    
    if (httpClient != null) {
      // http client is already started
      return true;
    }
    return false;
  }

  @Override
  public void start() {
    // attempt to create and start an http client
    try {
      httpClient = new HttpClientFactory(disableCertificateValidation,
              cert, host, this).build();
      httpClient.start();
    } catch (Exception ex) {
      LOG.error("Exception building httpClient: " + ex.getMessage(), ex);
      ConnectionCallbacks callbacks = getChannel().getCallbacks();
      callbacks.failed(null, ex);
    }
  }

  // Currently we never close http client. This method is added for symmetry
  // with startHttpClient.
  private void stopHttpClient() throws SecurityException {
    if (httpClient != null) {
      try {
        httpClient.close();
      } catch (IOException e) {
          LOG.error("Failed to shutdown HttpSender {}", e.getMessage());
      }
      httpClient = null;
    }
  }

  // set splunk specific http request headers
  private void setHttpHeaders(HttpRequestBase r) {
    r.setHeader(
            AuthorizationHeaderTag,
            String.format(AuthorizationHeaderScheme, token));
    
    r.setHeader(
            ChannelHeader,
            getChannel().getChannelId());
    
    if (host != null) {
      r.setHeader(Host, host);
    }
  }
  
  @Override
  public void postEvents(final HttpPostable events,
          FutureCallback<HttpResponse> httpCallback) {
    // make sure http client or simulator is started
    if (!started()) {
      start();
    }
    
    if (isSimulated()) {
      this.simulatedEndpoints.postEvents(events, httpCallback);
      return;
    }
    final String encoding = "utf-8";

    // create http request
    String endpointUrl = ((EventBatch)events).getTarget()
            == ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT ? eventUrl : rawUrl;
    if (endpointUrl == null) {
      throw new NullPointerException();
    }
    final HttpPost httpPost = new HttpPost(endpointUrl);
    setHttpHeaders(httpPost);
    
    httpPost.setEntity(events.getEntity());
    httpClient.execute(httpPost, httpCallback);
  }
  
  @Override
  public void pollAcks(HecIOManager hecIoMgr,
          FutureCallback<HttpResponse> httpCallback) {
    if (!started()) {
      start();
    }; // make sure http client or simulator is started
    AcknowledgementTracker.AckRequest ackReq = hecIoMgr.getAckPollRequest();
    try {
      if (ackReq.isEmpty()) {
          LOG.trace("no ackIds to poll for");
        return;
      } else {        
        hecIoMgr.setAckPollInProgress(true);
      }
      if (isSimulated()) {
        LOG.debug("SIMULATED POLL ACKS");
        this.simulatedEndpoints.pollAcks(hecIoMgr, httpCallback);
        return;
      }
      final HttpPost httpPost = new HttpPost(ackUrl);
      setHttpHeaders(httpPost);
      
      StringEntity entity;
      
      String req = ackReq.toString();
      LOG.debug("channel=" + getChannel() + " posting: " + req);
      entity = new StringEntity(req);
      
      entity.setContentType(HttpContentType);
      httpPost.setEntity(entity);
      httpClient.execute(httpPost, httpCallback);
    } catch (Exception ex) {
      hecIoMgr.setAckPollInProgress(false);
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }
  
  @Override
  public void pollHealth(FutureCallback<HttpResponse> httpCallback) {
    // make sure http client or simulator is started
    if (!started()) {
      start();
    }
    if (isSimulated()) {
      LOG.debug("SIMULATED POLL HEALTH");
      this.simulatedEndpoints.pollHealth(httpCallback);
      return;
    }
    // create http request
    final String getUrl = String.format("%s?ack=1&token=%s", healthUrl, token);
    final HttpGet httpGet = new HttpGet(getUrl);
    LOG.trace("Polling health {}", httpGet);
    setHttpHeaders(httpGet);
    httpClient.execute(httpGet, httpCallback);
  }

  @Override
  public void ackEndpointCheck(FutureCallback<HttpResponse> httpCallback) {
    if (!started()) {
      start();
    }
    if (isSimulated()) {
      this.simulatedEndpoints.ackEndpointCheck(httpCallback);
      return;
    }
    Set<Long> dummyAckId = new HashSet<>();
    dummyAckId.add(0L);
    AcknowledgementTracker.AckRequest dummyAckReq = new AcknowledgementTracker.AckRequest(dummyAckId);

    try {
      final HttpPost httpPost = new HttpPost(ackUrl);
      setHttpHeaders(httpPost);

      StringEntity entity;

      String req = dummyAckReq.toString();
      entity = new StringEntity(req);
      entity.setContentType(HttpContentType);
      httpPost.setEntity(entity);
      httpClient.execute(httpPost, httpCallback);
    } catch (Exception ex) {
      LOG.error(ex.getMessage());
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  /**
   * @return the simulatedEndpoints
   */
  public Endpoints getSimulatedEndpoints() {
    return simulatedEndpoints;
  }

  /**
   * @param simulatedEndpoints the simulatedEndpoints to set
   */
  public void setSimulatedEndpoints(
          Endpoints simulatedEndpoints) {
    this.simulatedEndpoints = simulatedEndpoints;
  }

  public String getToken() {
    return token;
  }

  /**
   * @return the baseUrl
   */
  public String getBaseUrl() {
    return baseUrl;
  }
  
}
