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
import com.splunk.cloudfwd.impl.CookieClient;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.util.HecChannel;
import com.splunk.cloudfwd.impl.util.LoadBalancer;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class performs the actually HTTP send to HEC
 * event collector.
 */
public final class HttpSender implements Endpoints, CookieClient {

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
  private String cookie;
  private ExecutorService resenderExecutor;
  //the following  posts/gets are used by health checks an preflight checks 
  private HttpPost ackCheck;
  private HttpGet healthEndpointCheck;
  private HttpPost dummyEventPost;

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
    abortPreflightAndHealthcheckRequests();    
    stopHttpClient();
  }

    public void abortPreflightAndHealthcheckRequests() {
        if(null != this.ackCheck){
            ackCheck.abort();
        }
        if(null != this.healthEndpointCheck){
            healthEndpointCheck.abort();
        }
        if(null != dummyEventPost){
            dummyEventPost.abort();
        }
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
  
  private void setHeaders(HttpRequestBase r){
      setHttpHeadersNoChannel(r);
      setHttpChannelHeaders(r);
  }
  
    // set splunk specific http request headers
  private void setHttpChannelHeaders(HttpRequestBase r) {
    r.setHeader(
            ChannelHeader,
            getChannel().getChannelId());
  }

  // set splunk specific http request headers
  private void setHttpHeadersNoChannel(HttpRequestBase r) {
    r.setHeader(
            AuthorizationHeaderTag,
            String.format(AuthorizationHeaderScheme, token));
    
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
      throw new NullPointerException("endpointUrl was null.");
    }
    final HttpPost httpPost = new HttpPost(endpointUrl);
    setHeaders(httpPost);
    
    httpPost.setEntity(events.getEntity());
    httpClient.execute(httpPost, httpCallback);
  }

    /**
     * This method exists because the /raw or /events endpoints are the only way to detect a disabled token
     * @param httpCallback
     */
    @Override
    public void checkRawEndpoint(FutureCallback<HttpResponse> httpCallback){
    // make sure http client or simulator is started
    if (!started()) {
      start();
    }
    EventBatchImpl dummyEvents = new EventBatchImpl();
    dummyEvents.setAckId(MAX_ACK_ID); //make sure we don't poll for acks with ackId=0 which could actually be in flight

    if (isSimulated()) {     
      this.simulatedEndpoints.checkRawEndpoint(httpCallback);
      return;
    }
    final String encoding = "utf-8";

    // create http request

    this.dummyEventPost = new HttpPost(rawUrl);
    setHeaders(dummyEventPost);
    StringEntity empty;
      try {
          empty = new StringEntity(Strings.EMPTY);
          dummyEventPost.setEntity(empty);
          httpClient.execute(dummyEventPost, httpCallback);
      } catch (UnsupportedEncodingException ex) {
          LOG.error(ex.getMessage(),ex);
      }
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
      setHeaders(httpPost);
      
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
  public void checkHealthEndpoint(FutureCallback<HttpResponse> httpCallback) {
    // make sure http client or simulator is started
    if (!started()) {
      start();
    }
    if (isSimulated()) {
      LOG.debug("SIMULATED POLL HEALTH");
      this.simulatedEndpoints.checkHealthEndpoint(httpCallback);
      return;
    }
    // create http request
    final String getUrl = String.format("%s?ack=1&token=%s", healthUrl, token);
    healthEndpointCheck= new HttpGet(getUrl);
    LOG.trace("Polling health {}", healthEndpointCheck);
    setHeaders(healthEndpointCheck);
    httpClient.execute(healthEndpointCheck, httpCallback);
  }

  @Override
  public void checkAckEndpoint(FutureCallback<HttpResponse> httpCallback) {
    if (!started()) {
      start();
    }
    if (isSimulated()) {
      this.simulatedEndpoints.checkAckEndpoint(httpCallback);
      return;
    }
    Set<Long> dummyAckId = new HashSet<>();
    dummyAckId.add(MAX_ACK_ID);//default max ack Id. TODO we should not let channels send this many event batches
    AcknowledgementTracker.AckRequest dummyAckReq = new AcknowledgementTracker.AckRequest(dummyAckId);

    try {
      this.ackCheck = new HttpPost(ackUrl);
      setHeaders(ackCheck); 

      StringEntity entity;

      String req = dummyAckReq.toString();
      LOG.debug(req);
      entity = new StringEntity(req);
      LOG.trace("checking health via ack endpoint: {}", req);
      entity.setContentType(HttpContentType);
      ackCheck.setEntity(entity);
      httpClient.execute(ackCheck, httpCallback);
    } catch (Exception ex) {
      LOG.error("Exception in checkAckEndpoint: {}",ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }
    private static final long MAX_ACK_ID = 100000L;

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

  @Override
  public void setSessionCookies(String cookie) {
    if (null == cookie || cookie.isEmpty()) {
      return;
    }
    if (null != this.cookie && !this.cookie.equals(cookie)) {
      synchronized(this) {
        LOG.warn("An attempt was made to change the Session-Cookie from {} to {} on {}", this.cookie, cookie, getChannel());
        this.cookie = cookie;
        LOG.warn("replacing channel, resending events, and killing {}", getChannel());
        getAcknowledgementTracker().kill();
        getChannel().close();
        resendEvents();
      }//end sync
    } else {
      this.cookie = cookie;
    }
  }

  private void resendEvents(){
    Runnable r = ()->{
      try {
        HecChannel c = getChannel();
        LoadBalancer lb = getConnection().getLoadBalancer();
        lb.addChannelFromRandomlyChosenHost(); //to compensate for the channel we are about to smoke
        lb.removeChannel(c.getChannelId(), true); //bye bye
        c.forceClose(); //smoke
        c.resendInFlightEvents();
      } catch (Exception ex) {
        LOG.error("Excepton '{}' trying to handle sticky session-cookie violation on {}", ex.getMessage(), getChannel(), ex);
      }
    };//end runnable
    
    if (this.resenderExecutor == null) {
      ThreadFactory f = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          return new Thread(r, "event resender channel " + getChannel());
        }
      };
      this.resenderExecutor = Executors.newSingleThreadExecutor(f);
    }
    this.resenderExecutor.execute(r);
  }
}
