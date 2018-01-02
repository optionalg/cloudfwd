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
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.splunk.cloudfwd.impl.util.ThreadScheduler;
import java.net.URISyntaxException;
import org.apache.http.HttpEntity;
import org.apache.http.client.utils.URIBuilder;

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
  private static final String Host = "host";
  private final String eventUrl;
  private final String rawUrl;
//  private final String token;
    private final String cert;
    private final String host; //hostname used for SSL certificate validation
//  private final String index;
//  private final String source;
//  private final String sourcetype;
  private CloseableHttpAsyncClient httpClient;
  private boolean disableCertificateValidation = false;
  private HecChannel channel = null;
  private final String ackUrl;
  private final String healthUrl;
  private Endpoints simulatedEndpoints;
  private final HecIOManager hecIOManager;
  private final String baseUrl; 
  private String cookie;
  //the following  posts/gets are used by health checks and preflight checks. We record them so we can cancel them on close. 
  private HttpPost ackCheck;
  private HttpGet healthEndpointCheck;
  private HttpPost dummyEventPost;
  private final ConnectionSettings connectionSettings;

  /**
   * Initialize HttpEventCollectorSender
   *
     * @param url THe URL for the host portion of the URL must be an IP address NOT a hostname
   * @param connectionSettings ConnectionSettings
   * @param disableCertificateValidation disables Certificate Validation
   * @param cert SSL Certificate Authority Public key to verify TLS with
   * Self-Signed SSL Certificate chain
   */
  public HttpSender(final String url, String ssl_host, final ConnectionSettings connectionSettings,
                    final boolean disableCertificateValidation, final String cert) {
    this.baseUrl = url;
    this.host = ssl_host;
    this.connectionSettings = connectionSettings;
//    this.index = connectionSettings.getIndex();
//    this.sourcetype = connectionSettings.getSourcetype();
//    this.source = connectionSettings.getSource();
//    this.token = connectionSettings.getToken();
    this.cert = cert;
    this.hecIOManager = new HecIOManager(this);
    this.eventUrl = this.baseUrl.trim() + "/services/collector/event";
    this.rawUrl = this.baseUrl.trim() + "/services/collector/raw";
    this.ackUrl = this.baseUrl.trim() + "/services/collector/ack";
    this.healthUrl = this.baseUrl.trim() + "/services/collector/health";
    this.disableCertificateValidation = disableCertificateValidation;

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

    public synchronized void abortPreflightAndHealthcheckRequests() {
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
        synchronized(this){
            if (httpClient != null) { //double check required once we enter synchronized block
                // http client is already started
                return true;
            }
        }
    }
    return false;
  }

  @Override
  public synchronized void start() {
    // attempt to create and start an http client
    try {
        this.httpClient = HttpClientHostMapper.getClientWrapper(this).getClient(this, disableCertificateValidation,cert);
    } catch (Exception ex) {
      LOG.error("Exception building httpClient: " + ex.getMessage(), ex);
      ConnectionCallbacks callbacks = getChannel().getCallbacks();
      callbacks.failed(null, ex);
    }
  }
  
  /**
   * Close events sender
   */
  @Override
  public synchronized void close() {
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

  // Currently we never close http client. This method is added for symmetry
  // with startHttpClient.
  private void stopHttpClient() throws SecurityException {
    if (httpClient != null) {
        //HttpClientWrapper.releaseClient(this);
         HttpClientHostMapper.getClientWrapper(this).releaseClient(this);
        httpClient = null;
    }
  }

  private String appendUri(String endpoint) {
      try {
          URIBuilder builder = new URIBuilder(endpoint);
          
          String index = connectionSettings.getIndex();
          if (!StringUtils.isEmpty(index)) {
              builder.addParameter("index", index);
          }
          
          String source = connectionSettings.getSource();
          if (!StringUtils.isEmpty(source)) {
              builder.addParameter("source", source);
          }
          
          String sourcetype =connectionSettings.getSourcetype();
          if (!StringUtils.isEmpty(sourcetype)) {
              builder.addParameter("sourcetype", sourcetype);
          }
          
          String hostField = connectionSettings.getHost();
          if (!StringUtils.isEmpty(hostField)) {
              builder.addParameter("host", hostField);
          }
          return builder.toString();
      } catch (URISyntaxException ex) {
          LOG.warn("problem with HEC endpoint URL: {}", ex.getMessage());
          return endpoint;
      }
  }
  
  private void setHeaders(HttpRequestBase r){
      setHttpHeadersNoChannel(r);
      setHttpChannelHeaders(r);
      setCookieHeader(r);
  }
  
  private void setCookieHeader(HttpRequestBase r){
      if(null != this.cookie && !this.cookie.isEmpty()){
          r.setHeader("Cookie", this.cookie);
      }
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
            String.format(AuthorizationHeaderScheme, connectionSettings.getToken()));
    
    if (host != null) {
      r.setHeader(Host, host);
    }
  }
  
  @Override
  public synchronized void postEvents(final HttpPostable events,
          FutureCallback<HttpResponse> httpCallback) {
    // make sure http client or simulator is started
    if (!started()) {
      start();
    }
    ((EventBatch)events).getLifecycleMetrics().setPostSentTimestamp(System.currentTimeMillis());
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

    String completeUrl = appendUri(endpointUrl);
    final HttpPost httpPost = new HttpPost(completeUrl);
    setHeaders(httpPost);
    HttpEntity e= events.getEntity();
    LOG.debug("executing event batch post on channel={}, eventBatch={}", getChannel(), e.toString());
    httpPost.setEntity(e);
    httpClient.execute(httpPost, httpCallback);
  }

    /**
     * This method exists because the /raw or /events endpoints are the only way to detect a disabled token
     * @param httpCallback
     */
    @Override
    public synchronized void checkRawEndpoint(FutureCallback<HttpResponse> httpCallback){
        try{    
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
            empty = new StringEntity("");
            dummyEventPost.setEntity(empty);
            LOG.debug("executing empty event post to raw on channel={}. Request: {}", getChannel(), dummyEventPost);
            httpClient.execute(dummyEventPost, httpCallback);
          } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }  
  
  @Override
  public synchronized void pollAcks(HecIOManager hecIoMgr,
          FutureCallback<HttpResponse> httpCallback) {
    try {
        if (!started()) {
          start();
        }; // make sure http client or simulator is started
        AcknowledgementTracker.AckRequest ackReq = hecIoMgr.getAckPollRequest();        
        if (ackReq.isEmpty()) {
            LOG.trace("no ackIds to poll for on {}", getChannel());
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
        LOG.debug("executing ack poll request on channel={} posting: {}", getChannel(), req);
        entity = new StringEntity(req);

        entity.setContentType(HttpContentType);
        httpPost.setEntity(entity);
        if(null != httpClient){ //httpClient can be null if close happened
            httpClient.execute(httpPost, httpCallback);
        }
    } catch (Exception ex) {
      hecIoMgr.setAckPollInProgress(false);
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }
  
  
  @Override
  public synchronized void checkHealthEndpoint(FutureCallback<HttpResponse> httpCallback) {
    try{
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
        final String getUrl = String.format("%s?ack=1&token=%s", healthUrl, connectionSettings.getToken());
        healthEndpointCheck= new HttpGet(getUrl);
        LOG.debug("executing poll on health endpoint, channel={}. Request: {}", getChannel(), healthEndpointCheck);
        setHeaders(healthEndpointCheck);
        if(null != httpClient){ //httpClient can be null if close happened
            httpClient.execute(healthEndpointCheck, httpCallback);
        }
    }catch (Exception ex) {
      LOG.error("{}", ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  @Override
  public synchronized void checkAckEndpoint(FutureCallback<HttpResponse> httpCallback) {
    try {
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
        this.ackCheck = new HttpPost(ackUrl);
        setHeaders(ackCheck); 

        StringEntity entity;

        String req = dummyAckReq.toString();
        LOG.debug(req);
        entity = new StringEntity(req);
        LOG.trace("checking health via ack endpoint: {}", req);
        entity.setContentType(HttpContentType);
        ackCheck.setEntity(entity);
        if(null != httpClient){ //httpClient can be null if close happened
          LOG.debug("executing ack check on channel={}", getChannel());
          httpClient.execute(ackCheck, httpCallback);
        }else{
            LOG.error("httpClient is null");
        }
    } catch (Exception ex) {
      LOG.error("{}", ex.getMessage(), ex);
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
            synchronized (this) { //we don't want to make multiple attempts to handle the same detected change
                if (null != this.cookie && !this.cookie.equals(cookie)) { //must double-check the condition once inside sync'd block
                    LOG.warn(
                            "An attempt was made to change the Session-Cookie from {} to {} on {}",
                            this.cookie, cookie, getChannel());
                    this.cookie = cookie; //record the new cookie so that subsequent same cookies don't try to resend again                    
                    LOG.warn("replacing channel, resending events, and killing {}",
                            getChannel());
                    getChannel().killAckTracker(); //we want to immediately ignore any in-flight acks that could arrive from the channel
                    getChannel().close(); //close the channel as quickly as possible to prevent more event piling into it
                    resendEvents(); //will ultimately result in this channel getting killed
                }
            }//end sync
        } else {
            this.cookie = cookie;
        }
    }
    
    private void resendEvents(){
        Runnable r = ()->{
             try {
                getChannel().closeAndReplaceAndResend();
            } catch (Exception ex) {
                ex.printStackTrace();
                LOG.error("Exception '{}' trying to handle sticky session-cookie violation on channel={}", ex.getMessage(), getChannel(), ex);
            }            
        };//end runnable
        ThreadScheduler.getSharedExecutorInstance("event_resender").execute(r);
    }
    
    public String getSslHostname(){
        return this.host;
    }
  
}
