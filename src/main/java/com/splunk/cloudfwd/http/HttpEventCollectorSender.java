package com.splunk.cloudfwd.http;

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
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.LoggingChannel;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.cert.X509Certificate;
import java.util.logging.Logger;

/**
 * This is an internal helper class that sends logging events to Splunk http
 * event collector.
 */
public final class HttpEventCollectorSender implements Endpoints {

  private static final Logger LOG = Logger.getLogger(
          HttpEventCollectorSender.class.getName());

  public static final String MetadataTimeTag = "time";
  public static final String MetadataHostTag = "host";
  public static final String MetadataIndexTag = "index";
  public static final String MetadataSourceTag = "source";
  public static final String MetadataSourceTypeTag = "sourcetype";
  private static final String AuthorizationHeaderTag = "Authorization";
  private static final String AuthorizationHeaderScheme = "Splunk %s";
  private static final String HttpContentType = "application/json; profile=urn:splunk:event:1.0; charset=utf-8";
  private static final String ChannelHeader = "X-Splunk-Request-Channel";
  private static final String CookieHeader = "Cookie";
  private final AckManager ackManager;

  /**
   * Recommended default values for events batching.
   */
  public static final int DefaultBatchInterval = 10 * 1000; // 10 seconds
  public static final int DefaultBatchSize = 10 * 1024; // 10KB
  public static final int DefaultBatchCount = 10; // 10 events

  private final String eventUrl;
  private final String rawUrl;
  private final String token;
  private EventBatch eventsBatch;// = new EventBatch();
  private CloseableHttpAsyncClient httpClient;
  private boolean disableCertificateValidation = false;
  private LoggingChannel channel = null;
  private ElbCookie cookie;
  private final String ackUrl;
  private final String healthUrl;
  private Endpoints simulatedEndpoints;

  /**
   * Initialize HttpEventCollectorSender
   *
   * @param url http event collector input server
   * @param token application token
   */
  public HttpEventCollectorSender(final String url, final String token) {
    this.eventUrl = url.trim() + "/services/collector/event";
    this.rawUrl = url.trim() + "/services/collector/raw";
    this.ackUrl = url.trim() + "/services/collector/ack";
    this.healthUrl = url.trim() + "/services/collector/health";
    this.token = token;
    this.ackManager = new AckManager(this);
  }

  public LoggingChannel getChannel() {
    return channel;
  }

  public Connection getConnection(){
    return channel.getConnection();
  }

  public AckWindow getAckWindow() {
    return ackManager.getAckWindow();
  }

  public AckManager getAckManager() {
    return this.ackManager;
  }

  /**
   * Immediately send the EventBatch
   *
   * @param events the batch of events to immediately send
   */
  public synchronized void sendBatch(EventBatch events) {
    if (events.isFlushed()) {
      String msg = "Illegal attempt to send already-flushed batch. EventBatch is not reusable.";
      LOG.severe(msg);
      throw new IllegalStateException(msg);
    }
    this.eventsBatch = events;
    eventsBatch.setSender(this);
    /*
    if (isSimulated()) {
      eventsBatch.setSimulatedEndpoints(this.simulatedEndpoints);
    }*/

    eventsBatch.flush();
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
    if (null != eventsBatch) { //can happen if no msgs sent on this sender
      eventsBatch.close();
    }
    this.ackManager.close();
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
    return this.ackManager.getChannelMetrics();
  }

  @Override
  public synchronized void start() {
    if (isSimulated()) {
      simulatedEndpoints.start();
    }

    if (httpClient != null) {
      // http client is already started or we don't need it because we are simulated
      return;
    }
    // limit max  number of async requests in sequential mode, 0 means "use
    // default limit"
    if (!disableCertificateValidation) {
      // create an http client that validates certificates
      httpClient = HttpAsyncClients.custom()
              .setMaxConnTotal(0) //parallel requests
              .build();
    } else {
      // create strategy that accepts all certificates
      TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
        public boolean isTrusted(X509Certificate[] certificate,
                String type) {
          return true;
        }
      };
      SSLContext sslContext = null;
      try {
        sslContext = SSLContexts.custom().loadTrustMaterial(
                null, acceptingTrustStrategy).build();
        httpClient = HttpAsyncClients.custom()
                .setMaxConnTotal(0) //parallel requests
                .setHostnameVerifier(
                        SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)
                .setSSLContext(sslContext)
                .build();
      } catch (Exception e) {
        LOG.severe(e.getMessage());
        e.printStackTrace(); //fixme TODO
      }
    }
    httpClient.start();
  }

  // Currently we never close http client. This method is added for symmetry
  // with startHttpClient.
  private void stopHttpClient() throws SecurityException {
    if (httpClient != null) {
      try {
        httpClient.close();
      } catch (IOException e) {
      }
      httpClient = null;
    }
  }

  @Override
  public void postEvents(final EventBatch events,
          FutureCallback<HttpResponse> httpCallback) {
    start(); // make sure http client or simulator is started
    if (isSimulated()) {
      this.simulatedEndpoints.postEvents(events, httpCallback);
      return;
    }
    final String encoding = "utf-8";

    // create http request
    String endpointUrl = ((events.getEndpoint() == EventBatch.Endpoint.event ? eventUrl : rawUrl));
    final HttpPost httpPost = new HttpPost(endpointUrl);
    httpPost.setHeader(
            AuthorizationHeaderTag,
            String.format(AuthorizationHeaderScheme, token));

    httpPost.setHeader(
            ChannelHeader,
            getChannel().getChannelId());

    setCookieIfExists(httpPost);

    StringEntity entity = new StringEntity(eventsBatch.toString(),//eventsBatchString.toString(),
            encoding);
    entity.setContentType(HttpContentType);
    httpPost.setEntity(entity);
    httpClient.execute(httpPost, httpCallback);
  }

  @Override
  public void pollAcks(AckManager ackMgr,
          FutureCallback<HttpResponse> httpCallback) {
    start(); // make sure http client or simulator is started
    if (isSimulated()) {
      System.out.println("SIMULATED POLL ACKS");
      this.simulatedEndpoints.pollAcks(ackMgr, httpCallback);
      return;
    }

    final String encoding = "utf-8";

    // create http request
    final HttpPost httpPost = new HttpPost(ackUrl);
    httpPost.setHeader(
            AuthorizationHeaderTag,
            String.format(AuthorizationHeaderScheme, token));

    httpPost.setHeader(
            ChannelHeader,
            getChannel().getChannelId());

    setCookieIfExists(httpPost);

    StringEntity entity;
    try {
      String req = ackMgr.getAckPollReq();
      System.out.println("channel=" + getChannel() + " posting: " + req);
      entity = new StringEntity(req);
    } catch (UnsupportedEncodingException ex) {
      LOG.severe(ex.getMessage());
      throw new RuntimeException(ex.getMessage(), ex);
    }
    entity.setContentType(HttpContentType);
    httpPost.setEntity(entity);
    httpClient.execute(httpPost, httpCallback);
  }

  @Override
  public void pollHealth(FutureCallback<HttpResponse> httpCallback) {
    start(); // make sure http client or simulator is started
    if (isSimulated()) {
      System.out.println("SIMULATED POLL HEALTH");
      this.simulatedEndpoints.pollHealth(httpCallback);
      return;
    }
    // create http request
    final String getUrl = String.format("%s?ack=1&token=%s", healthUrl, token);
    final HttpGet httpGet = new HttpGet(getUrl);
    httpGet.setHeader(
            AuthorizationHeaderTag,
            String.format(AuthorizationHeaderScheme, token));

    httpGet.setHeader(
            ChannelHeader,
            getChannel().getChannelId());

    setCookieIfExists(httpGet);

    httpClient.execute(httpGet, httpCallback);
  }

  private void setCookieIfExists(HttpRequestBase request) {
    if (cookie != null) {
      request.setHeader(
              CookieHeader,
              cookie.getNameValuePair());
    }
  }

  public void setCookie(ElbCookie cookie) {
    this.cookie = cookie;
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

  public void setChannel(LoggingChannel c) {
    this.channel=c;
  }

}
