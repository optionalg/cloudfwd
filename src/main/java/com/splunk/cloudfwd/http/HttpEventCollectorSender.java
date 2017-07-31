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
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.cert.X509Certificate;
import java.util.logging.Logger;

/**
 * This is an internal helper class that sends logging events to Splunk http
 * event collector.
 */
public final class HttpEventCollectorSender implements HttpEventCollectorMiddleware.IHttpSender {

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


  /**
   * Sender operation mode. Parallel means that all HTTP requests are
   * asynchronous and may be indexed out of order. Sequential mode guarantees
   * sequential order of the indexed events.
   */
  public enum SendMode {
    Sequential,
    Parallel
  };

  /**
   * Recommended default values for events batching.
   */
  public static final int DefaultBatchInterval = 10 * 1000; // 10 seconds
  public static final int DefaultBatchSize = 10 * 1024; // 10KB
  public static final int DefaultBatchCount = 10; // 10 events

  private final String eventUrl;
  private final String token;
  private EventBatch eventsBatch;// = new EventBatch();
  private CloseableHttpAsyncClient httpClient;
  private boolean disableCertificateValidation = false;
  private final SendMode sendMode;
  private final HttpEventCollectorMiddleware middleware = new HttpEventCollectorMiddleware();
  private final String channel = newChannel();
  private final String ackUrl;
  private final AckMiddleware ackMiddleware;
  private final String healthUrl;

  /**
   * Initialize HttpEventCollectorSender
   *
   * @param url http event collector input server
   * @param token application token
   */
  public HttpEventCollectorSender(final String url, final String token) {
    this.sendMode = SendMode.Parallel;
    this.eventUrl = url.trim() + "/services/collector/event";    
    this.ackUrl = url.trim() + "/services/collector/ack";
    this.healthUrl = url.trim() + "/services/collector/health";
    this.token = token;
    this.ackMiddleware = new AckMiddleware(this);
    this.middleware.add(ackMiddleware);
  }
  
  public String getChannel() {
    return channel;
  }

  private static String newChannel() {
    return java.util.UUID.randomUUID().toString();
  }  

  public AckWindow getAckWindow() {
    return this.ackMiddleware.getAckManager().getAckWindow();
  }

  /**
   * Immediately send the EventBatch
   *
   * @param events the batch of events to immediately send
   */
  public synchronized void sendBatch(EventBatch events) {
    if (events.isFlushed()) {
      LOG.severe(
              "Illegal attempt to send already-flushed batch. EventBatch is not reusable.");
      throw new IllegalStateException(
              "Illegal attempt to send already-flushed batch. EventBatch is not reusable.");
    }
    this.eventsBatch = events;
    eventsBatch.setSender(this);
    eventsBatch.flush();

  }

  /**
   * Close events sender
   */
  public void close() {
    if (null != eventsBatch) { //can happen if no msgs sent on this sender
      eventsBatch.close();
    }
    this.ackMiddleware.close();
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
    return this.ackMiddleware.getChannelMetrics();
  }

  private synchronized void startHttpClient() {
    if (httpClient != null) {
      // http client is already started
      return;
    }
    // limit max  number of async requests in sequential mode, 0 means "use
    // default limit"
    int maxConnTotal = sendMode == SendMode.Sequential ? 1 : 0;
    if (!disableCertificateValidation) {
      // create an http client that validates certificates
      httpClient = HttpAsyncClients.custom()
              .setMaxConnTotal(maxConnTotal)
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
                .setMaxConnTotal(maxConnTotal)
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

  void postEventsAsync(final EventBatch events) {
    this.middleware.postEvents(events, this,
            new HttpEventCollectorMiddleware.IHttpSenderCallback() {
      @Override
      public void completed(int statusCode, String reply) {
        if (statusCode != 200) {
          HttpEventCollectorErrorHandler.error(
                  events,
                  new HttpEventCollectorErrorHandler.ServerErrorException(
                          reply));
        }
      }

      @Override
      public void failed(Exception ex) {
        HttpEventCollectorErrorHandler.error(
                eventsBatch,
                new HttpEventCollectorErrorHandler.ServerErrorException(
                        ex.getMessage()));
      }
    });
  }

  @Override
  public void postEvents(final EventBatch events,
          final HttpEventCollectorMiddleware.IHttpSenderCallback callback) {
    startHttpClient(); // make sure http client is started
    final String encoding = "utf-8";

    // create http request
    final HttpPost httpPost = new HttpPost(eventUrl);
    httpPost.setHeader(
            AuthorizationHeaderTag,
            String.format(AuthorizationHeaderScheme, token));

    httpPost.setHeader(
            ChannelHeader,
            getChannel());

    StringEntity entity = new StringEntity(eventsBatch.toString(),//eventsBatchString.toString(),
            encoding);
    entity.setContentType(HttpContentType);
    httpPost.setEntity(entity);
    httpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse response) {
        String reply = "";
        int httpStatusCode = response.getStatusLine().getStatusCode();
        // read reply only in case of a server error
        //if (httpStatusCode != 200) {
        try {
          reply = EntityUtils.toString(response.getEntity(), encoding);
        } catch (IOException e) {
          e.printStackTrace();
          //if IOException ocurrs toStringing response, this is not something we can expect client 
          //to handle
          throw new RuntimeException(e.getMessage(), e);
          //reply = e.getMessage();
        }
        //}
        callback.completed(httpStatusCode, reply);
      }

      @Override
      public void failed(Exception ex) {
        ex.printStackTrace();
        callback.failed(ex);
      }

      @Override
      public void cancelled() {
      }
    });
  }

  @Override
  public void pollAcks(AckManager ackMgr,
          HttpEventCollectorMiddleware.IHttpSenderCallback callback) {

    startHttpClient(); // make sure http client is started
    final String encoding = "utf-8";

    // create http request
    final HttpPost httpPost = new HttpPost(ackUrl);
    httpPost.setHeader(
            AuthorizationHeaderTag,
            String.format(AuthorizationHeaderScheme, token));

    httpPost.setHeader(
            ChannelHeader,
            getChannel());

    StringEntity entity;
    try {
      String req = ackMgr.getAckPollReq();
      System.out.println("channel=" + getChannel() + " posting acks: " + req);
      entity = new StringEntity(req);
    } catch (UnsupportedEncodingException ex) {
      LOG.severe(ex.getMessage());
      throw new RuntimeException(ex.getMessage(), ex);
    }
    entity.setContentType(HttpContentType);
    httpPost.setEntity(entity);
    httpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse response) {
        String reply = "";
        int httpStatusCode = response.getStatusLine().getStatusCode();
        // read reply only in case of a server error
        //if (httpStatusCode != 200) {
        try {
          reply = EntityUtils.toString(response.getEntity(), encoding);
          //System.out.println("reply: " + reply);	//fixme undo hack
        } catch (IOException e) {
          e.printStackTrace();
          //if IOException ocurrs toStringing response, this is not something we can expect client 
          //to handle
          throw new RuntimeException(e.getMessage(), e);
          //reply = e.getMessage();
        }
        //}
        callback.completed(httpStatusCode, reply);
      }

      @Override
      public void failed(Exception ex) {
        ex.printStackTrace();
        callback.failed(ex);
      }

      @Override
      public void cancelled() {
        System.out.println("cancelled"); //todo fixme
      }
    });

  }

  public void pollHealth(
          HttpEventCollectorMiddleware.IHttpSenderCallback callback) {
    startHttpClient(); // make sure http client is started
    // create http request
    final String getUrl = String.format("%s?ack=1&token=%s", healthUrl, token);
    final HttpGet httpGet = new HttpGet(getUrl);
    httpGet.setHeader(
            AuthorizationHeaderTag,
            String.format(AuthorizationHeaderScheme, token));

    httpGet.setHeader(
            ChannelHeader,
            getChannel());

    httpClient.execute(httpGet, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse response) {
        final String encoding = "utf-8";
        String msg = "";
        int statusCode = response.getStatusLine().getStatusCode();
        try {
          msg = EntityUtils.toString(response.getEntity(), encoding);
        } catch (IOException e) {
          e.printStackTrace(); //fixme todo
          throw new RuntimeException(e.getMessage(), e);
        }
        callback.completed(statusCode, msg);
      }

      @Override
      public void failed(Exception ex) {
        callback.failed(ex);
      }

      @Override
      public void cancelled() {
      }
    });

  }

}
