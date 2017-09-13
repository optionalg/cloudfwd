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
package com.splunk.cloudfwd.sim;

import com.splunk.cloudfwd.http.HttpPostable;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class EventEndpoint implements Endpoint {

  private static final Logger LOG = LoggerFactory.getLogger(EventEndpoint.class.
          getName());
  Random rand = new Random(System.currentTimeMillis());
  final ScheduledExecutorService executor;
  private AcknowledgementEndpoint ackEndpoint;

  protected EventEndpoint() {
    ThreadFactory f = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "EventEndpoint");
      }
    };
    executor = Executors.newScheduledThreadPool(1, f);
  }

  public EventEndpoint(AcknowledgementEndpoint ackEndpoint) {
    this();
    this.ackEndpoint = ackEndpoint;
    //ackEndpoint.start();
  }

  public void post(HttpPostable events, FutureCallback<HttpResponse> cb) {
    Runnable respond = () -> {
      cb.completed(new EventPostResponse(
              new AckIdRespEntity(nextAckId())
      ));
    };
    //return a single response with a delay uniformly distributed between  [0,5] ms
    delayResponse(respond);
  }
  
  protected void delayResponse(Runnable r) {
    //return a single response with a delay uniformly distributed between  [0,5] ms
    executor.schedule(r, (long) rand.nextInt(2), TimeUnit.MILLISECONDS);
  }

  protected long nextAckId() {
    return ackEndpoint.nextAckId();
  }

  private static HttpEntity nextAckRespEntity(final int ackId) {

    return new AckIdRespEntity(ackId);
  }

  /**
   * @return the ackEndpoint
   */
  public AcknowledgementEndpoint getAckEndpoint() {
    return ackEndpoint;
  }

  @Override
  public void close() {
    LOG.debug("SHUTDOWN EVENT ENDPOINT DELAY SIMULATOR");
    executor.shutdownNow();
  }

  @Override
  public void start() {
    //no-op
  }

  private static class EventPostResponse extends CannedOKHttpResponse {

    public EventPostResponse(HttpEntity e) {
      super(e);
    }
  }

  private static class AckIdRespEntity extends CannedEntity {

    public AckIdRespEntity(long ackId) {
      super("{\"ackId\":" + ackId + "}");
    }
  }

}
