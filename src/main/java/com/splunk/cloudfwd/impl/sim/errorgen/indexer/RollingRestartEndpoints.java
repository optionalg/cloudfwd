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
package com.splunk.cloudfwd.impl.sim.errorgen.indexer;

import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.CannedOKHttpResponse;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulate communicating with indexer in manual detention
 * @author meemax
 */
public class RollingRestartEndpoints extends SimulatedHECEndpoints {
  private static final Logger LOG = LoggerFactory.getLogger(RollingRestartEndpoints.class.getName());

  private static IndexerStateScheduler scheduler = new IndexerStateScheduler();
  private static int[] numPerUrl = new int[1];
  private static int numOfUrl = 1;
  private static int numOfChannelsPerUrl = 1;
  private static int nextIndexerId = 0;
  private static int downIndexer = -1;

  private DownIndexerAckEndpoint downAckEndpoint;
  private DownIndexerEventEndpoint downEventEndpoint;
  private DownIndexerHealthEndpoint downHealthEndpoint;

  // defaults to 0, all channels will point to same url/indexer if not updated
  private int indexId = 0;


  // call before start()
  public static void init(int urls, int channels) {
    numPerUrl = new int[urls];
    numOfChannelsPerUrl = channels;
    numOfUrl = urls;
  }

  // only start when all indexers have channels
  private static synchronized void tryRollingRestart() {
    if (!scheduler.hasScheduled() &&
      numPerUrl[numOfUrl-1] >= numOfChannelsPerUrl) {
      scheduler.schedule(new Runnable() {
        @Override
        public void run() {
            if (downIndexer < numOfUrl) {
              downIndexer++;
              LOG.debug("downed indexer id {}", downIndexer);
            }
            else { // downIndexer > numOfUrl
              // just go through indexers once like a rolling restart
              scheduler.shutdown();
            }
        }
      }, 60, TimeUnit.SECONDS);
    }
  }

  private synchronized void assignIndexerId() {
    // if it hit max, we're already defunc-ing some channels
    if (numPerUrl[nextIndexerId] < numOfChannelsPerUrl)
      numPerUrl[nextIndexerId]++;

    // alternately create a channel through the indexers
    indexId = nextIndexerId++;
    LOG.debug("Assigned indexer id {}", indexId);
    if (nextIndexerId == numOfUrl)
      nextIndexerId = 0;
  }

  @Override
  public synchronized void start() {
      if(started){
          return;
      }
    // assign indexer id, a group of channels should have
    assignIndexerId();
    LOG.debug("STARTING Indexer ID: {}", indexId);

    // create down endpoints
    downAckEndpoint = new DownIndexerAckEndpoint();
    downEventEndpoint = new DownIndexerEventEndpoint();
    downHealthEndpoint = new DownIndexerHealthEndpoint();

    if(indexId == downIndexer){ //this indexer is down
      LOG.trace("indexer of this channel is down");
    }else{
      LOG.trace("indexer of this channel is live");
    }
    super.start();

    // only start when all indexers with respective channels are up
    if (indexId == numOfUrl-1)
      tryRollingRestart();
  }

  @Override
  public void ackEndpointCheck(FutureCallback<HttpResponse> httpCallback) {
    if (indexId == downIndexer) {
      LOG.trace("splunkCheck, indexer down");
      ((HttpCallbacksAbstract)httpCallback).failed(new Exception("Unable to connect"));
    }
    else {
      httpCallback.completed(
          new CannedOKHttpResponse(
            new CannedEntity("Simulated pre-flight check OK")));
    }
  }

  @Override
  public void postEvents(HttpPostable events,
          FutureCallback<HttpResponse> httpCallback) {
    if (indexId == downIndexer) {
      LOG.trace("postEvents, indexer down");
      downEventEndpoint.post(events, httpCallback);
    }
    else {
      eventEndpoint.post(events, httpCallback);
    }
  }

  @Override
  public void pollAcks(HecIOManager ackMgr,
          FutureCallback<HttpResponse> httpCallback) {
    if (indexId == downIndexer) {
      LOG.trace("postAcks, indexer down");
      downAckEndpoint.pollAcks(ackMgr, httpCallback);
    }
    else {
      ackEndpoint.pollAcks(ackMgr, httpCallback);
    }
  }

  @Override
  public void pollHealth(FutureCallback<HttpResponse> httpCallback) {
    if (indexId == downIndexer) {
      LOG.trace("pollHealth, indexer down");
      downHealthEndpoint.pollHealth(httpCallback);
    }
    else {
      this.healthEndpoint.pollHealth(httpCallback);
    }
  }

  static class IndexerStateScheduler {
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> countdown = null;

    public synchronized void schedule(Runnable task, long delay, TimeUnit units) {
        if (countdown != null) {
          return;
        }
        countdown = scheduler.scheduleWithFixedDelay(task, 0, delay, units);
    }

      public synchronized boolean hasScheduled() {
        return countdown != null;
      }

      public synchronized boolean await(long timeout, TimeUnit unit) throws InterruptedException {
          return scheduler.awaitTermination(timeout, unit);
      }

      public synchronized boolean isDone() {
          return (countdown == null) || countdown.isDone();
      }

      public synchronized void shutdown() {
          scheduler.shutdownNow();
          countdown = null;
      }
  }
}