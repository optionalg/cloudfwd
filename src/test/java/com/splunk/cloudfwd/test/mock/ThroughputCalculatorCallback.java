package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.LoggerFactory;

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
/**
 *
 * @author ghendrey
 */
public class ThroughputCalculatorCallback extends BasicCallbacks {

    protected static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ThroughputCalculatorCallback.class.
          getName());

  Map<Comparable, Long> batchSizes = new ConcurrentHashMap<>();
  AtomicLong count = new AtomicLong(0);
  AtomicLong ackedSize = new AtomicLong(0);
  AtomicLong failedCount = new AtomicLong(0);
  AtomicLong batchCount = new AtomicLong(0);
  AtomicLong totLatency = new AtomicLong(0);
  long lastLatency;
  double meanSquaredLatency = 0;


  public long getAcknowledgedSize() {
    return ackedSize.get();
  }

  @Override
  public void failed(EventBatch events, Exception ex) {
      
    LOG.error("EventBatch failure recorded. Exception message: " + ex.
            getMessage(), ex);
    if(null != events){
        LOG.error("Failed event batch: {}", events);
    }
    failedCount.incrementAndGet();
  }

  @Override
  public void acknowledged(EventBatch events) {
    super.acknowledged(events);
    lastLatency = System.currentTimeMillis() - ((EventBatchImpl)events).getSendTimestamp();
    totLatency.addAndGet(lastLatency);

    Long size = batchSizes.remove(events.getId());
    if (size != null) {
      if (size < 0) {
        throw new RuntimeException("negative size:" + size);
      }
      ackedSize.addAndGet(size);
    } else {
      //no-op. acknowledgements happen for batches that were sent during the perf test warmup period
      //during which we would not have recorded the batch via deferCountUntilAck
    }
  }

  public ThroughputCalculatorCallback(int expected) {
    super(expected);
  }

  public void deferCountUntilAck(Comparable id, Long nChars) {
    if (nChars < 1) {
      throw new RuntimeException(
              "Illegal attempt to record non-positive nChars" + nChars);
    }
    this.batchSizes.put(id, nChars);
  }

    private double meanSquaredLatency(long eventLatency, double avgLatency,
            long totLatency) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * @return the avgLatency
     */
    public double getAvgLatency() {
        return ((double)ackedSize.get())/totLatency.get();
    }
    
    public double getLastLatency(){
        return lastLatency;
    }

}
