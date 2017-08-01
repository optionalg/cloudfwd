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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.http.AckManager;
import com.splunk.cloudfwd.http.EventBatch;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 *
 * @author ghendrey
 */
public class AckEndpoint {
  ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
  AtomicInteger ackId = new AtomicInteger(0);
  NavigableMap<Integer, Boolean> acksStates= new ConcurrentSkipListMap<>();
  Random rand = new Random(System.currentTimeMillis());
  
  //start periodically flipping ackIds from false to true. This simulated event batches getting indexed.
  //To mimick the observed behavior of splunk, we flip the lowest unacknowledge ackId before
  //any higher ackId
  public void start(){
    //stateFrobber will set the ack to TRUE
    Runnable stateFrobber = new Runnable(){
      @Override
      public void run() {
        Integer lowestKey = acksStates.floorKey(0);
        if(null == lowestKey){
          return;
        }
        acksStates.put(lowestKey, true); //flip it
      }      
    };
    long when = rand.nextInt(30); //outstanding acks get flipped from false to true everu [0.30] ms
    executor.scheduleAtFixedRate(stateFrobber, 0, when, TimeUnit.MILLISECONDS);
  }
  
  public int nextAckId(){
    int newId = this.ackId.incrementAndGet();
    this.acksStates.put(newId, false);
    return newId;
  }
  
  private Boolean check(int ackId){
    return this.acksStates.remove(ackId);    
  }
  
  
  
  
  public void pollAcks(AckManager ackMgr,FutureCallback<HttpResponse> cb){
    try {
      Set<EventBatch> unacked = ackMgr.getAckWindow().getUnacknowleldgedEvents();
      Map<Integer, Boolean> acks = new LinkedHashMap<>();
      unacked.stream().forEach((events)->{
        int ackId = events.getAckId().intValue();
        Boolean was = check(ackId);
        if(was != null){
          acks.put(ackId, was);
        }
      });
      Map resp = new HashMap();
      resp.put("acks", acks);
      cb.completed(getResult(resp));
    } catch (JsonProcessingException ex) {
      cb.failed(ex);
    }
  }

  private HttpResponse getResult(Map acks) throws JsonProcessingException {
          ObjectMapper serializer = new ObjectMapper();
      String str = serializer.writeValueAsString(acks);
    AckEndpointResponseEntity e = new AckEndpointResponseEntity(str);
    return new AckEndpointResponse(e);
  }

  private static class AckEndpointResponseEntity extends CannedEntity{

    public AckEndpointResponseEntity(String acks) {
      super(acks);
    }
  }

  private static class AckEndpointResponse extends CannedOKHttpResponse{

    public AckEndpointResponse(AckEndpointResponseEntity e) {
      super(e);
    }
  }
}
