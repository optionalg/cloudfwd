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
import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 *
 * @author ghendrey
 */
public class AckEndpoint implements Closeable {

  private static final Logger LOG = Logger.getLogger(AckEndpoint.class.getName());
  
  ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  protected AtomicLong ackId = new AtomicLong(0);
  protected NavigableMap<Long, Boolean> acksStates= new ConcurrentSkipListMap<>(); //key is ackId
  Random rand = new Random(System.currentTimeMillis());
  volatile boolean started;
  
  //start periodically flipping ackIds from false to true. This simulated event batches getting indexed.
  //To mimick the observed behavior of splunk, we flip the lowest unacknowledge ackId before
  //any higher ackId
  public synchronized void start(){
    if(started){
      return;
    }
    //stateFrobber will set the ack to TRUE
    Runnable stateFrobber = new Runnable(){
      @Override
      public void run() {
        Long lowestKey = acksStates.ceilingKey(0L); 
        if(null == lowestKey){
          return;
        }
        acksStates.put(lowestKey, true); //flip it
      }      
    };
    //use stateFrobber to flip an ackId from false to true every so ofter
    //ong when = rand.nextInt(30); //outstanding acks get flipped from false to true everu [0.30] ms
    executor.scheduleAtFixedRate(stateFrobber, 0, 1, TimeUnit.MILLISECONDS);
    started = true;
  }
  
  public long nextAckId(){
    long newId = this.ackId.incrementAndGet();
    this.acksStates.put(newId, true); //mock/pretend the events got indexed
    //System.out.println("ackStates: " + this.acksStates);
    return newId;
  }
  
  private Boolean check(long ackId){
    //System.out.println("checking " + ackId);
    return this.acksStates.remove(ackId);    
  }
     
  public void pollAcks(AckManager ackMgr,FutureCallback<HttpResponse> cb){
    try {
      //System.out.println("Server side simulation: " + this.acksStates.size() + " acks tracked on server: " + acksStates);
      Collection<Long> unacked = ackMgr.getAckWindow().getUnacknowleldgedEvents();
      //System.out.println("Server recieved these acks to check: " + unacked);      
      SortedMap<Long, Boolean> acks = new TreeMap<>();
      for(long  ackId:unacked){
        Boolean was = check(ackId);
        if(was != null){
          acks.put(ackId, was);
        }
      }
      Map resp = new HashMap();
      resp.put("acks", acks);
      //System.out.println("these are the ack states returned from the server: "+acks);
      cb.completed(getResult(resp));
    } catch (Exception ex) {
      LOG.severe(ex.getMessage());
      cb.failed(ex);
    }
  }

  private HttpResponse getResult(Map acks) throws JsonProcessingException {
          ObjectMapper serializer = new ObjectMapper();
      String str = serializer.writeValueAsString(acks);
    AckEndpointResponseEntity e = new AckEndpointResponseEntity(str);
    return new AckEndpointResponse(e);
  }

  @Override
  public void close() {
    this.executor.shutdownNow();
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