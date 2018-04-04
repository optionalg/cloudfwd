package com.splunk.cloudfwd.test.util;

import com.splunk.cloudfwd.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.splunk.cloudfwd.impl.util.HecHealthImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;

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
public abstract class AbstractConnectionTest {
  protected static Map<String, String> cliProperties = new HashMap<>();

  private class HecLoggerFactoryImpl implements HecLoggerFactory {
    public Logger getLogger(String name) {
      return LoggerFactory.getLogger(name);
    }
  }

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractConnectionTest.class.getName());

  /**
   * set enabled=false in test.properties to disable test.properties and
   * fallback on cloudfwd.properties
   */
  public static final String TEST_METHOD_GUID_KEY = "testMethodGUID";

  protected BasicCallbacks callbacks;
  protected Connection connection;
  protected SimpleDateFormat dateFormat = new SimpleDateFormat(
          "MMM dd hh:mm:ss a ZZZ");
  protected final static String TEST_CLASS_INSTANCE_GUID = java.util.UUID.
          randomUUID().
          toString();
  protected String testMethodGUID;
  protected List<Event> events;
  private Exception sendException;
  private String sendExceptionMsg;
  


  //override to do stuff like set buffering or anything else affecting connection
  protected void configureConnection(Connection connection) {
    //noop
  }

  protected Event.Type eventType = Event.Type.TEXT; //default to TEXT event content

  @Before
  public void setUp() {
    LOG.info("starting test setup()");
    extractCliTestProperties();
    if(connectionInstantiationShouldFail() && getNumEventsToSend() != 0){
        throw new RuntimeException("connectionInstantiationShouldFail returns true, but getNumEventsToSend not returning 0. "
                + "You should override getNumEventsToSend and return zero.");
    }
    this.callbacks = getCallbacks();
    this.connection = createAndConfigureConnection();
    this.testMethodGUID = java.util.UUID.randomUUID().toString();
    this.events = new ArrayList<>();
  }
  
  protected Connection createAndConfigureConnection(){
    ConnectionSettings settings = getTestProps();
    configureProps(settings);
    connection = createConnection(callbacks, settings);
    if(null == connection){
      return null;
    }
    connection.setLoggerFactory(new HecLoggerFactoryImpl());
    configureConnection(connection);
    return connection;
  }
  
  protected Connection createConnection(ConnectionCallbacks c, ConnectionSettings settings){
      boolean didThrow = false;
      Connection conn = null;
      try{
        conn = Connections.create(callbacks, settings);
      }catch(Exception e){
          e.printStackTrace();
          didThrow = true;
          if(!connectionInstantiationShouldFail()){
            e.printStackTrace();
              Assert.fail("Connection instantiation should not have failed, but it did: " +e);
          }else{
              if(! isExpectedConnInstantiationException(e)){
                   Assert.fail("Connection instantiation failure was expected, but we didn't get the *expected* Exception.  Got: " + e);
              }
          }
      }
      if(!didThrow && connectionInstantiationShouldFail()){
          Assert.fail("expected a Connection instantiation Exception to be caught. None was caught.");
      }
      return conn;
  }
  
    /**
     * Test should override this to test specific exceptions thrown on connection creation
     * @param e
     * @return
     */
    protected boolean isExpectedConnInstantiationException(Exception e) {
       return true;
    }
  
    /**
     * Override in test if your test wants Connection instantiation to fail
     * @return
     */
    protected boolean connectionInstantiationShouldFail() {
        return false;
    }

  /**
   * Test should override to extract the CLI properties it cares about
   */
  protected void extractCliTestProperties() {
      // no op
  }

  @After
  public void tearDown() {
      LOG.info("tearing down test");
    //in case of failure we probably have events stuck on a channel. Therefore a regular close will just
    //hang out waiting (infinitely?) for the messages to flush out before gracefully closing. So when we see
    //a failure we must use the closeNow method which closes the channel regardless of whether it has
    //messages in flight.
    if(null == connection){
        return;
    }
    if (callbacks.isFailed() || callbacks.shouldFail()) {     
          connection.closeNow();      
    }else{
        connection.close();
    }
    LOG.info("teardown complete");
//    this.callbacks = null; //unregister the callback first
//    this.connection = null;
//    System.gc();
  }
  
  /**
   * Subclass should override when custom handling of errors, warnings, and failures is required
   * @return
   */
  protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend());
    }

  protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
      sendEvents(true, false);
  }

  protected void sendEvents(boolean shouldClose, boolean closeNow) throws InterruptedException, HecConnectionTimeoutException {

        int expected = getNumEventsToSend();
        if(expected <= 0){
            return;
        }
      try {
          LOG.trace(
                "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
                        + "And test method GUID " + testMethodGUID);
          for (int i = 0; i < expected; i++) {
          ///final EventBatch events =nextEventBatch(i+1);
              Event event = nextEvent(i + 1);
              LOG.trace("Send event {} i={}", event.getId(), i);

              connection.send(event);
          }          
      } catch(Exception e) {
          this.sendException = e;
          this.sendExceptionMsg = e.getMessage();
          LOG.warn("In Test caught exception on Connection.send(): {} with message {}", e, e.getMessage());
      }
      checkSendExceptions();
        
      if (shouldClose) {
        if (closeNow) {
          connection.closeNow();
        } else {
          connection.close();
        }
      }
      if(null == sendException){      //only await callback-related fails and warns if we did not throw a send exception
        this.callbacks.await(10, TimeUnit.MINUTES);
        this.callbacks.checkFailures();
        this.callbacks.checkWarnings();
      }
  }

  public void checkSendExceptions() {
      if (shouldSendThrowException() && !didSendThrowException()) {
          Assert.fail("Send should have thrown an exception but it didn't");
      }
      if (didSendThrowException() && !isExpectedSendException(sendException)) {
          Assert.fail(
              "There was an unexpected exception thrown on send  " +
                  getSendException() + " and message " + getSendExceptionMsg());
      }
  }

  private String getSendExceptionMsg() {
      return this.sendExceptionMsg;
  }

  private Exception getSendException() {
      return this.sendException;
  }

  private boolean didSendThrowException() {
      return this.sendException != null;
  }

  /**
   * Subclasses can override to return true if expecting an exception from send (to suppress printing of stacktrace).
   * @param e The Exception that was thrown on send
   * @return
   */
  protected boolean isExpectedSendException(Exception e) {
    return false;
  }

  protected boolean shouldSendThrowException() {
      return false;
  }

  protected List<HecHealth> healthCheck() throws InterruptedException {
    LOG.trace(
        "HEC CHECK WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
        + "And test method GUID " + testMethodGUID);
    List<HecHealth> healthStatus = connection.getHealth();
    connection.close(); //will flush    
    return healthStatus;
  }

  protected void sendCombinationEvents() throws TimeoutException, InterruptedException, HecConnectionTimeoutException {
    LOG.trace("SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    int expected = getNumEventsToSend();
    for (int i = 0; i < expected; i++) {
      ///final EventBatch events =nextEventBatch(i+1);
      if (i % 2 == 1) {
        this.eventType = Event.Type.TEXT;
      } else {
        this.eventType = Event.Type.JSON;
      }
      Event event = nextEvent(i + 1);
      LOG.trace("Send event: " + event.getId() + " i=" + i);
      this.connection.send(event); //will immediately send event in batch since buffer defaults to zero
    }
    connection.close(); //will flush
    this.callbacks.await(10, TimeUnit.MINUTES);
    if (callbacks.isFailed()) {
      Assert.fail(callbacks.getFailMsg());
    }
  }

  /**
   * test should override this to add properties on top of
   * cloudfwd.properties+test.properties
   *
   * @return
   */
  protected void configureProps(ConnectionSettings settings) {
    //default behavior is no "hard coded" test-specific properties
  }

  /**
   * reads default test properties out of test_defaults.properties (these
   * overlay on top of cloudfwd.properties)
   *
   * @return
   */
  protected ConnectionSettings getTestProps() {
      ConnectionSettings testSettings = ConnectionSettings.fromPropsFile(getTestPropertiesFileName());
    if (testSettings.getTestPropertiesEnabled()) {
      return testSettings;
    } else {
      LOG.warn("test.properties disabled, using cloudfwd.properties only");
      return ConnectionSettings.fromPropsFile(getCloudfwdPropertiesFileName()); //ignore test.properties
    }
  }

  private String getCloudfwdPropertiesFileName() { return "/cloudfwd.properties"; }

  /**
   * test can override this if a test requires its own .properties file to slap
   * on top of cloudfwd.properties (instead of slapping test.properties on top of
   * cloudfwd.properties)
   *
   * @return
   */
  protected String getTestPropertiesFileName() {
    return "/test.properties";
  }

  /**
   * Default implementation will return the next event to send
   *
   * @param seqno
   * @return
   */
  protected Event nextEvent(int seqno) {
     if(getNumEventsToSend() <= 100){
         LOG.info("sending id={}", seqno); //if less than 100 events in test, print them all
     } else if(getNumEventsToSend() <= 1000 && seqno%100==0){ //less than 1000, print every 100
         LOG.info("sending id={}", seqno);
     }else if (seqno%1000==0){ //print every 1000th event
         LOG.info("sending id={}", seqno);  
     }
    Event event = null;
    switch (this.eventType) {
      case TEXT: {
        event = getTextEvent(seqno);
        break;
      }
      case JSON: {
        event = getJsonEvent(seqno);
        break;
      }
      case UNKNOWN: {
        event = getUnvalidatedBytesEvent(seqno);
        break;
      }
      default: {
        throw new RuntimeException("unsupported type");
      }
    }
    if (shouldCacheEvents()) {
      events.add(event);
    }
    return event;
  }

  private Event getUnvalidatedBytesEvent(int seqno) {
    Event event;
    if (connection.getSettings().getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
      event = getUnvalidatedBytesToRawEndpoint(seqno);
    } else {
      event = getUnvalidatedBytesToEventEndpoint(seqno);
    }
    return event;
  }

  protected Event getJsonEvent(int seqno) {
    Event event;
    if (connection.getSettings().getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
      event = getJsonToRawEndpoint(seqno);
    } else {
      event = getJsonToEvents(seqno);
    }
    return event;
  }

  protected Event getTextEvent(int seqno) {
    Event event;
    if (connection.getSettings().getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
      event = getTimestampedRawEvent(seqno);
    } else {
      event = getTextToEvents(seqno);
    }
    return event;
  }

  protected Map getStructuredEvent() {
    Map map = new LinkedHashMap();
    map.put("foo", "bar");
    map.put("baz", "yeah I am json field");
    map.put("trace", getEventTracingInfo());
    map.put(TEST_METHOD_GUID_KEY, testMethodGUID);
    return map;
  }

  protected abstract int getNumEventsToSend();

  protected RawEvent getTimestampedRawEvent(int seqno) {
    return RawEvent.
            fromText(dateFormat.format(new Date()) + " TEXT FOR /raw ENDPOINT with " + getEventTracingInfo() + " seqno=" + seqno,
                    seqno);
  }

  protected String getEventTracingInfo() {
    return "GUID=" + TEST_CLASS_INSTANCE_GUID + " " + TEST_METHOD_GUID_KEY + "=" + testMethodGUID;
  }

  // override if you need to access the events you send
  protected boolean shouldCacheEvents() {
    return false;
  }

  protected List<Event> getSentEvents() {
    if (!shouldCacheEvents()) {
      throw new RuntimeException(
              "Events were not cached. Override shouldCacheEvents() to store sent events.");
    }
    return events;
  }

  protected Event getJsonToRawEndpoint(int seqno) {
    try {
      Map m = getStructuredEvent();
      m.put("where_to", "/raw");
      m.put("seqno", Integer.toString(seqno));
      Event event = RawEvent.fromObject(m, seqno);
      return event;
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  protected Event getJsonToEvents(int seqno) {
    Map m = getStructuredEvent();
    m.put("where_to", "/events");
    m.put("seqno", Integer.toString(seqno));
    Event event = new EventWithMetadata(m, seqno);
    return event;
  }

  protected Event getTextToEvents(int seqno) {
    Event event = new EventWithMetadata(
            "TEXT FOR /events endpoint 'event' field with "
            + getEventTracingInfo() + " seqno=" + seqno,
            seqno);
    return event;
  }

  protected Event getUnvalidatedBytesToRawEndpoint(int seqno) {
    //create a valid JSON to /events, grab its bytes, and wrap it in UnvalidatedBytes to simulate
    //the creation of /event endpoint envelope "by hand"
//    return new UnvalidatedBytesEvent(getJsonToRawEndpoint(seqno).getBytes(),
//            seqno);
    return new UnvalidatedByteBufferEvent(ByteBuffer.wrap(getJsonToRawEndpoint(seqno).getBytes()), seqno);
  }

  private Event getUnvalidatedBytesToEventEndpoint(int seqno) {
    //create a valid JSON to /events, grab its bytes, and wrap it in UnvalidatedBytes to simulate
    //the creation of /event endpoint envelope "by hand"
   // return new UnvalidatedBytesEvent(getJsonToEvents(seqno).getBytes(), seqno);
    return new UnvalidatedByteBufferEvent(ByteBuffer.wrap(getJsonToEvents(seqno).getBytes()), seqno);
  }
  
  /**
   * assert that health is not empty and all channels failed with 
   * provided exceptionClass and exceptionMessage.
   *
   * @param exceptionClass
   * @param exceptionMessage
   */
  public void assertAllChannelsFailed(Class exceptionClass, String exceptionMessage) {
    List<HecHealth> healths = connection.getHealth();
    Assert.assertTrue("Expected health checks to be not empty, but got this healths: \"" + healths + "\"", !healths.isEmpty());
    // we expect all channels to fail catching SSLPeerUnverifiedException in preflight 
    healths.stream().forEach(e ->LOG.debug("Got exception in healths: " + e.getStatus().getException().getMessage()));
    if (healths.stream()
            .map(h -> h.getStatus().getException())
            .filter(e -> exceptionClass.isInstance(e))
            .filter(e -> e.getMessage().equals(exceptionMessage))
            .count() != healths.size()) {
      Assert.fail("Expected all health channels to fail with ex: \"" + exceptionClass +
              "\" and message: \"" + exceptionMessage +
              "\", but got instead the following exceptions in healths: " +
              Arrays.toString(healths.stream().map(h -> h.getStatus().getException()).toArray()));
    }
  }
  
  public List<String> getChannelId(Connection connection) {
    ArrayList channels = new ArrayList();
    for (Object c : connection.getHealth()) {
      channels.add(((HecHealthImpl) c).getChannelId());
    }
    LOG.info("List of channel ids {}", channels);
    return channels;
  }
  
}
