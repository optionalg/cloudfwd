//package com.splunk.cloudfwd.impl.sim;
//        
//        import com.splunk.cloudfwd.impl.http.HecIOManager;
//        import com.splunk.cloudfwd.impl.http.HttpPostable;
//        import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
//        import com.splunk.cloudfwd.impl.sim.*;
//        import com.splunk.cloudfwd.impl.http.HttpPostable;
//        import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
//        
//        import com.splunk.cloudfwd.impl.sim.AckEndpoint;
//        import com.splunk.cloudfwd.impl.sim.CannedEntity;
//        import com.splunk.cloudfwd.impl.sim.CookiedOKHttpResponse;
//        import com.splunk.cloudfwd.impl.sim.EventEndpoint;
//        import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
//        import com.splunk.cloudfwd.impl.sim.errorgen.PreFlightAckEndpoint;
//        import org.apache.http.HttpResponse;
//        import org.apache.http.concurrent.FutureCallback;
//        import org.slf4j.Logger;
//        import org.slf4j.LoggerFactory;
//        
//        import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksEventPost;
//
//public class SyncAckedEndpoint extends SimulatedHECEndpoints {
//  private static final Logger LOG = LoggerFactory.getLogger(com.splunk.cloudfwd.impl.sim.SyncAckedEndpoint.class.getName());
//  
////  private static final String header = HttpCallbacksEventPost.SPLUNK_ACK_HEADER_NAME;
//  
//  @Override
//  public void checkAckEndpoint(FutureCallback<HttpResponse> httpCallback) {
//    httpCallback.completed(
//            new CookiedOKHttpResponse(
//                    new CannedEntity("{\\\"acks\\\":[0:false]}")’
//  }
//  
//  @Override
//  public void checkHealthEndpoint(FutureCallback<HttpResponse> httpCallback) {
//    LOG.debug("Health check with cookie: " + currentCookie);
//    httpCallback.completed(
//            new CookiedOKHttpResponse(
//                    new CannedEntity("Healthy with cookies"),
//                    currentCookie));
//  }
//  
//  @Override
//  protected PreFlightAckEndpoint createPreFlightAckEndpoint() {
//    return new com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints.CookiedPreFlightEnpoint();
//  }
//  
//  @Override
//  protected EventEndpoint createEventEndpoint() {
//    return new com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints.CookiedEventpoint(ackEndpoint);
//  }
//  
//  @Override
//  protected AckEndpoint createAckEndpoint() {
//    return new com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints.CookiedAckEndpoint();
//  }
//  
//  class CookiedPreFlightEnpoint extends PreFlightAckEndpoint {
//    @Override
//    public void checkAckEndpoint(FutureCallback<HttpResponse> httpCallback) {
//      Runnable respond = () -> {
//        httpCallback.completed(
//                new CookiedOKHttpResponse(
//                        new CannedEntity("{\\\"acks\\\":[0:false]}"),
//                        currentCookie));
//      };
//      delayResponse(respond);
//    }
//  }
//  
//  class CookiedEventpoint extends EventEndpoint {
//    public CookiedEventpoint(AcknowledgementEndpoint ackEndpoint) {
//      this.ackEndpoint = ackEndpoint;
//      //ackEndpoint.start();
//    }
//    
//    @Override
//    public void post(HttpPostable events, FutureCallback<HttpResponse> cb) {
//      Runnable respond = () -> {
//        LOG.debug("Event post response with cookie: " + currentCookie);
//        ((HttpCallbacksAbstract) cb).completed(
//                new CookiedOKHttpResponse(
//                        new CannedEntity("{\"ackId\":" + nextAckId() + "}"),
//                        currentCookie));
//      };
//      delayResponse(respond);
//    }
//    
//    protected long nextAckId() {
//      return ackEndpoint.nextAckId();
//    }
//  }
//  
//  class CookiedAckEndpoint extends AckEndpoint {
//    
//    @Override
//    public synchronized long nextAckId() {
//      long newId = this.ackId.incrementAndGet();
//      this.acksStates.put(newId, true);
//      return newId;
//    }
//    
//    @Override
//    protected HttpResponse getHttpResponse(String entity) {
//      LOG.info("Ack response with cookie: " + currentCookie);
//      CannedEntity e = new CannedEntity(entity);
//      return new CookiedOKHttpResponse(e, currentCookie);
//    }
//  }
//  
//  
//}
//
