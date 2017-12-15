package com.splunk.cloudfwd.impl.sim.errorgen.discoverer;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.concurrent.FutureCallback;


/**
 * Scenario: Connection instantiated with an invalid host URL, so 
 * http client raises ClientProtocolException on any request attempt
 * 
 * Created by ssergeev on 10/21/17.
 */
public class InvalidHostName extends SimulatedHECEndpoints {
    @Override
    public void postEvents(HttpPostable events, FutureCallback<HttpResponse> cb) {
        throw new IllegalStateException("We should fail before trying to post events.");
    }
    
    @Override
    public void pollAcks(HecIOManager ackMgr, FutureCallback<HttpResponse> cb) {
        (cb).failed(new ClientProtocolException(
                "URI does not specify a valid host name: foobarunknownhostbaz:8088/services/collector/ack"));
    }
    
    @Override
    public void checkHealthEndpoint(FutureCallback<HttpResponse> cb) {
        (cb).failed(new ClientProtocolException(
                "URI does not specify a valid host name: foobarunknownhostbaz:8088/services/collector/ack"));
    }
    
    @Override
    public void checkAckEndpoint(FutureCallback<HttpResponse> cb) {
        ((HttpCallbacksAbstract) cb).failed(new ClientProtocolException(
                "URI does not specify a valid host name: foobarunknownhostbaz:8088/services/collector/ack"));
    }
    
}
