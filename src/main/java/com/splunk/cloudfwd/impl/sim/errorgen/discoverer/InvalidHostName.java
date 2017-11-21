package com.splunk.cloudfwd.impl.sim.errorgen.discoverer;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import com.splunk.cloudfwd.impl.sim.UnauthorizedStatusLine;
import com.splunk.cloudfwd.impl.sim.errorgen.HecErrorResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.concurrent.FutureCallback;


/**
 *
 * Created by ssergeev on 10/21/17.
 */
public class InvalidHostName extends SimulatedHECEndpoints {
    @Override
    public void postEvents(HttpPostable events,
                           FutureCallback<HttpResponse> httpCallback) {
        throw new IllegalStateException("We should fail before trying to post events.");
    }
    
    @Override
    public void pollAcks(HecIOManager ackMgr, FutureCallback<HttpResponse> cb) {
        System.out.println("/ack rest endpoint fails because invalid host exception");
        ((HttpCallbacksAbstract) cb).failed(new ClientProtocolException(
                "URI does not specify a valid host name: foobarunknownhostbaz:8088/services/collector/ack"));
    }
    
    @Override
    public void checkHealthEndpoint(FutureCallback<HttpResponse> cb) {
        System.out.println("/health rest endpoint fails because invalid host exception");
        ((HttpCallbacksAbstract) cb).failed(new ClientProtocolException(
                "URI does not specify a valid host name: foobarunknownhostbaz:8088/services/collector/ack"));
    }
    
    @Override
    public void checkAckEndpoint(FutureCallback<HttpResponse> cb) {
        System.out.println("/ack rest endpoint fails because invalid host exception");
        ((HttpCallbacksAbstract) cb).failed(new ClientProtocolException(
                "URI does not specify a valid host name: foobarunknownhostbaz:8088/services/collector/ack"));
    }
    
}
