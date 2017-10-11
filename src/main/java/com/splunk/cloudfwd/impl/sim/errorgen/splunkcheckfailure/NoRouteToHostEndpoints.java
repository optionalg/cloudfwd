package com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import com.splunk.cloudfwd.impl.sim.errorgen.PreFlightAckEndpoint;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

import java.net.NoRouteToHostException;

/**
 * Created by eprokop on 10/9/17.
 */
public class NoRouteToHostEndpoints extends SimulatedHECEndpoints {
    @Override
    protected PreFlightAckEndpoint createPreFlightAckEndpoint() { return new NoRouteToHostPreFlightEndpoint(); }

    @Override
    public void postEvents(HttpPostable events,
                           FutureCallback<HttpResponse> httpCallback) {
        throw new IllegalStateException("We should fail before trying to post events.");
    }

    @Override
    public void pollAcks(HecIOManager ackMgr,
                         FutureCallback<HttpResponse> httpCallback) {
        throw new IllegalStateException("We should fail before trying to poll for acks.");
    }

    @Override
    public void pollHealth(FutureCallback<HttpResponse> httpCallback) {
        throw new IllegalStateException("We should fail before trying to poll for health.");
    }

    private class NoRouteToHostPreFlightEndpoint extends PreFlightAckEndpoint {
        @Override
        public void ackEndpointCheck(FutureCallback<HttpResponse> cb) {
            Runnable respond = () -> {
                // simulates behavior of apache async http client if a route to host cannot be found.
                cb.failed(new NoRouteToHostException("No route to host"));
            };
            delayResponse(respond);
        }
    }
}
