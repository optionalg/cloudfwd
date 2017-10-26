package com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure;

import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.CannedOKHttpResponse;
import com.splunk.cloudfwd.impl.sim.Forbidden403StatusLine;
import com.splunk.cloudfwd.impl.sim.errorgen.HecErrorResponse;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import com.splunk.cloudfwd.impl.sim.PreFlightAckEndpoint;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;


/**
 *
 * Created by eprokop on 9/1/17.
 */
public class InvalidTokenEndpoints extends SimulatedHECEndpoints {
    @Override
    protected PreFlightAckEndpoint createPreFlightAckEndpoint() { return new InvalidTokenPreFlightEndpoint(); }

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
    public void checkHealthEndpoint(FutureCallback<HttpResponse> httpCallback) {
        throw new IllegalStateException("We should fail before trying to poll for health.");
    }

    private static class InvalidTokenEntity extends CannedEntity {

        public InvalidTokenEntity() {
            super("{\"text\":\"Invalid token\",\"code\":4}");
        }
    }

    private class InvalidTokenPreFlightEndpoint extends PreFlightAckEndpoint {
        @Override
        public void checkAckEndpoint(FutureCallback<HttpResponse> cb) {
            Runnable respond = () -> {
                cb.completed(new HecErrorResponse(
                        new InvalidTokenEntity(), new Forbidden403StatusLine()
                ));
            };
            delayResponse(respond);
        }
    }
}
