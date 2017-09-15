package com.splunk.cloudfwd.impl.sim.errorgen.preflightfailure;

import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.ForbiddenStatusLine;
import com.splunk.cloudfwd.impl.sim.errorgen.HecErrorResponse;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;


/**
 *
 * Created by eprokop on 9/1/17.
 */
public class InvalidTokenEndpoints extends SimulatedHECEndpoints {
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

    @Override
    public void preFlightCheck(FutureCallback<HttpResponse> httpCallback) {
        httpCallback.completed(new HecErrorResponse(
                new InvalidTokenEntity(), new ForbiddenStatusLine()
        ));
    }

    private static class InvalidTokenEntity extends CannedEntity {

        public InvalidTokenEntity() {
            super("{\"text\":\"Invalid token\",\"code\":4}");
        }
    }
}
