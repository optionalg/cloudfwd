package com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.UnauthorizedStatusLine;
import com.splunk.cloudfwd.impl.sim.errorgen.HecErrorResponse;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;


/**
 *
 * Created by meemax on 9/1/17.
 */
public class InvalidAuthEndpoints extends SimulatedHECEndpoints {
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
    public void splunkCheck(FutureCallback<HttpResponse> httpCallback) {
        httpCallback.completed(new HecErrorResponse(
                new InvalidAuthEntity(), new UnauthorizedStatusLine()
        ));
    }

    private static class InvalidAuthEntity extends CannedEntity {

        public InvalidAuthEntity() {
            super("{\"text\":\"Invalid authorization\",\"code\":3}");
        }
    }
}
