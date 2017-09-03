package com.splunk.cloudfwd.sim.errorgen.preflightfailure;

import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.http.HecIOManager;
import com.splunk.cloudfwd.sim.BadRequestStatusLine;
import com.splunk.cloudfwd.sim.CannedEntity;
import com.splunk.cloudfwd.sim.HecErrorResponse;
import com.splunk.cloudfwd.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import sun.plugin.dom.exception.InvalidStateException;


/**
 *
 * Created by eprokop on 9/1/17.
 */
public class AckDisabledEndpoints extends SimulatedHECEndpoints {
    @Override
    public void postEvents(EventBatch events,
                           FutureCallback<HttpResponse> httpCallback) {
        throw new InvalidStateException("We should fail before trying to post events.");
    }

    @Override
    public void pollAcks(HecIOManager ackMgr,
                         FutureCallback<HttpResponse> httpCallback) {
        throw new InvalidStateException("We should fail before trying to poll for acks.");
    }

    @Override
    public void pollHealth(FutureCallback<HttpResponse> httpCallback) {
        throw new InvalidStateException("We should fail before trying to poll for health.");
    }

    @Override
    public void preFlightCheck(FutureCallback<HttpResponse> httpCallback) {
        httpCallback.completed(new HecErrorResponse(
                new AckDisabledEntity(), new BadRequestStatusLine()
        ));
    }

    private static class AckDisabledEntity extends CannedEntity {

        public AckDisabledEntity() {
            super("{\"text\":\"ACK is disabled\",\"code\":14}");
        }
    }
}
