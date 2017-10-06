package com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure;

import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.BadRequestStatusLine;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.errorgen.HecErrorResponse;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;


/**
 *
 * Created by eprokop on 9/1/17.
 */
public class AckDisabledEndpoints extends SimulatedHECEndpoints {
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
    public void ackEndpointCheck(FutureCallback<HttpResponse> httpCallback) {
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
