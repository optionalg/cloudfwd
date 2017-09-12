package com.splunk.cloudfwd.sim;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.http.HecIOManager;
import com.splunk.cloudfwd.http.HttpSender;
import junit.framework.AssertionFailedError;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.junit.Assert;

/**
 * Validates all Connection properties that must wait
 * for channels to be refreshed to go into effect.
 *
 * Note: Waiting CHANNEL_DECOM_MS after setting new
 * properties must be done on the "sender" (client) side.
 *
 * Created by eprokop on 9/11/17.
 */
public class ValidatePropsDelayEndpoint extends SimulatedHECEndpoints {
    // set these in the test itself when changing properties
    public static String TOKEN;
    private static AssertionError fail = null;

    @Override
    public void pollAcks(HecIOManager ackMgr,
                         FutureCallback<HttpResponse> httpCallback) {
        validate(ackMgr.getSender());
        ackEndpoint.pollAcks(ackMgr, httpCallback);
    }

    private void validate(HttpSender sender) {
        try {
            Assert.assertEquals(TOKEN, sender.getToken());
        } catch (AssertionError e) {
            fail = e;
            throw e;
        }
        // TODO: add asserts for more properties
    }

    public static AssertionError getAssertionFailures() {
        return fail;
    }
}
