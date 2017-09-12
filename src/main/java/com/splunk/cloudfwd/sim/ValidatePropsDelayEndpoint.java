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

    @Override
    public void pollAcks(HecIOManager ackMgr,
                         FutureCallback<HttpResponse> httpCallback) {
        // TODO: figure out a less shitty way to do this
        if (validate(ackMgr.getSender())) {
            ackEndpoint.pollAcks(ackMgr, httpCallback);
        } else {
            httpCallback.failed(new RuntimeException("Tokens do not match."));
        }
    }

    private boolean validate(HttpSender sender) {
//        Assert.assertEquals(TOKEN, sender.getToken());
        return TOKEN.equals(sender.getToken());
        // TODO: add asserts for more properties
    }
}
