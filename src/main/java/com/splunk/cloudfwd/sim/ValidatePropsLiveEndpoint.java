package com.splunk.cloudfwd.sim;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.IEventBatch;
import com.splunk.cloudfwd.http.HecIOManager;
import com.splunk.cloudfwd.http.HttpPostable;
import com.splunk.cloudfwd.http.HttpSender;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.junit.Assert;

import java.net.URL;
import java.util.List;

/**
 * Validates Connection properties that are "live" and
 * take effect without delay.
 *
 * Created by eprokop on 9/11/17.
 */
public class ValidatePropsLiveEndpoint extends SimulatedHECEndpoints {

    public static List<URL> URLS; // set with PropertiesFileHelper.getUrls()
    public static long ACK_TIMEOUT_MS;

    @Override
    public void pollAcks(HecIOManager ackMgr,
    FutureCallback<HttpResponse> httpCallback) {
        // TODO: figure out a less shitty way to do this
        if (validate(ackMgr.getSender())) {
            ackEndpoint.pollAcks(ackMgr, httpCallback);
        } else {
            httpCallback.failed(new RuntimeException("URLs or ack timeout didn't match."));
        }
    }

    private boolean validate(HttpSender sender) {
        boolean match = false;
        for (URL url : URLS) {
            if (url.toString().equals(sender.getBaseUrl())) {
                match = true;
            }
        }

//        Assert.assertTrue("Sender url: " + sender.getBaseUrl()
//                + ", must match a url in url list: " + URLS.toString(), match);
//        Assert.assertEquals("Ack timeouts do not match.",
//                sender.getConnection().getPropertiesFileHelper().getAckTimeoutMS(), ACK_TIMEOUT_MS);
        return match &&
                sender.getConnection().getPropertiesFileHelper().getAckTimeoutMS() == ACK_TIMEOUT_MS;
    }
}
