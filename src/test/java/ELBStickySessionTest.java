import org.apache.http.concurrent.FutureCallback;
import com.splunk.cloudfwd.http.*;
import com.splunk.cloudfwd.sim.StickyEndpoints;
import org.apache.http.HttpResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 8/14/17.
 */
public class ELBStickySessionTest {
    private HttpEventCollectorSender sender;
    private StickyEndpoints simulatedStickyEndpoints;

    @Before
    public void setUp() {
        sender = new HttpEventCollectorSender("dummyUrl", "dummyToken");
        simulatedStickyEndpoints = new StickyEndpoints();
        sender.setSimulatedEndpoints(simulatedStickyEndpoints);
    }

    @After
    public void tearDown() {
        // TODO do whatever is relevant from Connection.close()
    }

    private EventBatch nextEventBatch() {
        EventBatch events = new EventBatch(EventBatch.Endpoint.raw,
                EventBatch.Eventtype.json);
        events.add(new HttpEventCollectorEvent("info", "foo", "HEC_LOGGER",
                Thread.currentThread().getName(), new HashMap(), null, null));
        return events;
    }

    /**
     * Tests that an "AWSELB" cookie is properly set on the HttpEventCollectorSender
     * object when receiving a response with a "Set-Cookie" header.
     */
    @Test
    public void testCookieSetOnSender() {
        CountDownLatch latch = new CountDownLatch(1);
        EventBatch events = nextEventBatch();

        // Values to test. These are captured in the callback
        // and asserted AFTER the we pass through CountDownLatch
        // to prevent the test from blocking in the case where
        // an assert fails. Marked "final" because they are assigned in inner class.
        final String[] cookieValues = new String[2];
        final ElbCookie[] cookieObj = new ElbCookie[1];
        final String[] cookieValueName = new String[1];

        // callback
        FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {
            @Override
            protected void completed(String reply, int code, ElbCookie cookie) {
                if (code == 200) {
                    cookieValues[0] = cookie.getValue();
                    sender.getHecIOManager().consumeEventPostResponse(reply, events, cookie);
                    cookieObj[0] = sender.getCookie();
                    cookieValues[1] = sender.getCookie().getValue();
                    cookieValueName[0] = sender.getCookie().getNameValuePair().split("=")[0];
                } else {
                    Assert.fail("Simulated endpoint broken: didn't receive a 200");
                }
                latch.countDown();
            }

            @Override
            public void failed(Exception e) {
                Assert.fail("Simulated endpoint called failed callback");
            }

            @Override
            public void cancelled() {
                Assert.fail("Simulated endpoint called cancelled callback");
            }
        };

        // send events
        sender.getHecIOManager().getAckTracker().preEventPost(events);
        sender.postEvents(events, null, cb);

        // asserts
        try {
            boolean success = latch.await(1, TimeUnit.MINUTES);
            if (!success) Assert.fail("CountDownLatch timed out");
            Assert.assertNotNull("Cookie value passed to consumeEventPostResponse should not be null",
                    cookieValues[0]);
            Assert.assertNotNull("Cookie in HttpEventCollectorSender should not be null", cookieObj[0]);
            Assert.assertNotNull("Cookie value in HttpEventCollectorSender should not be null", cookieValues[1]);
            Assert.assertEquals("Cookie name should be 'AWSELB'", "AWSELB", cookieValueName[0]);
        } catch (InterruptedException e) {
            Assert.fail("CountDownLatch was interrupted.");
        }
    }

    /**
     * Tests that a newly instantiated HttpEventCollectorSender does not
     * send a cookie in the first batch POST and does send an "AWSELB" cookie in
     * subsequent batch POSTs.
     */
    @Test
    public void testCookieSent() {
        StickyEndpoints stickyEndpoints = (StickyEndpoints)sender.getSimulatedEndpoints();
        stickyEndpoints.setRoutingMode(StickyEndpoints.ROUND_ROBIN); // so cookie is predictable
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        EventBatch events1 = nextEventBatch();
        EventBatch events2 = nextEventBatch();

        // values to test
        final String[] cookieInRequest = new String[2];

        // callbacks
        FutureCallback<HttpResponse> cb1 = new AbstractHttpCallback() {
            @Override
            protected void completed(String reply, int code, ElbCookie cookie) {
                if (code == 200) {
                    System.out.println("testCookieSent: first callback received");
                    sender.getHecIOManager().consumeEventPostResponse(reply, events1, cookie);
                    cookieInRequest[0] = stickyEndpoints.getCookieInRequest();
                    if (cookieInRequest[0] == null) System.out.println("good!");
                } else {
                    Assert.fail("Simulated endpoint broken: didn't receive a 200");
                }
                latch1.countDown();
            }
            @Override
            public void failed(Exception e) {
                Assert.fail("Simulated endpoint called failed callback");
            }
            @Override
            public void cancelled() {
                Assert.fail("Simulated endpoint called cancelled callback");
            }
        };

        FutureCallback<HttpResponse> cb2 = new AbstractHttpCallback() {
            @Override
            protected void completed(String reply, int code, ElbCookie cookie) {
                if (code == 200) {
                    System.out.println("testCookieSent: second callback received");
                    sender.getHecIOManager().consumeEventPostResponse(reply, events2, cookie);
                    cookieInRequest[1] = stickyEndpoints.getCookieInRequest();
                    System.out.println(cookieInRequest[1]);
                } else {
                    Assert.fail("Simulated endpoint broken: didn't receive a 200");
                }
                latch2.countDown();
            }
            @Override
            public void failed(Exception e) {
                Assert.fail("Simulated endpoint called failed callback");
            }
            @Override
            public void cancelled() {
                Assert.fail("Simulated endpoint called cancelled callback");
            }
        };

        // first batch
        sender.getHecIOManager().getAckTracker().preEventPost(events1);
        sender.postEvents(events1, null, cb1);
        try {
            latch1.await();
        } catch (InterruptedException e) {
            Assert.fail("CountDownLatch was interrupted.");
        }

        // second batch
        sender.getHecIOManager().getAckTracker().preEventPost(events2);
        sender.postEvents(events2, null, cb2);
        try {
            latch2.await();
        } catch (InterruptedException e) {
            Assert.fail("CountDownLatch was interrupted.");
        }

        // asserts
        Assert.assertNull("The first request from a newly created sender should not include a cookie", cookieInRequest[0]);
        Assert.assertEquals("Request did not include the expected cookie value","AWSELB=simulatedCookieValue0", cookieInRequest[1]);
    }

    private void runTests() {
        Result result = JUnitCore.runClasses(getClass());

        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
    }

    public static void main(String[] args) {
        new ELBStickySessionTest().runTests();
    }

    // Test ideas TODO:
    // 1. Send 5 eventBatches. First event does not include a cookie. Make sure the rest of the events have Cookie header and cookie is set in the sender
    // 2. Test different ways of sending/setting cookie (e.g. Set-Cookie header with multiple cookies, Cookie: header with multiple cookies)
    // 3. test /ack and /health

}
