package com.splunk.cloudfwd.sim;

import com.splunk.cloudfwd.http.ElbCookie;
import com.splunk.cloudfwd.http.Endpoints;
import com.splunk.cloudfwd.http.EventBatch;
import com.splunk.cloudfwd.http.HecIOManager;
import org.apache.http.*;
import org.apache.http.concurrent.FutureCallback;
import sun.plugin.dom.exception.InvalidStateException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by eprokop on 8/14/17.
 */
public class StickyEndpoints implements Endpoints {
    final private int NUMBER_INDEXERS = 3;
    private Map<String, SimulatedHECEndpoints> indexers = new LinkedHashMap<>(); // Cookie value (i.e. "AWSELB=simulatedCookieValue0") -> set of /event, /ack, and /health endpoints
    private String cookieInRequest = null;
    private AtomicInteger cookieCounter = new AtomicInteger(0);

    // routing modes
    final public static String ROUND_ROBIN = "round-robin";
    final public static String RANDOM = "random";
    private String routingMode = RANDOM; // default to routing to a random indexer
    private int robin = 0;

    public StickyEndpoints() {
        for (int i = 0; i < NUMBER_INDEXERS; i++) {
            indexers.put(generateSimulatedCookie(), new SimulatedHECEndpoints());
        }
    }

    @Override
    public void postEvents(EventBatch events, HttpRequest request, FutureCallback<HttpResponse> httpCallback) {
        cookieInRequest = extractCookie(request);
        if (cookieInRequest != null) {
            // Request had a cookie, so route to the correct indexer
            SimulatedHECEndpoints indexer = indexers.get(cookieInRequest);
            indexer.postEvents(events, request, httpCallback);
        } else {
            // Route request to a random indexer
            String setCookieValue = getNextSimulatedCookieValue();
            SimulatedHECEndpoints indexer = indexers.get(setCookieValue);
            indexer.postEventsSticky(events, httpCallback, setCookieValue);
        }
    }

    @Override
    public void pollAcks(HecIOManager ackMgr, HttpRequest request,
                         FutureCallback<HttpResponse> httpCallback) {
        String stickyCookie = extractCookie(request);
        if (stickyCookie != null) {
            // TODO: what if the cookie doesn't correspond to an indexer?
            SimulatedHECEndpoints indexer = indexers.get(stickyCookie);
            indexer.pollAcks(ackMgr, request, httpCallback);
        } else {
            throw new InvalidStateException("Should never receive an ack poll request without a cookie.");
        }
    }

    @Override
    public void pollHealth(final HttpRequest request, FutureCallback<HttpResponse> httpCallback) {
        String stickyCookie = extractCookie(request);
        if (stickyCookie != null) {
            // TODO: what if the cookie doesn't correspond to an indexer?
            SimulatedHECEndpoints indexer = indexers.get(stickyCookie);
            indexer.pollHealth(request, httpCallback);
        } else {
            throw new InvalidStateException("Should never receive an ack health poll request without a cookie.");
        }
    }

    @Override
    public void close() {
        for (SimulatedHECEndpoints indexer : indexers.values()) {
            if(null != indexer.ackEndpoint){
                indexer.ackEndpoint.close();
            }
            if(null != indexer.eventEndpoint){
                indexer.eventEndpoint.close();
            }
            if(null != indexer.healthEndpoint){
                indexer.healthEndpoint.close();
            }
        }

    }

    @Override
    public void start() {
        for (SimulatedHECEndpoints endpoints : indexers.values()) {
            endpoints.start();
        }
    }

    private String generateSimulatedCookie() {
        return ElbCookie.COOKIE_NAME + "=" + "simulatedCookieValue" + Integer.toString(cookieCounter.getAndIncrement());
    }

    private String extractCookie(HttpRequest request) {
        HeaderIterator headerIterator = request.headerIterator("Cookie");
        while (headerIterator.hasNext()) {
            Header header = (Header)headerIterator.next();
            HeaderElement[] elements = header.getElements();
            for (HeaderElement element : elements) {
                if (element.getName().equals(ElbCookie.COOKIE_NAME)) {
                    return element.getName() + "=" + element.getValue();
                }
            }
        }
        return null;
    }

    private String getNextSimulatedCookieValue() {
        switch (routingMode) {
            case (RANDOM):
                return getRandomSimulatedCookie();
            case (ROUND_ROBIN):
                return getRoundRobinSimulatedCookie();
            default:
                throw new InvalidStateException("Routing mode not supported");
        }
    }

    private String getRandomSimulatedCookie() {
        Random generator = new Random();
        List<String> keys = new ArrayList<>(indexers.keySet());
        return keys.get(generator.nextInt(keys.size()));
    }

    private String getRoundRobinSimulatedCookie() {
        List<String> keys = new ArrayList<>(indexers.keySet());
        int index = robin++ % keys.size();
        return keys.get(index);
    }

    public String getCookieInRequest() {
        return cookieInRequest;
    }

    public void setRoutingMode(String mode) {
        routingMode = mode;
    }
}
