package com.splunk.cloudfwd.sim;

import com.splunk.cloudfwd.http.AckManager;
import com.splunk.cloudfwd.http.ElbCookie;
import com.splunk.cloudfwd.http.Endpoints;
import com.splunk.cloudfwd.http.EventBatch;
import org.apache.http.*;
import org.apache.http.concurrent.FutureCallback;
import sun.plugin.dom.exception.InvalidStateException;

import java.util.*;

/**
 * Created by eprokop on 8/14/17.
 */
public class StickyEndpoints implements Endpoints {
    final private int NUMBER_INDEXERS = 3;
    private Map<String, SimulatedHECEndpoints> indexers = new HashMap<>();

    public StickyEndpoints() {
        for (int i = 0; i < NUMBER_INDEXERS; i++) {
            indexers.put(getRandomCookieValue(), new SimulatedHECEndpoints());
        }
    }

    @Override
    public void postEvents(EventBatch events, HttpRequest request, FutureCallback<HttpResponse> httpCallback) {
        String stickyCookie = extractCookie(request);
        if (stickyCookie != null) {
            // Request had a cookie, so route to the correct indexer
            SimulatedHECEndpoints indexer = indexers.get(stickyCookie);
            indexer.postEvents(events, request, httpCallback);
        } else {
            // Route request to a random indexer
            String setCookieValue = getRandomIndexerCookie();
            SimulatedHECEndpoints indexer = indexers.get(setCookieValue);
            indexer.postEventsSticky(events, httpCallback, setCookieValue);
        }
    }

    @Override
    public void pollAcks(AckManager ackMgr, HttpRequest request,
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

    private String getRandomCookieValue() {
        return ElbCookie.COOKIE_NAME + "=" + UUID.randomUUID().toString();
    }

    private String extractCookie(HttpRequest request) {
        HeaderIterator headerIterator = request.headerIterator("Cookie");
        while (headerIterator.hasNext()) {
            Header header = (Header)headerIterator.next();
            HeaderElement[] elements = header.getElements();
            for (HeaderElement element : elements) {
                if (element.getParameterByName(ElbCookie.COOKIE_NAME) != null) {
                    return ElbCookie.COOKIE_NAME + "=" + element.getValue();
                }
            }
        }
        return null;
    }

    private String getRandomIndexerCookie() {
        Random generator = new Random();
        List<String> keys = new ArrayList<>(indexers.keySet());
        return keys.get(generator.nextInt(keys.size()));
    }
}
