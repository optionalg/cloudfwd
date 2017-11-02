package com.splunk.cloudfwd.impl.sim.errorgen;

import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.CannedOKHttpResponse;
import com.splunk.cloudfwd.impl.sim.Endpoint;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 9/27/17.
 */
public class PreFlightAckEndpoint implements Endpoint {
    private static final Logger LOG = LoggerFactory.getLogger(PreFlightAckEndpoint.class.getName());

    final ScheduledExecutorService executor;
    Random rand = new Random(System.currentTimeMillis());

    public PreFlightAckEndpoint() {
        ThreadFactory f = (Runnable r) -> new Thread(r, "PreFlightAckEndpoint");
        executor = Executors.newScheduledThreadPool(1, f);
    }

    @Override
    public void start() {
        //no-op
    }

    @Override
    public void close() {
        LOG.debug("SHUTDOWN PREFLIGHT ENDPOINT DELAY SIMULATOR");
        executor.shutdownNow();
    }

    public void checkAckEndpoint(FutureCallback<HttpResponse> cb) {
        Runnable respond = () -> {
            cb.completed(new CannedOKHttpResponse(
                // response that means "preflight check is OK"
                new CannedEntity("{\"acks\":[0:false]}")));
        };

        delayResponse(respond);
    }

    protected void delayResponse(Runnable r) {        
        //return a single response with a delay uniformly distributed between  [0,5] ms
        if(!executor.isShutdown()){
            executor.schedule(r, (long) rand.nextInt(2), TimeUnit.MILLISECONDS);
        }
    }
}
