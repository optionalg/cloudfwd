package com.splunk.cloudfwd.impl.util;

import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import org.slf4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;

import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.RESEND_INTERRUPTED_BY_CONNECTION_CLOSE;

/**
 * Created by eprokop on 10/23/17.
 */
public class ResendQueue {
    private final Logger LOG;
    private BlockingQueue<EventBatchImpl> queue = new LinkedBlockingQueue<>();
    private Thread queuePoller;
    private boolean started;
    private ConnectionImpl connection;
    
    public ResendQueue(ConnectionImpl c) {
        this.LOG = c.getLogger(HecChannel.class.getName());
        this.connection = c;
    }
    
    public void start() {
        if (started) {
            return;
        }
        Runnable r = ()->{
            while (true) {
                EventBatchImpl e = null;
                try {
                    if (Thread.interrupted()) {
                        LOG.info("Resend queue poller interrupted");
                        return;
                    }
                    e = this.queue.poll(1, TimeUnit.SECONDS);
                    if (e != null) {
                        LOG.trace("Dequeued {} for resending.", e);
                        connection.getLoadBalancer().sendRoundRobin(e, true);
                    }
                } catch (InterruptedException ex) {
                    LOG.warn("Resend queue poller interrupted");
                    return;
                } catch (Exception ex2) {
                    connection.getCallbacks().failed(e, ex2);
                }
            }
        };
        queuePoller = new Thread(r, "resend queue poller");
        queuePoller.start();
        started = true;
    }
    
    public boolean offer(EventBatchImpl e) {
        if (!started) {
            LOG.error("Attempt to offer to resend queue that has not been started.");
            return false;
        }
        queue.offer(e);
        return true;
    }
    
    public void quiesce() {
        if (!started) {
            return;
        }
        ThreadFactory f = (r) -> new Thread(r, "resend queue interruptor");
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(f);
        executor.schedule(this::stop, 3, TimeUnit.MINUTES);
    }
    
    public void stop() {
        if (!started) {
            return;
        }
        queuePoller.interrupt();
        queue.forEach(e -> {
            String msg = "Events not sent because queue poller was interrupted: " + e;
            LOG.warn(msg);
            connection.getCallbacks().failed(
                e, new HecConnectionStateException(msg, RESEND_INTERRUPTED_BY_CONNECTION_CLOSE));
        });
        started = false;
    }
}
