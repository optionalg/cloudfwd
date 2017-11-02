package com.splunk.cloudfwd.impl.util;

import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecIllegalStateException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import org.slf4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONNECTION_CLOSED;

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
    
    synchronized public void start() {
        if (started) {
            return;
        }
        Runnable r = ()->{
            while (started) {
                EventBatchImpl e = null;
                try {
                    e = this.queue.poll(1, TimeUnit.SECONDS);
                    if (!tryResend(e)) return;
                } catch (InterruptedException ex) {
                    LOG.warn("Resend queue poller interrupted");
                } catch (Exception ex2) {
                    LOG.error(ex2.getMessage());
                    if (e != null) {
                        connection.getCallbacks().failed(e, ex2);
                    } else {
                        connection.getCallbacks().systemError(ex2);
                    }
                }
            }
        };
        queuePoller = new Thread(r, "resend queue poller");
        queuePoller.start();
        started = true;
    }
    
    // Automatically stops if there are no channels in the load balancer and the queue is empty.
    private boolean tryResend(EventBatchImpl e) {
        if (e != null) {
            LOG.trace("Dequeued {} for resending.", e);
            if (connection.getLoadBalancer().hasNoChannels()) {
                String msg = "ResendQueue: no channels in load balancer. Failing event batch " + e;
                LOG.error(msg);
                connection.getCallbacks().failed(e, new HecIllegalStateException(msg,
                        HecIllegalStateException.Type.LOAD_BALANCER_NO_CHANNELS));
                if (queue.isEmpty()) {
                    stop();
                    return false;
                }
            } else {
                connection.getLoadBalancer().sendRoundRobin(e, true);
                LOG.info("resent event batch {}", e);
            }
        }
        return true;
    }
    
    public boolean offer(EventBatchImpl e) {
        // possibly add isResendable logic here. Or maybe we want to remove the channel from eventBatchImpl but do it in the load balancer TODO 
        if (!started) {
            LOG.error("Attempt to offer to resend queue that has not been started.");
            return false;
        }
        return queue.offer(e);
    }
    
    public synchronized void stop() {
        if (!started) {
            return;
        }
        started = false;
        queue.forEach(e -> {
            String msg = "Event batch not sent because resend queue poller was stopped: " + e;
            LOG.warn(msg);
            connection.getCallbacks().failed(e, 
                new HecConnectionStateException(msg, CONNECTION_CLOSED));
        });
        LOG.info("ResendQueue poller stopped.");
    }
}
