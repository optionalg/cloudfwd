package com.splunk.cloudfwd.impl.sim.errorgen.ackslost;

import com.splunk.cloudfwd.impl.sim.AcknowledgementEndpoint;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by eprokop on 11/1/17.
 * 
 * These endpoints never return any acks.
 */
public class AllLossyEndpoints extends SimulatedHECEndpoints {
    private static final Logger LOG = LoggerFactory.getLogger(LossyEndpoints.class.getName());
    @Override
    protected AcknowledgementEndpoint createAckEndpoint() {
        LOG.trace("Add ENDPOINT DEAD");
        return new AckLossyEndpoint();
    }
}
