/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splunk.cloudfwd.impl.sim;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author meemax
 */
public class RandomAckEndpoint extends AckEndpoint {
    
    private static final Logger LOG = LoggerFactory.getLogger(RandomAckEndpoint.class.
            getName());
    
    public RandomAckEndpoint() {
      this.unacked = new ArrayList<>(); // not yet acked
    }

    //start periodically flipping ackIds from false to true. This simulated event batches getting indexed.
    //Simulate getting acks out of order
    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        //stateFrobber will set the ack to TRUE
        Runnable stateFrobber = new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (RandomAckEndpoint.this) {
                        if(unacked.isEmpty()){
                            return;
                        }
                        int index = (int)(unacked.size() * Math.random());
                        Long key = (Long)((ArrayList<Long>)unacked).get(index);
                        if (null == key) {
                            return;
                        }
                        ((ArrayList<Long>)unacked).remove(index);
                        acked.add(key);
                    }//synchronized
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        };
        //NOTE: with fixed *DELAY* NOT scheduleAtFixedRATE. The latter will cause threads to pile up
        //if the execution time of a task exceeds the period. We don't want that.
        executor.scheduleWithFixedDelay(stateFrobber, 0, 10,
            TimeUnit.MICROSECONDS);
        started = true;
    }
}
