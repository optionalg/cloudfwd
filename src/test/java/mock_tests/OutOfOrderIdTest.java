package mock_tests;

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.PropertyKeys;
import test_utils.AbstractConnectionTest;
import test_utils.BasicCallbacks;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

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
/**
 *
 * @author ghendrey
 */
public class OutOfOrderIdTest extends AbstractConnectionTest {

    final int n = 100000;
    
    @Test
    public void testOutofOrderIDsWithCheckpointDisabled() throws InterruptedException{
        
        Assert.assertEquals("This test not designed to work with batches because it "
                +"counts events ack'd and compares them to number sent",0, connection.getSettings().getEventBatchSize());
        sendEvents();
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(PropertyKeys.ENABLE_CHECKPOINTS, "false");
        return props;
    }

    @Override
    protected int getNumEventsToSend() {
        return n;
    }

    @Override
    protected Event nextEvent(int seqno) {
        return super.nextEvent(Integer.MAX_VALUE-seqno); //sequence numbers counting down, not up!
    }

    protected BasicCallbacks getCallbacks() {
        return new IgnoreCheckpointCallbacks(n);
    }

    private class IgnoreCheckpointCallbacks extends BasicCallbacks {

        public IgnoreCheckpointCallbacks(int expected) {
            super(expected);
        }

        @Override
        public void checkpoint(EventBatch events) {
            //noop
        }

        @Override
        public void acknowledged(EventBatch events) {

            if (!acknowledgedBatches.add(events.getId())) {
                Assert.fail(
                        "Received duplicate acknowledgement for event batch:" + events.
                        getId());
                latch.countDown();
            }

            if (acknowledgedBatches.size() == getNumEventsToSend()) {
                latch.countDown();
            }

        }

    }

}
