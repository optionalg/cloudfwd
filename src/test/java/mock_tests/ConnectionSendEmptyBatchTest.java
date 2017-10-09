package mock_tests;/*
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

import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.Events;
import test_utils.AbstractConnectionTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kchen on 9/27/17.
 */
public class ConnectionSendEmptyBatchTest extends AbstractConnectionTest {
    @Test
    public void sendBatch() {
        EventBatch batch = Events.createBatch();
        int size = connection.sendBatch(batch);
        if (size != 0) {
            Assert.fail("Expect 0 bytes, but got: " + size);
        }
    }

    @Override
    protected int getNumEventsToSend() {
        return 2;
    }
}
