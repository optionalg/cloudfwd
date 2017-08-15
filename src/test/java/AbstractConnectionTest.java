
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.FutureCallback;
import com.splunk.cloudfwd.http.EventBatch;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

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
public abstract class AbstractConnectionTest {

  private AckTracker ackTracker;
  protected Connection connection;

  @Before
  public void setUp() {   
    this.ackTracker = getAckTracker();
    this.connection = new Connection((FutureCallback)ackTracker, getProps());

  }
  
  @After
  public void tearDown() {
    //in case of failure we probably have events stuck on a channel. Therefore a regular close will just
    //hang out waiting (infinitely?) for the messages to flush out before gracefully closing. So when we see
    //a failure we must use the closeNow method which closes the channel regardless of whether it has
    //messages in flight.
    if(ackTracker.isFailed()){
      connection.closeNow();
    }
  }
  
  protected void sendEvents() throws InterruptedException{
    int i, expected = getNumBatchesToSend();
     for (i = 0; (!interrupt() && i < expected); i++) {
      final EventBatch events =nextEventBatch();
      events.setSeqNo(i+1); //1-based sequence numbers
      System.out.println("Send batch: " + events.getId() + " i=" + i);
      this.connection.sendBatch(events);
    }
    if (interrupt()) {
      System.out.println("Send batch i=" + i +
             " was the final batch before interrupted in test");
      System.out.println("Expected number of batches was " +
             expected);
    }
    connection.close(); //will flush 
    this.ackTracker.await(10, TimeUnit.MINUTES);
  }
  
  protected boolean interrupt() {
  return false;
  }

  protected abstract Properties getProps();
  protected abstract EventBatch nextEventBatch();
  protected abstract int getNumBatchesToSend();

  protected AckTracker getAckTracker() {
    return new AckTracker(getNumBatchesToSend());
  }
  
  protected void runTests(){
    Result result = JUnitCore.runClasses(getClass());

    for (Failure failure : result.getFailures()) {
      System.out.println(failure.toString());
    }
    System.out.println(result.wasSuccessful());
  }
  
}
