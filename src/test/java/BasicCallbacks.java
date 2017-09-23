
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.EventBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class BasicCallbacks implements ConnectionCallbacks {

  private static final Logger LOG = LoggerFactory.getLogger(BasicCallbacks.class.getName());

  private Integer expectedAckCount;
  protected final CountDownLatch latch;
  protected final Set<Comparable> acknowledgedBatches = new ConcurrentSkipListSet<>();
  protected boolean failed;
  private Comparable lastId;
  protected String failMsg;
  protected Exception exception;
  

  public BasicCallbacks(int expected) {
    LOG.trace("Constructing BasicCallbacks");
    this.expectedAckCount = expected;
    this.latch = new CountDownLatch(1);
  }

    public void checkFailures() {
        if(shouldFail() && !isFailed()){
            Assert.fail("A failed callback was expected, but none occurred.");
        }
        if (isFailed() && !isFailureExpected(exception)) {
            Assert.fail(
                    "There was a failure callback with exception class  " + 
                    getException() + " and message " + getFailMsg());
        }
    }
  /**
   * Sublcasses can override to return true if expecting an exception (to suppress printing of stacktrace).
     * @param e The Exception that was received by failed() callback
   * @return
   */
  protected boolean isFailureExpected(Exception e){
    return false;
  }
  
  public boolean shouldFail(){
      return false;
  }
  
  @Override
  public void acknowledged(EventBatch events) {
    if (null != lastId && lastId.compareTo(events.getId()) >= 0) {
      Assert.fail(
              "checkpoints received out of order. " + lastId + " before " + events.
              getId());
    }

    if (!acknowledgedBatches.add(events.getId())) {
      Assert.fail(
              "Received duplicate acknowledgement for event batch:" + events.
              getId());
    }

  }

  @Override
  public void failed(EventBatch events, Exception ex) {
    failed = true;   
    failMsg = "EventBatch failed to send. Exception message: " + ex.
            getMessage();
    exception = ex;
    if(!isFailureExpected(ex)){
      ex.printStackTrace(); //print the stack trace if we were not expecting failure
    }
    //make sure we set the failed, failMsg and Exception *before* we unlatch    
    latch.countDown();

  }

  @Override
  public void checkpoint(EventBatch events) {
    LOG.info("SUCCESS CHECKPOINT " + events.getId());
    if (expectedAckCount.compareTo((Integer) events.getId()) == 0) {
      latch.countDown();
    }
  }

  public void await(long timeout, TimeUnit u) throws InterruptedException {
    this.latch.await(timeout, u);
  }

  /**
   * @param expectedAckCount the expectedAckCount to set
   */
  public void setExpectedAckCount(int expectedAckCount) {
    this.expectedAckCount = expectedAckCount;
  }

  /**
   * @return the failed
   */
  public boolean isFailed() {
    return failed;
  }

  /**
   * @return the failMsg
   */
  public String getFailMsg() {
    return failMsg;
  }

  /**
   * @return the exception
   */
  public Exception getException() {
    return exception;
  }

}
