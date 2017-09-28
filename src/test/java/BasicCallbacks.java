
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
  protected final CountDownLatch failLatch; //if failures are expected, this latch gates the test until it is released;
  protected final CountDownLatch latch; //if failures are expected, this latch gates the test until it is released;
  protected final CountDownLatch warnLatch;//if warnings are expected, this latch gates the test until it is released
  protected final Set<Comparable> acknowledgedBatches = new ConcurrentSkipListSet<>();
  protected boolean failed;
  private Comparable lastId;
  protected String failMsg;
  protected Exception exception;
  protected Exception systemWarning;
  

  public BasicCallbacks(int expected) {
    LOG.trace("Constructing BasicCallbacks");
    this.expectedAckCount = expected;
    this.failLatch= new CountDownLatch(1);
    this.latch = new CountDownLatch(1);
    this.warnLatch = new CountDownLatch(1);
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
  
  protected final  boolean isFailed() {
    return failed;
  }  
  
  protected boolean isWarnExpected(Exception e){
    return false;
  }
  
  public boolean shouldWarn(){
      return false;
  }
  
    protected final  boolean isWarned(){
      return systemWarning != null;
  }

  
    public void checkWarnings() {
        if(shouldWarn()&& !isWarned()){
            Assert.fail("A warn callback was expected, but none occurred.");
        }
        if (isWarned() && !isWarnExpected(systemWarning)) {
            Assert.fail(
                    "There was a systemWarning callback with exception class  " + 
                    getWarning());
        }
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
    failLatch.countDown(); //will lest test exit

  }
  
    @Override
    public void systemError(Exception ex) {
        LOG.error("SYSTEM ERROR {}",ex.getMessage());
        failed = true;   
        failMsg = "EventBatch failed to send. Exception message: " + ex.
                getMessage();
        exception = ex;
        if(!isFailureExpected(ex)){
          ex.printStackTrace(); //print the stack trace if we were not expecting failure
        }        
       failLatch .countDown();
    }

    @Override
    public void systemWarning(Exception ex) {
        LOG.warn("SYSTEM WARNING {}", ex.getMessage());
        this.systemWarning = ex;
        if(!isWarnExpected(ex)){
            ex.printStackTrace();
        }
        warnLatch.countDown();
    }  

  @Override
  public void checkpoint(EventBatch events) {
    LOG.info("SUCCESS CHECKPOINT " + events.getId()); 
    if (expectedAckCount.compareTo((Integer) events.getId()) == 0) {
      latch.countDown();
    }
  }

  public void await(long timeout, TimeUnit u) throws InterruptedException {
    if(shouldFail() && !this.failLatch.await(timeout, u)){
        throw new RuntimeException("test timed out waiting on failLatch");
    }
    if(shouldWarn() && !this.warnLatch.await(timeout, u)){
        throw new RuntimeException("test timed out waiting on warnLatch");
    }    
    if(!shouldFail() && !shouldWarn() &&  !this.latch.await(timeout, u)){
        throw new RuntimeException("test timed out waiting on latch");
    }
  }

  /**
   * @param expectedAckCount the expectedAckCount to set
   */
  public void setExpectedAckCount(int expectedAckCount) {
    this.expectedAckCount = expectedAckCount;
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
  
  public Exception getWarning(){
      return systemWarning;
  }


}
