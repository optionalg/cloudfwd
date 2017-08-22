
import com.splunk.cloudfwd.EventBatch;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import com.splunk.cloudfwd.ConnectonCallbacks;

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
public class BasicCallbacks implements ConnectonCallbacks {

  private Integer expectedAckCount;
  protected final CountDownLatch latch;
  private final Set<Comparable> acknowledgedBatches = new ConcurrentSkipListSet<>();
  protected boolean failed;
  private Comparable lastId;
  private String failMsg;

  public BasicCallbacks(int expected) {
    this.expectedAckCount = expected;
    this.latch = new CountDownLatch(1);
  }

  @Override
  public void acknowledged(EventBatch events) {
    if(null != lastId && lastId.compareTo(events.getId())>=0){
      Assert.fail("checkpoints received out of order. " + lastId + " before " + events.getId());
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
    latch.countDown();
    failMsg = "EventBatch failed to send. Exception message: " + ex.
            getMessage();
  }

  @Override
  public void checkpoint(EventBatch events) {
    System.out.println("SUCCESS CHECKPOINT " + events.getId());
    if (expectedAckCount.compareTo((Integer)events.getId())==0) {
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

}
