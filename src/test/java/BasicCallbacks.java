
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.EventBatch;

import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import com.splunk.cloudfwd.ConnectionCallbacks;

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

    Connection c = events.getSender().getConnection();

    /*
     Description of data:

     ** The below is logged when an event batch is acknowledged **

     test_id: unique ID for each @test
     run_id: unique ID that is the same across each test in the class
     num_channel_post_requests: number of event post requests for the channel
          at the time this event batch was posted(does not include retries)
     num_connection_post_requests: total number of event post requests for the
          connection at the time this event batch was posted
     ack_time_millis: time it took for this event batch to get acknowledged
     channel: the channel id
     url: the url this event batch was sent to
     ack_id: ack_id used within HEC and cloudfwd to poll for acks
     id: id of the event batch (equivalently, id of the last event added)
     label: a key for locating these logging lines if grepping

     */
    System.out.println(
        "test_id=" + c.getTestId() +
        " run_id=" + c.getRunId() +
        " num_channel_post_requests=" + events.getChannelPostCount() +
        " num_connection_post_requests=" + events.getConnectionPostCount() +
        " ack_time_millis=" + (System.currentTimeMillis() - events.getPostTime()) +
        " channel=" + events.getSender().getChannel().getChannelId() +
        " url=" + events.getSender().getEndpointUrl() +
        " ack_id=" + events.getAckId() +
        " id=" + events.getId() +
        " label=ACK"
    );
  }

  @Override
  public void failed(EventBatch events, Exception ex) {
    failed = true;
    latch.countDown();
    ex.printStackTrace();
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
