import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.UnvalidatedByteBufferEvent;
import java.nio.ByteBuffer;
import java.util.Set;
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
public class ByteBufferWithMixedEventsIT extends AbstractReconciliationTest {

  int n = 25;
  int eventCounter = 0;

  @Test
  public void chalkFullOBytesForRawEndpoint() throws InterruptedException, HecConnectionTimeoutException {
    connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
    connection.getSettings().setToken(createTestToken("__singleline"));
    super.sendEvents();
    Set<String> searchResults = getEventsFromSplunk();
    verifyResults(getSentEvents(), searchResults);    
  }

  @Test
  public void chalkFullOBytesForEventEndpoint() throws InterruptedException, HecConnectionTimeoutException {
    connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    connection.getSettings().setToken(createTestToken(null));
    super.sendEvents();
    Set<String> searchResults = getEventsFromSplunk();
    verifyResults(getSentEvents(), searchResults);    
  }
  
  @Override
  protected Event nextEvent(int seqno) {

    byte[] a = new byte[512 * n]; //back the bytebuffer with a large enough array (can't say for sure size of each even below)
    ByteBuffer buf = ByteBuffer.wrap(a);
    for (int i = 0; i < n; i++) { //each pass through loop we stuff text and json into the same ByteBuffer
      Event jsonToRaw = getJsonEvent(++eventCounter); //each event in the bytes needs a unique ID for reconcilliation
      Event textToRaw = getTextEvent(++eventCounter); 
      buf.put(jsonToRaw.getBytes()).put(textToRaw.getBytes()); //mix json and text in one "event" buffer
      // ...So even though we jammed many event into one UnvalidatedByteBufferEvent, we will expect each to come back
      //distinctly from splunk. We are proving that we can stuff many events into an opaque byte buffer and send them
      //to splunk. Therefore we now record each of the bytes we stuffed into the larger ByteBuffer as an event.
      events.add(jsonToRaw); //cached for reconcilliation 
      events.add(textToRaw);
    }
    //Kids: don't forget to flip your buffers!
    buf.flip(); //set limit to positon (first byte past last byte of event data), and sets position to zero.
    //now we wrap the ByteBuffer, which contains 100 pairs of json and text into a single UnvalidatedByteBufferEvent.
    Event event = new UnvalidatedByteBufferEvent(buf, seqno); 
    return event;
  }

  @Override
  protected int getNumEventsToSend() {
    return 2;
  }

}
