package com.splunk.cloudfwd.test.perf;

import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.test.mock.ThroughputCalculatorCallback;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by eprokop on 9/13/17.
 */
public class MultiThreadedVolumeTest extends AbstractPerformanceTest {
    private int numBatches = 200000;
    private int numThreads = 64;
    private AtomicInteger batchCounter = new AtomicInteger(0);
    private Map<Comparable, SenderWorker> waitingSenders = new ConcurrentHashMap<>(); // ackId -> SenderWorker
    private ExecutorService executor;
    private ByteBuffer buffer;
    private final String eventsFilename = "./1KB_event_5MB_batch.sample";
    private long start = 0;
    private long finish = 0;
    final float warmup = 0.0005f;
  
  protected static Map<String, String> cliProperties = new HashMap<>();
  
  private static final String MIN_THROUGHPUT_MBPS_KEY = "min_tp";
  private static final String MAX_THREADS_KEY = "max_threads";
  private static final String DURATION_MINUTES_KEY = "duration_mins";
  private static final String MAX_MEMORY_MB_KEY = "mem_mb";
  private static final String NUM_SENDERS_KEY = "num_senders";
  private static final String NUM_CONNECTIONS_KEY = "num_connections";
  private int numSenderThreads;
  
  private List<Connection> connections = new ArrayList<>();
  
  // defaults for CLI parameters
  static {
    cliProperties.put(MIN_THROUGHPUT_MBPS_KEY, "50");
    cliProperties.put(MAX_THREADS_KEY, "300");
    cliProperties.put(DURATION_MINUTES_KEY, "15");
    cliProperties.put(MAX_MEMORY_MB_KEY, "1024"); //500MB
    cliProperties.put(NUM_SENDERS_KEY, "384");
    cliProperties.put(PropertyKeys.TOKEN, null); // will use token in cloudfwd.properties by default
    cliProperties.put(PropertyKeys.COLLECTOR_URI, null); // will use uri in cloudfwd.properties by default
    // AWS uses 50 shards each with its own connection  
    cliProperties.put(NUM_CONNECTIONS_KEY, "4");
  }
  
  
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedVolumeTest.class.getName());

    @Override
    protected int getNumEventsToSend() {
        return numBatches; // how many batches callbacks should expect
    }

    @Test
    public void sendTextToRaw() throws InterruptedException {   
        //create executor before connection. Else if connection isntantiation failes, NPE on cleanup via null executor
        executor = Executors.newFixedThreadPool(numThreads,
                (Runnable r) -> new Thread(r, "Connection client")); // second argument is Threadfactory
        readEventsFile();
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        eventType = Event.Type.TEXT;
        List<Callable<Object>> callables = new ArrayList<>();
        createConnectionPool();
        for (int i = 0; i < numThreads; i++) {
            callables.add(Executors.callable(new SenderWorker()::sendAndWaitForAcks));
        }
//        startHealthCheck();
        executor.invokeAll(callables);
        close();
        // do thread cleanup
    }
  private Connection createAndConfigureTestConnection() {
    Connection connection = createAndConfigureConnection();
    ConnectionSettings connectionSettings = connection.getSettings();
    if (null ==connection){
      Assert.fail("null connection");
    }
    //to accurately simulate amazon load tests, we need to set the properties AFTER the connection is 
    //instantiated
    if (cliProperties.get(PropertyKeys.TOKEN) != null) {
      connectionSettings.setToken(cliProperties.get(PropertyKeys.TOKEN));
    }
    if (cliProperties.get(PropertyKeys.COLLECTOR_URI) != null) {
      connectionSettings.setUrls(cliProperties.get(PropertyKeys.COLLECTOR_URI));
    }
    return connection;
  }
  
  private void createConnectionPool() {
    for(int i = 0; i < Long.parseLong(cliProperties.get(NUM_CONNECTIONS_KEY)); i++) {
      connections.add(createAndConfigureTestConnection());
    }
  }
  
  protected Connection createAndConfigureConnection(){
    Properties settings = getTestProps();
    connection = createConnection(callbacks, settings);
    if(null == connection){
      return null;
    }
    configureConnection(connection);
    return connection;
  }
  
  private void readEventsFile() {
        try {
            URL resource = getClass().getClassLoader().getResource(eventsFilename); // to use a file on classpath in resources folder.
            byte[] bytes = Files.readAllBytes(Paths.get(resource.getFile()));
            buffer = ByteBuffer.wrap(bytes);
        } catch (Exception ex) {
            Assert.fail("Problem reading file " + eventsFilename + ": " + ex.getMessage());
        }
    }

    private void close() throws InterruptedException {
        connection.close(); //will flush
        this.callbacks.await(10, TimeUnit.MINUTES);
        if (callbacks.isFailed()) {
            Assert.fail(
                "There was a failure callback with exception class  " + callbacks.
                    getException() + " and message " + callbacks.getFailMsg());
        }
    }

    @Override
    protected String getTestPropertiesFileName() {
        return "cloudfwd.properties"; //try as hard as we can to ignore test.properties and not use it
    }

    @After
    public void cleanup() {
        if (null != executor){
            executor.shutdownNow();
        }
//        externalHealthPoller.stop();
        if (waitingSenders.size() != 0) {
            Assert.fail("All acks were not received.");
        }
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new NotifyingCallbacks(getNumEventsToSend());
    }

    @Override
    protected Properties getProps() {
        Properties p = new Properties();
        p.put(PropertyKeys.MOCK_HTTP_KEY, "false");
        p.put(KEY_ENABLE_TEST_PROPERTIES, false);
        p.put(PropertyKeys.MOCK_HTTP_CLASSNAME, "com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints");
        return p;
    }

    public class SenderWorker {
        private boolean failed = false;
        public void sendAndWaitForAcks() {
            try {
                EventBatch next = nextBatch(batchCounter.incrementAndGet());
                while ((Integer)next.getId() <= numBatches) {
                    EventBatch eb = next;
                    long sent = connections.get((int) eb.getId() % connections.size() ).sendBatch(eb);
                    logMetrics(eb, sent);
                    LOG.info("Sent batch with id=" + batchCounter.get());
                    next = nextBatch(batchCounter.incrementAndGet());

                    synchronized (this) {
                        // wait while the batch hasn't been acknowledged and it hasn't failed
                        while (!callbacks.getAcknowledgedBatches().contains(eb.getId()) && !failed) {
                            waitingSenders.put(eb.getId(), this);
                            wait(connection.getSettings().getAckTimeoutMS());
                        }
                    }
                    waitingSenders.remove(eb.getId());
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

        private void logMetrics(EventBatch batch, long sent) {
            Integer seqno = (Integer)batch.getId();
            int expected = getNumEventsToSend();
            boolean windingDown;
            boolean warmingUp;
            warmingUp = (((float) seqno) / expected) < warmup;
            if (warmingUp) {
                LOG.info("WARMING UP");
            }
            windingDown = (((float) seqno) / expected) > (1 - warmup);
            if (windingDown) {
                LOG.info("WINDING DOWN");
            }
            // not synchronized but OK since approximate start and finish time is fine
            if (start == 0L && !warmingUp) { //true first time we exit the warmup period and enter valid sampling period
                start = System.currentTimeMillis(); //start timing after warmup
            }
            if (finish == 0L && windingDown) {
                finish = System.currentTimeMillis();
            }
            //if (!warmingUp && !windingDown) {
                ((ThroughputCalculatorCallback)callbacks).
                    deferCountUntilAck(batch.getId(), sent);
                showThroughput(System.currentTimeMillis(), start);
            //}
        }

        private EventBatch nextBatch(int seqno) {
            EventBatch batch = Events.createBatch();
            UnvalidatedByteBufferEvent e = new UnvalidatedByteBufferEvent(
                buffer.asReadOnlyBuffer(), seqno);
            batch.add(e);
            return batch;
        }

        public void tell() {
            synchronized (this) {
                notify();
            }
        }

        public void failed() {
            this.failed = true;
        }
    }

    public class NotifyingCallbacks extends ThroughputCalculatorCallback {
        public NotifyingCallbacks(int numToExpect) {
            super(numToExpect);
        }

        @Override
        public void acknowledged(EventBatch events) {
            super.acknowledged(events);
            // sometimes events get acknowledged before the SenderWorker starts waiting
            if (waitingSenders.get(events.getId()) != null) {
                waitingSenders.get(events.getId()).tell();
            }
        }

        @Override
        public void failed(EventBatch events, Exception ex) {
            if(null ==events){
                LOG.error("failed {}", ex);
                return;
            }else{
                LOG.error("EventBatch with id=" + events.getId() + "failed");
            }
            super.failed(events, ex);
            SenderWorker s = waitingSenders.get(events.getId());
            if (s != null) {
                s.failed();
                s.tell();
            }
        }
    }
}
