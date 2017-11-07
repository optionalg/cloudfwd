package com.splunk.cloudfwd.test.perf;

import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.test.mock.ThroughputCalculatorCallback;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import com.sun.javafx.binding.StringFormatter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Optionally pass command line parameters "token" and "url" as: 
 * mvn test -Dtest=MultiThreadedVolumeTest -DargLine="-Durl=<URL> -Dtoken=<TOKEN>"
 * 
 * Created by eprokop on 9/13/17.
 */
public class MultiThreadedVolumeTest extends AbstractPerformanceTest {
    private static final String MIN_THROUGHPUT_MBPS_KEY = "min_tp";
    private static final String MAX_THREADS_KEY = "max_threads";
    private static final String DURATION_MINUTES_KEY = "duration_mins";
    private static final String MAX_MEMORY_MB_KEY = "mem_mb";
    private static final String NUM_SENDERS_KEY = "num_senders";    
    
    // defaults for CLI parameters
    static {
        cliProperties.put(MIN_THROUGHPUT_MBPS_KEY, "75");
        cliProperties.put(MAX_THREADS_KEY, "300");
        cliProperties.put(DURATION_MINUTES_KEY, "15");
        cliProperties.put(MAX_MEMORY_MB_KEY, "500"); //500MB
        cliProperties.put(NUM_SENDERS_KEY, "128"); //128 senders
        cliProperties.put(PropertyKeys.TOKEN, null); // will use token in cloudfwd.properties by default
        cliProperties.put(PropertyKeys.COLLECTOR_URI, null); // will use token in cloudfwd.properties by default
    }
    
    private int numSenderThreads = 128;
    private AtomicInteger batchCounter = new AtomicInteger(0);
    private Map<Comparable, SenderWorker> waitingSenders = new ConcurrentHashMap<>(); // ackId -> SenderWorker
    private ByteBuffer buffer;
    private final String eventsFilename = "./many_text_events_no_timestamp.sample";
    private final String eventsFilenameJson = "./many_json_events_no_timestamp.sample";
    private long start = 0;
    private long testStartTimeMillis = System.currentTimeMillis();
    private long warmUpTimeMillis = 2*60*1000; // 2 mins
    private int batchSizeMB = 5; // 5 MBytes
    final String cloudwatchTemplate = "{\"version\": \"0\", \"id\": \"%s-%d\", \"detail-type\": \"EC2 Instance State-change Notification\", \"source\": \"aws.ec2\", \"account\": \"111122223333\", \"time\": \"%s\", \"region\": \"us-east-1\", \"resources\": [ \"arn:aws:ec2:us-east-1:123456789012:instance/i-12345678\" ], \"detail\": { \"instance-id\": \"i-12345678\", \"state\": \"terminated\"}}";
    final private int eventSize = cloudwatchTemplate.length() + UUID.randomUUID().toString().length() + String.valueOf(Integer.MAX_VALUE).length();

    private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedVolumeTest.class.getName());
    

    @Test
    public void sendTextToRaw() throws InterruptedException {   
        readEventsFile(eventsFilename);
        eventType = Event.Type.TEXT;
        sendToRaw();
    }
    
    @Test
    public void sendJsonToRaw() throws InterruptedException {
        generateEventByteBuffer();
        eventType = Event.Type.JSON;
        sendToRaw();
    }
    
    public void sendToRaw() throws InterruptedException {
        numSenderThreads = Integer.parseInt(cliProperties.get(NUM_SENDERS_KEY));
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        //create executor before connection. Else if connection instantiation fails, NPE on cleanup via null executor
        ExecutorService senderExecutor = Executors.newFixedThreadPool(numSenderThreads,
                (Runnable r) -> new Thread(r, "Connection client")); // second argument is Threadfactory
        List<Future> futureList = new ArrayList<>();
        for (int i = 0; i < numSenderThreads; i++) {
            futureList.add(senderExecutor.submit(new SenderWorker(i)::sendAndWaitForAcks));
        }
        
        try {
            Thread.sleep(Long.parseLong(cliProperties.get(DURATION_MINUTES_KEY))*60*1000); // blocks
        } catch (InterruptedException ex) {
            Assert.fail("Main thread was interrupted and test did not run for intended duration.");
        }
        
        checkAndLogPerformance(true);
        LOG.info("Performance passed all checks.");
        LOG.info("Stopping sender threads");
        futureList.forEach((f)->{
            f.cancel(true);
        });
        senderExecutor.shutdownNow();
        LOG.info("Closing connection");
        connection.close();
    }
    
    
    @Override
    protected void extractCliTestProperties() {
        if (System.getProperty("argLine") != null) {
            Set<String> keys = cliProperties.keySet();
            for (String e : keys) {
                if (System.getProperty(e) != null) {
                    cliProperties.replace(e, System.getProperty(e));
                }
            }
        }
        LOG.info("Test arguments: " + cliProperties 
            + " (token and url will be pulled from cloudfwd.properties if null)");
    }

    private void generateEventByteBuffer() {
        long generate_start = System.currentTimeMillis();
        String dateTime = new SimpleDateFormat("yyyy.MM.dd'T'HH:mm:ssz").format(new Date());
        buffer = ByteBuffer.allocate(batchSizeMB * 1024 * 1024 * 2 + eventSize * 2);
        int i = 0;
        String s = "s";
        try {
            while (buffer.position() < (batchSizeMB * 1024 * 1024)) {
                i++;
                s = String.format(cloudwatchTemplate, UUID.randomUUID(), i, dateTime);
                buffer.put(s.getBytes());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.info(String.format("Failed to generate eventByteBuffer. " +
                    "Generated s: %s, length: %d, buffer length: %s\n", s, i, buffer.position()));
        }
        LOG.info(String.format("Successfully generated eventByteBuffer in %s seconds. " + 
                "last event: %s, event length: %s, id: %s, buffer.length: %s\n", 
                (System.currentTimeMillis() - generate_start)/1000.0 , s, s.length(), i, 
                buffer.position()));
    }
    
    private void readEventsFile(String fileName) {
        try {
            URL resource = getClass().getClassLoader().getResource(fileName); // to use a file on classpath in resources folder.
            byte[] bytes = Files.readAllBytes(Paths.get(resource.getFile()));
            buffer = ByteBuffer.wrap(bytes);
        } catch (Exception ex) {
            Assert.fail("Problem reading file " + eventsFilename + ": " + ex.getMessage());
        }
    }
    
    @Override
    protected String getTestPropertiesFileName() {
        return "cloudfwd.properties"; //try as hard as we can to ignore test.properties and not use it
    }

    // not used
    @Override
    protected int getNumEventsToSend() {
        return Integer.MAX_VALUE;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new NotifyingCallbacks(getNumEventsToSend());
    }

    @Override
    protected Properties getProps() {
        Properties p = new Properties();
        if (cliProperties.get(PropertyKeys.TOKEN) != null) {
            p.put(PropertyKeys.TOKEN, cliProperties.get(PropertyKeys.TOKEN));
        }
        if (cliProperties.get(PropertyKeys.COLLECTOR_URI) != null) {
            p.put(PropertyKeys.COLLECTOR_URI, cliProperties.get(PropertyKeys.COLLECTOR_URI));
        }
        p.put(PropertyKeys.MOCK_HTTP_KEY, "false");
        p.put(KEY_ENABLE_TEST_PROPERTIES, "false");
        return p;
    }

    private void checkAndLogPerformance(boolean shouldAssert) {
        // throughput
        float mbps = showThroughput(System.currentTimeMillis(), start);

        // failures
        Integer numFailed = callbacks.getFailedCount();
        Integer numSent = batchCounter.get();
        float percentFailed = ( (float) numFailed / (float) numSent ) * 100F;
        LOG.info("Failed count: " + numFailed + " / " + numSent + " failed callbacks. (" + percentFailed + "%)");
        
        // acknowledged throughput
        int numAckedBatches = callbacks.getAcknowledgedBatches().size();
        long elapsedSeconds = (System.currentTimeMillis() - testStartTimeMillis) / 1000;
        LOG.info("Acknowledged batches: " + numAckedBatches);
        LOG.info("Batch size (MB): " + batchSizeMB);
        LOG.info("Acknowledged throughput (MBps): " + (float) batchSizeMB * (float) numAckedBatches / (float) elapsedSeconds);
        LOG.info("Acknowledged throughput (mbps): " + (float) batchSizeMB * 8F * (float) numAckedBatches / (float) elapsedSeconds);

        // thread count
        long threadCount = Thread.activeCount() - numSenderThreads;
        LOG.info("Thread count: " + threadCount);

        // memory usage
        long memoryUsed = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1000000; // MB
        LOG.info("Memory usage: " + memoryUsed + " MB");

        // asserts
        if (shouldAssert) {
            if (mbps != Float.NaN) {
                Assert.assertTrue("Throughput must be above minimum value of " + cliProperties.get(MIN_THROUGHPUT_MBPS_KEY),
                    mbps > Float.parseFloat(cliProperties.get(MIN_THROUGHPUT_MBPS_KEY)));
            }
            Assert.assertTrue("Percentage failed must be below 2%", percentFailed < 2F);
            Assert.assertTrue("Thread count must be below maximum value of " + cliProperties.get(MAX_THREADS_KEY),
                threadCount < Long.parseLong(cliProperties.get(MAX_THREADS_KEY)));
            Assert.assertTrue("Memory usage must be below maximum value of " + cliProperties.get(MAX_MEMORY_MB_KEY) + " MB",
                memoryUsed < Long.parseLong(cliProperties.get(MAX_MEMORY_MB_KEY)));
        }
    }

    public class SenderWorker {      
        private boolean failed = false;
        private int workerNumber;
        
        public SenderWorker(int workerNum){
            this.workerNumber = workerNum;
        }
        public void sendAndWaitForAcks() {
            try{
                EventBatch next = nextBatch(batchCounter.incrementAndGet());
                while (!Thread.currentThread().isInterrupted()) {
                    try{
                        failed = false;
                        EventBatch eb = next;
                        LOG.debug("Sender {} about to log metrics with id={}", workerNumber,  eb.getId());
                        logMetrics(eb, eb.getLength());
                        LOG.debug("Sender {} about to send batch with id={}", workerNumber,  eb.getId());
                        long sent = connection.sendBatch(eb);
                        LOG.info("Sender {} sent batch with id={}", workerNumber,  eb.getId());                        
                        next = nextBatch(batchCounter.incrementAndGet());
                        LOG.info("Sender {} generated next batch", workerNumber);
                        synchronized (this) {
                            // wait while the batch hasn't been acknowledged and it hasn't failed
                           while (!callbacks.getAcknowledgedBatches().contains(eb.getId()) && !failed) {
                               LOG.debug("Sender {}, about to wait", workerNumber);
                                waitingSenders.put(eb.getId(), this);
                                wait(500); //wait 500 ms //fixme 500ms does not work well
                                LOG.debug("Sender {}, waited 500ms", workerNumber);
                            }
                        }
                        waitingSenders.remove(eb.getId());                        
                    } catch (InterruptedException ex) {
                        LOG.debug("Sender {} exiting.", workerNumber);
                        return;
                    }
                }
                LOG.debug("Sender {} exiting.", workerNumber);
            }catch(Exception e){
                LOG.error("Worker {} caught exception {}",workerNumber, e .getMessage(), e);
            }
        }

        private void logMetrics(EventBatch batch, long sent) {
            Integer seqno = (Integer)batch.getId();
            long elapsed = System.currentTimeMillis() - testStartTimeMillis;
            boolean warmingUp = System.currentTimeMillis() - testStartTimeMillis < warmUpTimeMillis;
            if (warmingUp) {
                LOG.info("WARMING UP");
            }
            LOG.info("Elapsed time (mins): " + elapsed/(60 * 1000));
            
            // not synchronized but OK since approximate start and finish time is fine
            if (start == 0L && !warmingUp) { //true first time we exit the warmup period and enter valid sampling period
                LOG.info("Setting start time!");
                start = System.currentTimeMillis(); //start timing after warmup
            }

            ((ThroughputCalculatorCallback) callbacks).deferCountUntilAck(batch.getId(), sent);

            //if (seqno % 25 == 0) {
                checkAndLogPerformance(!warmingUp); // assert only if we are not warming up
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
            //sometimes events get acknowledged before the SenderWorker starts waiting
            if (waitingSenders.get(events.getId()) != null) {
                waitingSenders.get(events.getId()).tell();
            }
        }

        @Override
        public void failed(EventBatch events, Exception ex) {
            if(null ==events){
                LOG.error("failed {}", ex);
                return;
            }
            LOG.error("EventBatch with id=" + events.getId() + " failed");
            super.failed(events, ex);
            SenderWorker s = waitingSenders.get(events.getId());
            if (s != null) {
                s.failed();
                s.tell();
            }
        }
    }
}
