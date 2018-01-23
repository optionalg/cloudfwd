package com.splunk.cloudfwd.test.perf;

import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.test.mock.ThroughputCalculatorCallback;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    private static final String ENABLE_LIFECYCLE_METRICS_LOGGING_KEY = "enable_lifecycle_metrics_logging";
    private static final String THREADS_PER_CONNECTION = "threads_per_connection";  //specifies how many threads can share the same connection

    // defaults for CLI parameters
    static {        
        cliProperties.put(MIN_THROUGHPUT_MBPS_KEY, "50");
        cliProperties.put(MAX_THREADS_KEY, "300");
        cliProperties.put(DURATION_MINUTES_KEY, "5");
        cliProperties.put(MAX_MEMORY_MB_KEY, "1024"); //500MB
        cliProperties.put(NUM_SENDERS_KEY, "384");
        cliProperties.put(PropertyKeys.TOKEN, null); // will use token in cloudfwd.properties by default
        cliProperties.put(PropertyKeys.COLLECTOR_URI, null); // will use token in cloudfwd.properties by default
        cliProperties.put(ENABLE_LIFECYCLE_METRICS_LOGGING_KEY, "false");
        cliProperties.put(THREADS_PER_CONNECTION, "1");  //One thread, one connection object
    }

    
    protected int numSenderThreads = 128;
    private int threadsPerConnection = 1;
    protected AtomicInteger batchCounter = new AtomicInteger(0);
    private Map<Comparable, SenderWorker> waitingSenders = new ConcurrentHashMap<>(); // ackId -> SenderWorker
    protected ByteBuffer buffer;
    protected String eventsFilename = "./1KB_event_5MB_batch.sample";
    protected long start = 0;
    protected long testStartTimeMillis = System.currentTimeMillis();
    protected long warmUpTimeMillis = 2*60*1000; //2 mins
    private int batchSizeMB;

    protected static final Logger LOG = LoggerFactory.getLogger(MultiThreadedVolumeTest.class.getName());


    @Test
    public void runTest() throws InterruptedException {
        sendTextToRaw();
    }

    public void sendTextToRaw() throws InterruptedException {   
        numSenderThreads = Integer.parseInt(cliProperties.get(NUM_SENDERS_KEY));
        threadsPerConnection = Integer.parseInt(cliProperties.get(THREADS_PER_CONNECTION));

        //create executor before connection. Else if connection instantiation fails, NPE on cleanup via null executor
       // ExecutorService senderExecutor = ThreadScheduler.getSharedExecutorInstance("Connection client");
        ExecutorService senderExecutor = Executors.newFixedThreadPool(numSenderThreads,
        (Runnable r) -> new Thread(r, "Connection client")); // second argument is Threadfactory
        readEventsFile();
        eventType = Event.Type.TEXT;
        List<Future> futureList = new ArrayList<>();

        ConnectionManager connManager = new ConnectionManager(threadsPerConnection);
        for (int i = 0; i < numSenderThreads; i++) {
            SenderWorkerSettings senderData = new SenderWorkerSettings(i, connManager.getConnection());
            futureList.add(senderExecutor.submit(()->{
                SenderWorker s;
                try {
                    s = new SenderWorker(senderData);
                } catch (UnknownHostException ex) {
                    throw new RuntimeException(ex.getMessage(), ex);
                }
                s.sendAndWaitForAcks();
            }));
        }
        LOG.info("MultiThreadedVolumeTest main thread sleeping for " + cliProperties.get(DURATION_MINUTES_KEY) + " mins");
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
        LOG.info("Closing connections");
        connManager.closeConnections();
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

    protected void readEventsFile() {
        try {
            URL resource = getClass().getClassLoader().getResource(eventsFilename); // to use a file on classpath in resources folder.
            byte[] bytes = Files.readAllBytes(Paths.get(resource.getFile()));
            batchSizeMB = bytes.length / 1000000;
            buffer = ByteBuffer.wrap(bytes);
        } catch (Exception ex) {
            Assert.fail("Problem reading file " + eventsFilename + ": " + ex.getMessage());
        }
    }

    @Override
    protected String getTestPropertiesFileName() {
        return "/cloudfwd.properties"; //try as hard as we can to ignore test.properties and not use it
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
    protected void configureProps(ConnectionSettings settings) {
        super.configureProps(settings);
        String token = System.getProperty(PropertyKeys.TOKEN);
        String url = System.getProperty(PropertyKeys.COLLECTOR_URI);
        if (System.getProperty(PropertyKeys.TOKEN) != null) {
            settings.setToken(token);
        }
        if (System.getProperty(PropertyKeys.COLLECTOR_URI) != null) {
            settings.setUrls(url);
        }
        settings.setMockHttp(false);
        settings.setTestPropertiesEnabled(false);
    }

    protected void checkAndLogPerformance(boolean shouldAssert) {
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

        /*
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
        */
    }
    
    protected void updateTimestampsOnBatch() {
        // no-op - overridden in child class to do timestamp configuration on buffer variable
    }
 /*
    protected void setSenderToken(ConnectionSettings connectionSettings) {
        if (cliProperties.get(PropertyKeys.TOKEN) != null) {
            connectionSettings.setToken(cliProperties.get(PropertyKeys.TOKEN));
        }
    }
*/
    public class SenderWorker {      
        private boolean failed = false;
        private final int workerNumber;
        private Connection connection;

        public SenderWorker(SenderWorkerSettings data) throws UnknownHostException{
            this.workerNumber = data.workerNum;
            this.connection = data.connection;
            if (null == connection){
                Assert.fail("null connection");
            }
        }
        public void sendAndWaitForAcks() {
            LOG.info("sender {} starting its send loop", workerNumber);
                EventBatch eb = nextBatch(batchCounter.incrementAndGet());
                while (!Thread.currentThread().isInterrupted()) {
                    try{
                        failed = false;
                        LOG.debug("Sender {} about to log metrics with id={}", workerNumber,  eb.getId());
                        logMetrics(eb, eb.getLength());
                        LOG.debug("Sender {} about to send batch with id={}", workerNumber,  eb.getId());
                        long sent = this.connection.sendBatch(eb);
                        LOG.info("Sender={} sent={} bytes with id={}", this.workerNumber, sent, eb.getId());                                            
                        synchronized (this) {
                            // wait while the batch hasn't been acknowledged and it hasn't failed
                           while (!callbacks.getAcknowledgedBatches().contains(eb.getId()) && !failed) {
                               LOG.debug("Sender {}, about to wait", workerNumber);
                                waitingSenders.put(eb.getId(), this);
                                wait(1000); //wait1 sec
                                LOG.debug("Sender {}, waited 1 sec,", workerNumber);
                            }
                        }
                        if(!failed){
                            LOG.info("sender {} ackd {} in {} ms", this.workerNumber, eb.getLength(), System.currentTimeMillis()- ((EventBatchImpl)eb).getSendTimestamp());                        
                        }else{
                            LOG.info("sender {} failed in {} ms", this.workerNumber, System.currentTimeMillis()- ((EventBatchImpl)eb).getSendTimestamp());
                        }
                        waitingSenders.remove(eb.getId());    
                        LOG.info("{} unacked batches, {}", waitingSenders.size(), waitingSenders.keySet().toString());      
                        LOG.info("Sender {} generated next batch", workerNumber);
                        eb = nextBatch(batchCounter.incrementAndGet());                   
                    } catch (InterruptedException ex) {                        
                        LOG.warn("Sender {} exiting.", workerNumber);
                        return;
                    } catch(Exception e){
                        LOG.warn("Worker {} caught exception {} sending {}.",workerNumber, e .getMessage(), eb, e);
                        waitingSenders.remove(eb.getId()); 
                        eb = nextBatch(batchCounter.incrementAndGet());
                    }
                }
                LOG.warn("Sender {} exiting.", workerNumber);
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
            updateTimestampsOnBatch();
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
              
                LOG.info("{} byte batch acknowledged in {} ms", events.getLength(), System.currentTimeMillis()- ((EventBatchImpl)events).getSendTimestamp());
                waitingSenders.get(events.getId()).tell();
            }
            
            if (Boolean.parseBoolean(cliProperties.get(ENABLE_LIFECYCLE_METRICS_LOGGING_KEY))) {
                LifecycleMetrics lm = events.getLifecycleMetrics();
                String lifecycleMetricTag = "LIFECYCLE_METRIC";
                long currentTime = System.currentTimeMillis();

                // event post metrics
                LOG.info("{} type={} time={} testId={}", lifecycleMetricTag, LifecycleMetrics.POST_SENT_TIMESTAMP, lm.getPostSentTimestamp().get(0), testId);
                LOG.info("{} type={} time={} testId={}", lifecycleMetricTag, LifecycleMetrics.POST_RESPONSE_TIMESTAMP, lm.getPostResponseTimeStamp().get(0), testId);
                LOG.info("{} type={} time={} latency={} testId={}", lifecycleMetricTag, "post_response_latency", currentTime, lm.getPostResponseLatency(), testId);

                // load balancer metrics
                LOG.info("{} type={} time={} spintime={} testId={}", lifecycleMetricTag, "load_balancer_spin_time", currentTime, lm.getTimeInLoadBalancer(), testId);

                // ack metrics
                LOG.info("{} type={} time={} testId={}", lifecycleMetricTag, LifecycleMetrics.ACKED_TIMESTAMP, lm.getAckedTimestamp().get(0), testId);
                LOG.info("{} type={} time={} timelatency={} testId={}", lifecycleMetricTag, "ack_latency", currentTime, lm.getAcknowledgedLatency(), testId);
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

    public class SenderWorkerSettings {
        public int workerNum;
        public Connection connection;
        SenderWorkerSettings(int workerNum, Connection connection){
            this.workerNum = workerNum;
            this.connection = connection;
        }
    }

    public class ConnectionManager{
        private Connection sharedConnection = null;
        private int shareCount = 0;
        private final int shareFactor;
        private  List<Connection> connections = new ArrayList<>();
        ConnectionManager(int shareFactor){

            this.shareFactor = shareFactor;
        }

        public synchronized Connection  getConnection(){

            if((shareCount % shareFactor) == 0 ){
                LOG.info("Share factor met, creating new connection");
                sharedConnection = createConnection();

            }
            shareCount ++;
            return sharedConnection;
        }
        void closeConnections(){
            for(Connection conn : connections){
                conn.close();
            }
        }

        private Connection createConnection(){
            connection = createAndConfigureConnection();
            ConnectionSettings connectionSettings = connection.getSettings();
            //to accurately simulate amazon load tests, we need to set the properties AFTER the connection is
            //instantiated
            if (cliProperties.get(PropertyKeys.TOKEN) != null) {
                connectionSettings.setToken(cliProperties.get(PropertyKeys.TOKEN));
            }
            if (cliProperties.get(PropertyKeys.COLLECTOR_URI) != null) {
                connectionSettings.setUrls(cliProperties.get(PropertyKeys.COLLECTOR_URI));
            }
            connectionSettings.setMockHttp(false);
            connectionSettings.setTestPropertiesEnabled(false);

            //keep track of connections
            connections.add(connection);
            LOG.info("Total Connection objects used by MultiThreadedVolumeTest: {}", connections.size() );

            return connection;
        }


    }
}
