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
package com.splunk.cloudfwd.test.perf;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.PropertyKeys;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test tests creating and closeNow a connection in a tight loop. 
 * @author ghendrey
 */
public class CloseNowInALoopTest {
    private static final Logger LOG = LoggerFactory.getLogger(CloseNowInALoopTest.class);
    
    @Test
    public void loop() throws InterruptedException, ExecutionException{
        int numConnections = 100;
        PropertiesFileHelper settings = new PropertiesFileHelper();
        settings.setUrls("https://127.0.0.1:8088");
        settings.setToken("7263336d-ac05-4db9-92c3-9536922d11b1");
        List<Connection> connections = Collections.synchronizedList(new ArrayList<>());
        Callable<Connection> callable =()->{
            try{
                LOG.info("CREATING CONNECTION");
                Connection c = Connections.create(settings);
                connections.add(c);
                LOG.info("Connections size = " + connections.size());
                return c;
            }catch(Exception e){
                e.printStackTrace();
                throw e;
            }
        };
        ExecutorService ex = Executors.newCachedThreadPool();
        //List<Future<Connection>> futures = new ArrayList<>();
        new Thread(()->{
        for(int i=0;i<numConnections;i++){
            ex.submit(callable);
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex1) {
                Assert.fail("Interrupted sleep in creare connections loop");
            }
        }
        }).start();
        long start = System.currentTimeMillis();
        while(true){
            Assert.assertTrue("Failed to create and destroy all channels in lass than 1 minute",
                    System.currentTimeMillis()-start <60000);
            final AtomicInteger closedCount = new AtomicInteger(0);
            connections.forEach(c->{
                if(!c.isClosed()){
                    c.closeNow();
                }
                //we have to do this for each loop until all the connections have been added to the set
                closedCount.incrementAndGet();                 
            });
            LOG.info("Closed count: " + closedCount);
            if(closedCount.get() == numConnections){ 
                break;//all the connections have been added to the set, and forcedClosed
            }
            Thread.sleep(500);
        }
        ex.shutdown();
        connections.clear();
        
    }
    
    
}
