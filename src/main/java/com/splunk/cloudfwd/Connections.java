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
package com.splunk.cloudfwd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for getting a Connection.
 *
 * @author ghendrey
 */
public class Connections {
    
    private static Logger LOG = LoggerFactory.getLogger(Connections.class.getName());

    private Connections() {

    }

    public static Connection create(ConnectionCallbacks c) {
        return create(c, new ConnectionSettings());
    }
    
    /**
     * When creating a connection, an attempt is made to check the server-side configurations of every channel managed by the 
     * load balancer. If any of the channels finds misconfigurations, such as HEC acknowldegements disabled, or invalid HEC
     * token, then an exception is thrown from Connections.create. This is 'fail fast' behavior designed to quickly expose 
     * serverside configuration issues.
     * @param cb
     * @param settings
     * @return
     */
    public static Connection create(ConnectionCallbacks cb, ConnectionSettings settings) {
        // Print out ConnectionSettings properties before Connection init - to troubleshoot failing init
        Connections.prettyPrintConnectionSettings(settings, 
                "ConnectionSettings properties before Connection init are:", 
                "Could not pretty print Connection properties before Connection init");

        ConnectionImpl c = new ConnectionImpl(cb, settings);
        Connections.setupConnection(c, settings);
        return c;
    }
    
    /**
     * Creates a Connection with DefaultConnectionCallbacks
     * @param settings Properties that customize the Connection
     * @return
     */
    public static Connection create(ConnectionSettings settings) {
        ConnectionImpl c = new ConnectionImpl(new DefaultConnectionCallbacks(), settings);
        Connections.setupConnection(c, (settings));
        return c;
    }   
    
    private static void setupConnection(ConnectionImpl c, ConnectionSettings settings) {
        LOG = c.getLogger(Connections.class.getName());
        settings.setConnection(c);

        // Print out ConnectionSettings properties
        Connections.prettyPrintConnectionSettings(settings, 
                "ConnectionSettings properties after Connection init are:", 
                "Could not pretty print ConnectionSettings properties after Connection init");
    }
    
    private static void prettyPrintConnectionSettings(ConnectionSettings settings, String successMsg, String failMsg) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        try {
            LOG.info(successMsg + "\n" + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(settings));
        } catch (JsonProcessingException e) {
            LOG.error(failMsg);
        }
    }
    
}
