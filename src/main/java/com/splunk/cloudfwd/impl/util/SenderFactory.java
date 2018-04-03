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
package com.splunk.cloudfwd.impl.util;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.http.HttpSender;

import java.net.*;
import com.splunk.cloudfwd.error.HecConnectionStateException;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class SenderFactory {
    
    private ConnectionSettings settings;
    private final Logger LOG;
    
    public SenderFactory(ConnectionImpl connection, ConnectionSettings settings) {
        this.settings = settings;
        LOG = connection.getLogger(SenderFactory.class.getName());
    }

    private HttpSender createSender(String url, String sslHost) {
      // enable http client debugging
      if (settings.isHttpDebugEnabled()){
          settings.setHttpDebugEnabled(true);
      }
      String sslCert = settings.getSSLCertContent();
      HttpSender sender = new HttpSender(url, sslHost, settings, settings.isCertValidationDisabled(), sslCert);
      if(settings.isMockHttp()){
        sender.setSimulatedEndpoints(settings.getSimulatedEndpoints());
      }
      return sender;
  }
  
    public HttpSender createSender(InetSocketAddress s) {
        try {
            // this is to support the creation of channels for socket addresses that are not resolvable
            // so that they can get decomissioned and recreated at a later time, in case DNS recovers
            if (s.isUnresolved()) { 
                //since the hostname could not be resolved to an IP address, we cannot construct the URL
                //using the resolved hostAddr as we do below. 3
                return createSender("https://"+s.toString(), s.getHostName() + ":" + s.getPort());
            }            
            //URLS for channel must be based on IP address not hostname since we
            //have many-to-one relationship between IP address and hostname via DNS records
            String hostAddr = s.getAddress().getHostAddress();
            if (s.getAddress() instanceof Inet6Address) {
                hostAddr = "[" + hostAddr + "]"; // java.net.URL requires braces for IPv6 host addresses
            }

            URL url = new URL("https://" + hostAddr + ":" + s.getPort());
            LOG.debug("createSender: adding url={}", url);
            //We should provide a hostname for http client, so it can properly set Host header
            //this host is required for many proxy server and virtual servers implementations
            //https://tools.ietf.org/html/rfc7230#section-5.4
            String host = s.getHostName() + ":" + s.getPort();

            return createSender(url.toString(), host);
        } catch (MalformedURLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new HecConnectionStateException(ex.getMessage(),
                    HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
    }

}
