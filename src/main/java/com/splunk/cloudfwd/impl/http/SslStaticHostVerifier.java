package com.splunk.cloudfwd.impl.http;

import org.apache.http.conn.ssl.AbstractVerifier;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

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
 *
 * Created by ssergeev on 6/29/17.
 *
 * This class is to verify a static hostname matches SSL Cert subject hostnames. 
 * This class can be used to verify common names in SSL Certificate for HTTP
 * Client connecting to a host by URL with IP address instead of hostname.
 * Standard verifier cannot verify IP address, as it does not know from which
 * hostname it was resolved from.
 *
 * Example of usage:

   final CloseableHttpAsyncClient httpClient = HttpAsyncClients
                .custom()
                .setHostnameVerifier(new SslStaticHostVerifier("www.myhostname.com"))
                .build();
   httpClient.start();

 * After the HttpClient is built and started, we can send a post request using
 * URL built from IP address resolved from www.myhostname.com DNS record
 * (given the server is configured with a valid certificate with CN,
 * like "*.myhostname.com" or a list of CNs like "myhostname.com", "www.myhostname.com")

   final HttpPost httpPost = new HttpPost("https://10.11.12.13:8433");
   httpClient.execute(httpPost, httpCallback);
 *
 */

/**
 * 
 */
public class SslStaticHostVerifier implements HostnameVerifier {

    private final String host;
    private final DefaultHostnameVerifier defaultHostnameVerifier = new DefaultHostnameVerifier();

    public SslStaticHostVerifier(String host) {
        this.host = host;
    }
    
    @Override
    public boolean verify(String s, SSLSession sslSession) {
        return defaultHostnameVerifier.verify(this.host, sslSession);
    }
}
