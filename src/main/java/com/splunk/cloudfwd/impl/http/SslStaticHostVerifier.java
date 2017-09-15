package com.splunk.cloudfwd.impl.http;

import org.apache.http.conn.ssl.AbstractVerifier;

import javax.net.ssl.SSLException;

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
 * This class is to verify a static hostname matches SSL Cert CNs. This
 * class can be used to verify common names in SSL Certificate for HTTP
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
 * URL built from IP addres resolved from www.myhostname.com DNS record
 * (given the server is configured with a valid certificate with CN,
 * like "*.myhostname.com")

   final HttpPost httpPost = new HttpPost("https://10.11.12.13:8433");
   httpClient.execute(httpPost, httpCallback);
   
 *
 */
public class SslStaticHostVerifier extends AbstractVerifier {

    private final String host;

    public SslStaticHostVerifier(String host) {
        this.host = host;
    }

    public final void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {
        this.verify(this.host, cns, subjectAlts, true);
    }

    public final String toString() {
        return "STRICT";
    }
}
