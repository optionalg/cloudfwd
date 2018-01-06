package com.splunk.cloudfwd.impl.http;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContexts;
import java.io.ByteArrayInputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.logging.Level;
import javax.net.ssl.SSLContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.slf4j.Logger;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.pool.ConnPoolControl;
import sun.security.provider.X509Factory;


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
 * Http Client Factory to create a proper Http Client based on parameters.
 * The library support followin modes:
 * - default http client with a regaular https ssl cert verification
 * - http client accepting only custom ssl cert
 * - insecure http client (should not be used for any production deployment)
 *
 */
public final class HttpClientFactory {
    //max connections per route, and max connections total will adjust dynamically when system is running
    public static int INITIAL_MAX_CONN_PER_ROUTE = 2; //Initial value. See HttpClientWrapper.adjustConnPoolSize for dyamic behavior
    public static int INITIAL_MAX_CONN_TOTAL = 4; //Initial value. See HttpClientWrapper.adjustConnPoolSize for dyamic behavior
    
    public static int CONNECT_TIMEOUT = 30000; //30 sec
    public static int SOCKET_TIMEOUT = 120000; //120 sec
    public static int REACTOR_SELECT_INTERVAL = 1000;   
    
    private final Logger LOG;
    // Enable Parallel mode for HttpClient, which will be set to the default org.apache.http pool size
    //private Integer maxConnTotal = 0;
    // Require a Valid SSL Cert by default
    private boolean disableCertVerification = false;
    // Optional SSL Certificate Authority public key
    private final String cert;
    // TODO
    private final String host;

    /**
     * Constructor
     * @param disableCertVerification disable SSL Certificate verification
     *                                set to true if using a self-signed cert on the HEC endpoint
     * @param cert SSL Certificate Authority public key to verify TLS with
     *             Self-Signed SSL Certificate chain
     * @param host hostname to match with Common Name records in ssl ceritificate. We use each http client
     *             to connect to just one IP address resolved from the hostname.
     */
    public HttpClientFactory(boolean disableCertVerification, String cert, String host, HttpSender sender) {
        LOG = sender.getConnection().getLogger(HttpClientFactory.class.getName());
        this.disableCertVerification = disableCertVerification;
        this.cert = cert;
        // Host header may include port, making sure we remove it
        this.host = host.replaceAll(":.*", "");
    }
    
    private HttpAsyncClientBuilder builderWithCustomOptions(final NHttpClientConnectionManager conMgr){
            return HttpAsyncClients.custom()
                    .setMaxConnTotal(INITIAL_MAX_CONN_TOTAL)
                    .setDefaultIOReactorConfig(null) //explicitely do NOT let the HttpAsyncClientBuilder construct its own ConnectionManager. We provide it.
                    .setConnectionManager(conMgr)
                    .disableCookieManagement()
                    .setMaxConnPerRoute(INITIAL_MAX_CONN_PER_ROUTE);        
    }   
    
    private PoolingNHttpClientConnectionManager createConnectionManager(SSLIOSessionStrategy sslStrategy){
            try{
                IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setSelectInterval(REACTOR_SELECT_INTERVAL)
                .setSoTimeout(SOCKET_TIMEOUT)
                .setConnectTimeout(CONNECT_TIMEOUT)
                //.setIoThreadCount(256)
                .build();
                ConnectingIOReactor ioreactor = new DefaultConnectingIOReactor(ioReactorConfig);   
                RegistryBuilder<SchemeIOSessionStrategy>  reg = RegistryBuilder.<SchemeIOSessionStrategy>create().register("https",
                        sslStrategy);               
               return new PoolingNHttpClientConnectionManager(ioreactor, reg.build());
            } catch (IOReactorException ex) {
                LOG.error(ex.getMessage(), ex);
                throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * Build and return an appropriate HttpAsyncClient
     * @return properly configure http client
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    public final CloseableHttpAsyncClientAndConnPoolControl build()
            throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException
    {
        // If non-empty custom SSL Cert was provided
        if (!cert.trim().isEmpty()) {
            return buildHttpClientCustomCert(cert);
        }
        // Build an Insecure HTTP Client if SSL Cert Verification is disabled
        if (disableCertVerification) {
            return buildHttpClientInsecure();

        }
        // Return a default HTTP client otherwise
        return buildDefaultClient();
    }

    /**
     * build a X509 certificate based on the certificate content 'cert' provided
     * @param cert
     * @return x509 cert built from cert content provided
     */
    private final X509Certificate CertStrToX509(String cert) throws CertificateException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        String cert_content_stripped = cert
                .replaceAll(X509Factory.BEGIN_CERT, "")
                .replaceAll(X509Factory.END_CERT, "");
        byte [] decoded = Base64.decode(cert_content_stripped);
        return (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(decoded));
    }

    /**
     * build an insecure SSL Context to accept all sertificates
     * @return insecure ssl context
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    public static final SSLContext buildSslContextAllowAll() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
            public boolean isTrusted(X509Certificate[] certificate,
                                     String type) {
                return true;
            }
        };
        return SSLContexts.custom().loadTrustMaterial(
                null, acceptingTrustStrategy).build();
    }

    /**
     * build a SSL Context which accepts only certificates in a chain created by provided SSL Certificate cert
     * @param cert ssl certificate content
     * @return ssl context configured to accept just the provided cert
     */
    public final SSLContext buildSslContext(String cert) {
        try {
            // load certificate from file
            X509Certificate cert_obj = CertStrToX509(cert);

            // add cloudCA to the KetStore
            KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
            keystore.load(null, null); // init an empty keystore
            keystore.setCertificateEntry("cloudfwd_custom_certificate", cert_obj); // load cert to the hostname

            // set up sslContext
            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(keystore,
                    new TrustSelfSignedStrategy()).build();
            return sslContext;
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * build a default async http client
     * @return default http client
     */
    public final CloseableHttpAsyncClientAndConnPoolControl buildDefaultClient(){
        
        try {
            SSLIOSessionStrategy sessStrat = new SSLIOSessionStrategy(SSLContext.getDefault(), new SslStaticHostVerifier(this.host));
            PoolingNHttpClientConnectionManager connectionMgr =createConnectionManager(sessStrat);
            CloseableHttpAsyncClient client = builderWithCustomOptions(connectionMgr)
                    .build();
            return new CloseableHttpAsyncClientAndConnPoolControl(client, connectionMgr);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }

        
    }


    /**
     * build an async http client with a custom ssl certificate 'cert' provided
     * @param cert
     * @return http client built with provided cert
     */
    public final CloseableHttpAsyncClientAndConnPoolControl buildHttpClientCustomCert(String cert) {
        // we want to make sure that SSL certificate match hostname in Host
        // header, as we may use IP address to connect to the SSL server
        SSLContext ssl_context  = buildSslContext(cert);
        SSLIOSessionStrategy sessStrat = new SSLIOSessionStrategy(ssl_context, new SslStaticHostVerifier(this.host));
        PoolingNHttpClientConnectionManager connectionMgr =createConnectionManager(sessStrat);
        CloseableHttpAsyncClient client = builderWithCustomOptions(connectionMgr)
                .build();
        return new CloseableHttpAsyncClientAndConnPoolControl(client, connectionMgr);
    }

    /**
     * build an insecure http client without ssl certificate validation.
     * Should not be used in any production environment.
     * @return insecure http client
     * @throws NoSuchAlgorithmException
     * @throws KeyStoreException
     * @throws KeyManagementException
     */
    public final CloseableHttpAsyncClientAndConnPoolControl buildHttpClientInsecure() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        SSLIOSessionStrategy sessStrat = new SSLIOSessionStrategy(buildSslContextAllowAll(), NoopHostnameVerifier.INSTANCE);
        PoolingNHttpClientConnectionManager connectionMgr =createConnectionManager(sessStrat);
        CloseableHttpAsyncClient client =  builderWithCustomOptions(connectionMgr)
                .build();
        return new CloseableHttpAsyncClientAndConnPoolControl(client, connectionMgr);
    }
    
    
}
