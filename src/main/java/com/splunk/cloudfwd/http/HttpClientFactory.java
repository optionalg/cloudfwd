package com.splunk.cloudfwd.http;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.conn.util.PublicSuffixMatcher;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.impl.cookie.RFC6265CookieSpecProvider;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
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

    private static final Logger LOG = Logger.getLogger(HttpSender.class.getName());

    private String url;
    // Enable Parallel mode for HttpClient, which will be set to the default org.apache.http pool size
    private Integer maxConnTotal = 0;
    // Require a Valid SSL Cert by default
    private boolean disableCertVerification = false;
    // Optional SSL Certificate Authority public key
    private final String cert;
    // TODO
    private final String host;

    /**
     * Constructor
     * @param url http event collector input server
     * @param disableCertVerification disable SSL Certificate verification
     *                                set to true if using a self-signed cert on the HEC endpoint
     * @param cert SSL Certificate Authority public key to verify TLS with
     *             Self-Signed SSL Certificate chain
     * @param host hostname to match with Common Name records in ssl ceritificate. We use each http client
     *             to connect to just one IP address resolved from the hostname.
     */
    public HttpClientFactory(String url,
                             boolean disableCertVerification,
                             String cert, String host) {
        this.url = url;
        this.disableCertVerification = disableCertVerification;
        this.cert = cert;
        // Host header may include port, making sure we remove it
        this.host = host.replaceAll(":.*", "");
    }

    /**
     * Build and return an appropriate HttpAsyncClient
     * @return properly configure http client
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    public final CloseableHttpAsyncClient build()
            throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException
    {
        // If non-empty custom SSL Cert was provided
        if (!cert.trim().isEmpty()) {
            return build_http_client_custom_cert(cert);
        }
        // Build an Insecure HTTP Client if SSL Cert Verification is disabled
        if (disableCertVerification) {
            return build_http_client_insecure();

        }
        // Return a default HTTP client otherwise
        return build_default_client();
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
    public final SSLContext build_ssl_context_allow_all() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
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
    public final SSLContext build_ssl_context(String cert) {
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
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * build a default async http client
     * @return default http client
     */
    public final CloseableHttpAsyncClient build_default_client(){
        return HttpAsyncClients.custom()
                .setDefaultCookieSpecRegistry(buildRegistry())
                .setMaxConnTotal(maxConnTotal)
                // we want to make sure that SSL certificate match hostname in Host
                // header, as we may use IP address to connect to the SSL server
                .setHostnameVerifier(new SslStaticHostVerifier(this.host))
                .build();
    }

    /**
     * build cookie registry to support proxy server with sticky session
     * @return cookie registry
      */
    private final Registry<CookieSpecProvider> buildRegistry() {
        // configure cookie parsing
        PublicSuffixMatcher publicSuffixMatcher = PublicSuffixMatcherLoader.
                getDefault();
        return RegistryBuilder.
                <CookieSpecProvider>create()
                .register(CookieSpecs.DEFAULT,
                        new RFC6265CookieSpecProvider(publicSuffixMatcher))
                .build();
    }

    /**
     * build an async http client with a custom ssl certificate 'cert' provided
     * @param cert
     * @return http client built with provided cert
     */
    public final CloseableHttpAsyncClient build_http_client_custom_cert(String cert) {

        SSLContext ssl_context  = build_ssl_context(cert);

        return HttpAsyncClients.custom()
                .setDefaultCookieSpecRegistry(buildRegistry())
                .setMaxConnTotal(maxConnTotal)
                // we want to make sure that SSL certificate match hostname in Host
                // header, as we may use IP address to connect to the SSL server
                .setHostnameVerifier(new SslStaticHostVerifier(this.host))
                .setSSLContext(ssl_context)
                .build();
    }

    /**
     * build an insecure http client without ssl certificate validation.
     * Should not be used in any production environment.
     * @return insecure http client
     * @throws NoSuchAlgorithmException
     * @throws KeyStoreException
     * @throws KeyManagementException
     */
    public final CloseableHttpAsyncClient build_http_client_insecure() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        return HttpAsyncClients.custom()
                .setDefaultCookieSpecRegistry(buildRegistry())
                .setMaxConnTotal(maxConnTotal)
                .setHostnameVerifier(SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)
                .setSSLContext(build_ssl_context_allow_all())
                .build();
    }
}