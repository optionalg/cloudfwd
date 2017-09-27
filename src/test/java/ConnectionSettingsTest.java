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

import java.util.Properties;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.PropertyKeys;
import org.junit.Test;
import org.junit.Assert;

/**
 * Created by kchen on 9/27/17.
 */
public class ConnectionSettingsTest {

    @Test
    public void getSSLCertContentForCloudInstance() {
        Properties props = new Properties();
        props.put(PropertyKeys.COLLECTOR_URI, "https://customer.cloud.splunk.com:8088");
        Connection c = Connections.create(null);

        // For cloud instance, if we didn't set CLOUD_SSL_CERT_CONTENT in overrides,
        // it will pick up from lb.properties
        // Non-exist ssl content
        ConnectionSettings settings = new ConnectionSettings(c, props);
        String defaultCloudCertContent = "-----BEGIN CERTIFICATE-----" +
                "MIIB/DCCAaGgAwIBAgIBADAKBggqhkjOPQQDAjB+MSswKQYDVQQDEyJTcGx1bmsg" +
                "Q2xvdWQgQ2VydGlmaWNhdGUgQXV0aG9yaXR5MRYwFAYDVQQHEw1TYW4gRnJhbmNp" +
                "c2NvMRMwEQYDVQQKEwpTcGx1bmsgSW5jMQswCQYDVQQIEwJDQTEVMBMGA1UECxMM" +
                "U3BsdW5rIENsb3VkMB4XDTE0MTExMDA3MDAxOFoXDTM0MTEwNTA3MDAxOFowfjEr" +
                "MCkGA1UEAxMiU3BsdW5rIENsb3VkIENlcnRpZmljYXRlIEF1dGhvcml0eTEWMBQG" +
                "A1UEBxMNU2FuIEZyYW5jaXNjbzETMBEGA1UEChMKU3BsdW5rIEluYzELMAkGA1UE" +
                "CBMCQ0ExFTATBgNVBAsTDFNwbHVuayBDbG91ZDBZMBMGByqGSM49AgEGCCqGSM49" +
                "AwEHA0IABPRRy9i3yQcxgMpvCSsI7Qe6YZMimUHOecPZWaGz5jEfB4+p5wT7dF3e" +
                "QrgjDWshVJZvK6KGO7nDh97GnbVXrTCjEDAOMAwGA1UdEwQFMAMBAf8wCgYIKoZI" +
                "zj0EAwIDSQAwRgIhALMUgLYPtICN9ci/ZOoXeZxUhn3i4wIo2mPKEWX0IcfpAiEA" +
                "8Jid6bzwUqAdDZPSOtaEBXV9uRIrNua0Qxl1S55TlWY=" +
                "-----END CERTIFICATE-----";
        String sslCert = settings.getSSLCertContent();
        if (!sslCert.equals(defaultCloudCertContent)) {
            Assert.fail("Expect: " + defaultCloudCertContent + "\nbut got: " + sslCert);
        }

        String expectedSSLCert = "testing ssl cert";
        props.put(PropertyKeys.CLOUD_SSL_CERT_CONTENT, expectedSSLCert);
        settings = new ConnectionSettings(c, props);
        sslCert = settings.getSSLCertContent();
        if (sslCert != expectedSSLCert) {
            Assert.fail("Expect: " + expectedSSLCert + "\nbut got:" + sslCert);
        }
    }

    @Test
    public void getSSLCertContentForNonCloudInstance() {
        Properties props = new Properties();
        props.put(PropertyKeys.COLLECTOR_URI, "https://localhost:8088");
        Connection c = Connections.create(null);

        // Non-exist ssl content
        ConnectionSettings settings = new ConnectionSettings(c, props);
        String sslCert = settings.getSSLCertContent();
        if (!sslCert.equals("")) {
            Assert.fail("Expect empty ssl cert content, but got: " + sslCert);
        }

        // Non-empty ssl content
        String expectedSSLCert = "testing ssl cert";
        props.put(PropertyKeys.SSL_CERT_CONTENT, expectedSSLCert);
        settings = new ConnectionSettings(c, props);
        sslCert = settings.getSSLCertContent();
        if (!sslCert.equals(expectedSSLCert)) {
            Assert.fail("Expect: " + expectedSSLCert + "\nbut got: " + sslCert);
        }
    }
}