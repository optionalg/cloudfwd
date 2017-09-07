package com.splunk.cloudfwd.http;

import com.splunk.cloudfwd.util.ConnectionSettings;

import java.net.URL;

/**
 * Created by eprokop on 9/6/17.
 */
public class HttpSenderFactory {
    public static HttpSender createSender(URL url, String host, ConnectionSettings settings) {
        // enable http client debugging
        if (settings.httpDebugEnabled()) settings.enableHttpDebug();
        String token = settings.getToken();
        String cert = settings.getSSLCertContent();
        boolean certsDisabled =  settings.isCertValidationDisabled();

        HttpSender sender = new HttpSender(
                url.toString(), token, certsDisabled, cert, host);

        if (settings.isMockHttp()) {
            sender.setSimulatedEndpoints(
                settings.getSimulatedEndpoints());
        }
        return sender;
    }
}
