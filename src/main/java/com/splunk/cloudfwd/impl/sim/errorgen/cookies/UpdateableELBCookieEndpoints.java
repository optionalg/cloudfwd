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
package com.splunk.cloudfwd.impl.sim.errorgen.cookies;

import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.sim.AckEndpoint;
import com.splunk.cloudfwd.impl.sim.AcknowledgementEndpoint;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.CookiedOKHttpResponse;
import com.splunk.cloudfwd.impl.sim.EventEndpoint;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import com.splunk.cloudfwd.impl.sim.errorgen.PreFlightAckEndpoint;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpCookie;
import java.util.Random;

/**
 * This endpoint emulates an ELB endpoint with stickySession cookie
 */
public class UpdateableELBCookieEndpoints extends UpdateableCookieEndpoints {
    public static int maxAge = 0;
    
    public UpdateableELBCookieEndpoints() {
        super();
        toggleELBCookie();
    }
    
    /*
    Setter for maxAge
     */
    public static void setMaxAge(int age) {
        maxAge = age;
    }
    
    /**
     * Provides a Generates a new AWSELB cookie. 
     */
    public static synchronized void toggleELBCookie() {
        //FixMe: HttpCookie does not provide methods to print a proper header string, 
        //so we just construct the header string manually
        currentCookie = "AWSELB="+ getRandomHexString(ELB_COOKIE_LENGTH) + ";PATH=/";
        if (maxAge != 0) {
            currentCookie = currentCookie + ";MAX-AGE=" + String.valueOf(maxAge);
        }
        LOG.debug("UpdateableCookieEndpoints.toggleELBCookie currentCookie={}", currentCookie);
    }
    
    /**
     * Generates a new AWSELB cookie with a MAX-AGE parameter set to maxAge 
     * @param maxAge
     */
    public static synchronized void toggleELBCookie(int maxAge) {
        UpdateableELBCookieEndpoints.maxAge = maxAge;
        toggleELBCookie();
    }
    
    /**
     * Generates a new random Hex String numchars long 
     */
    private static String getRandomHexString(int numchars){
        Random r = new Random();
        StringBuffer sb = new StringBuffer();
        while(sb.length() < numchars){
            sb.append(Integer.toHexString(r.nextInt()));
        }
        return sb.toString().substring(0, numchars);
    }
    
}