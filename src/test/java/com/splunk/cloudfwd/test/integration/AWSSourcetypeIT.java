package com.splunk.cloudfwd.test.integration;/*
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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.splunk.cloudfwd.*;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;

import org.apache.http.message.BasicNameValuePair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * It is NOT RECOMMENDED to run these tests against a production
 * Splunk instance. See AbstractReconciliationTest for more details.
 *
 * Sends sample AWS sourcetype data to Splunk and validates that
 * data has been indexed as expected and that searches contain
 * the expected fields as specified in props.conf in the
 * AWS Kinesis Firehose Add-on for Splunk.
 *
 * NOTE: THESE TESTS WILL INSTALL OR UPDATE the AWS Kinesis Firehose Add-on on the
 * search head specified in AbstractReconciliationTest. All sample data
 * sent to Splunk is deleted after the tests complete. See
 * AbstractReconciliationTest for more details.
 *
 * Created by eprokop on 9/14/17.
 */
public class AWSSourcetypeIT extends AbstractReconciliationTest {
    private static final Logger LOG = LoggerFactory.getLogger(AWSSourcetypeIT.class.
            getName());

    // filenames
    private String addOnFileName = "./Splunk_TA_aws-kinesis-firehose-0.9.3.spl";
    private String vpcFlowFileName = "cloudwatchlogs_vpcflowlog_lambdaprocessed.sample";
    private String cloudwatchFileName = "cloudwatchevents_ec2autoscale.sample";
    private String cloudtrailFileName = "cloudtrail_modinputprocessed.sample";

    // expected fields
    private String[] vpcFlowFields = { "version", "account_id", "interface_id", "src_ip", "dest_ip", "src_port",
        "dest_port", "protocol_code", "packets", "bytes", "start_time", "end_time", "vpcflow_action", "log_status",
        "duration", "src", "dest", "protocol", "protocol_full_name", "action", "aws_account_id" };
    private String [] cloudtrailFields = { "user", "userName", "errorCode", "user_type", "src", "msg", "start_time",
        "user_group_id", "dest", "dvc", "src_user", "object", "vendor", "product", "app", "aws_account_id", "region" };

    @Before
    public void configureSplunk() throws IOException {
        installAddOn();
    }

    @Test
    public void vpcFlowLogsToRaw() throws IOException, InterruptedException {
        LOG.info("test: vpcFlowLogsToRaw");
        this.EXPECTED_FIELDS = vpcFlowFields;
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken("aws:cloudwatchlogs:vpcflow"));
        sendFromFile(vpcFlowFileName);
        Set<String> results = getEventsFromSplunk();
        verifyResults(eventStringsFromFile(vpcFlowFileName), results);
    }

    @Test
    public void cloudtrailToRaw() throws IOException, InterruptedException {
        LOG.info("test: cloudtrailToRaw");
        this.EXPECTED_FIELDS = cloudtrailFields;
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken("aws:cloudtrail"));
        sendFromFile(cloudtrailFileName);
        Set<String> results = getEventsFromSplunk();
        verifyResults(eventStringsFromFile(cloudtrailFileName), results);
    }

    @Test
    public void cloudwatchToRaw() throws IOException, InterruptedException {
        LOG.info("test: cloudwatchToRaw");
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken("aws:cloudwatch:events"));
        sendFromFile(cloudwatchFileName);
        Set<String> results = getEventsFromSplunk();
        verifyResults(eventStringsFromFile(cloudwatchFileName), results);
    }

    /*
     * Sends contents of a file in classpath as a single UnvalidatedByteBufferEvent.
     */
    private void sendFromFile(String fileName) throws InterruptedException {
        EventBatch events = Events.createBatch();
        events.add(byteBufferEventFromFile(fileName));
        connection.sendBatch(events);
        connection.close();
        this.callbacks.await(10, TimeUnit.MINUTES); // wait for acks
        if (callbacks.isFailed()) {
            Assert.fail(
                "There was a failure callback with exception class  " + callbacks.
                    getException() + " and message " + callbacks.getFailMsg());
        }
    }

    /*
     * Puts contents of a file into an UnvalidatedByteBufferEvent.
     */
    private UnvalidatedByteBufferEvent byteBufferEventFromFile(String filename) {
        UnvalidatedByteBufferEvent e;
        try {
            URL resource = getClass().getClassLoader().getResource(filename); // to use a file on classpath in resources folder.
            byte[] bytes = Files.readAllBytes(Paths.get(resource.getFile()));
            e = new UnvalidatedByteBufferEvent(ByteBuffer.wrap(bytes), 1);
        } catch (Exception ex) {
            Assert.fail("Problem reading file " + filename + ": " + ex.getMessage());
            e = null;
        }
        return e;
    }

    // TODO: should extract the list of events at the same time we read all bytes, so we don't need to read the file again here.
    /*
     * Gets a list of events from a file. Assumes events are delimited by newlines.
     */
    private List<Event> eventStringsFromFile(String filename) {
        List<String> eventStrings;
        try {
            URL resource = getClass().getClassLoader().getResource(filename); // to use a file on classpath in resources folder.
            eventStrings = Files.readAllLines(Paths.get(resource.getFile())); // events must be delimited by newlines
        } catch (Exception ex) {
            Assert.fail("Problem reading file " + filename + ": " + ex.getMessage());
            eventStrings = null;
        }
        List<Event> eventList = new ArrayList<>();
        for (String e : eventStrings) {
            eventList.add(RawEvent.fromText(e, "a"));
        }
        return eventList;
    }

    /*
     * Installs the Splunk Firehose Add-on on Splunk instance.
     * No restart is required.
     */
    private void installAddOn() throws IOException {
        URL resource = getClass().getClassLoader().getResource(addOnFileName);
        if (resource == null) {
            Assert.fail("Cannot find add-on in classpath.");
        }
        if (resource.getFile().isEmpty()) {
            Assert.fail("Problem getting filename of add-on.");
        }

        // build post request
        HttpPost httppost = new HttpPost(
                mgmtSplunkUrl() + "/services/apps/local");
        List<NameValuePair> params = new ArrayList<>(2);
        params.add(new BasicNameValuePair("output_mode", "json"));
        params.add(new BasicNameValuePair("name", resource.getFile()));
        params.add(new BasicNameValuePair("filename", "true"));
        params.add(new BasicNameValuePair("update", "true"));
        httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

        HttpResponse postResponse = httpClient.execute(httppost);
        String postReplyString = parseHttpResponse(postResponse);

        ObjectNode node = (ObjectNode)json.readTree(postReplyString);
        String location = node.findValue("location").asText();
        String status = node.findValue("status").asText();
        String label = node.findValue("label").asText();
        LOG.info(label + " " + status + " at " + location);
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        super.configureProps(settings);
        settings.setToken(createTestToken(null));
    }

    @Override
    protected int getNumEventsToSend() {
        return 1;
    }
}
