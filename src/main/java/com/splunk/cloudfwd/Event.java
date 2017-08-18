package com.splunk.cloudfwd;

/**
 * @copyright
 *
 * Copyright 2013-2015 Splunk, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"): you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

import static com.splunk.cloudfwd.http.HttpEventCollectorSender.MetadataHostTag;
import static com.splunk.cloudfwd.http.HttpEventCollectorSender.MetadataIndexTag;
import static com.splunk.cloudfwd.http.HttpEventCollectorSender.MetadataSourceTag;
import static com.splunk.cloudfwd.http.HttpEventCollectorSender.MetadataSourceTypeTag;
import static com.splunk.cloudfwd.http.HttpEventCollectorSender.MetadataTimeTag;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Container for Splunk http event collector event data
 */
public class Event {
    private double time; // time in fractional seconds since "unix epoch" format
    private final String severity;
    private final String message;
    private final String logger_name;
    private final String thread_name;
    private final Map<String, String> properties;
    private final String exception_message;
    private final Serializable marker;
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    /**
     * Create a new HttpEventCollectorEventInfo container
     * @param severity of event
     * @param message is an event content
     */
    public Event(
            final String severity,
            final String message,
            final String logger_name,
            final String thread_name,
            final Map<String, String> properties,
            final String exception_message,
            final Serializable marker
    ) {
        this.time = System.currentTimeMillis() / 1000.0;
        this.severity = severity;
        this.message = message;
        this.logger_name = logger_name;
        this.thread_name = thread_name;
        this.properties = properties;
        this.exception_message = exception_message;
        this.marker = marker;
    }

    /**
     * @return event timestamp in epoch format
     */
    public double getTime() {
        return time;
    }

    /**
     * @return event severity
     */
    public final String getSeverity() {
        return severity;
    }

    /**
     * @return event message
     */
    private byte[] getByteArray() {
        return message.getBytes();
    }

    private String eventFormatter(byte[] event) {
        String s = new String(event);
        return s;
    }

    /**
     * @return event logger name
     */
    public final String getLoggerName() {
        return logger_name;
    }

    /**
     * @return event thread name
     */
    public final String getThreadName() { return thread_name; }

    /**
     * @return event MDC properties
     */
    public Map<String,String> getProperties() { return properties; }

    /**
     * @return event's exception message
     */
    public final String getExceptionMessage() { return exception_message; }

    /**
     * @return event marker
     */
    public Serializable getMarker() { return marker; }

    @SuppressWarnings("unchecked")
    private static void putIfPresent(Map collection, String tag,
                                     String value) {
        if (value != null && value.length() > 0) {
            collection.put(tag, value);
        }
    }

    private String raw_blob_formatter() {
        // Append a new line to the end
        byte[] event = getByteArray();
        int event_size = getByteArray().length;
        if (event_size > 0 && event[event_size - 1] != '\n') {
            byte[] line_feed = new byte[]{'\n'};
            byte[] new_event = new byte[event.length + line_feed.length];
            // append line feed
            System.arraycopy(event, 0, new_event, 0, event.length);
            System.arraycopy(line_feed, 0, new_event, event.length, line_feed.length);
            return eventFormatter(new_event);
        } else {
            return eventFormatter(event);
        }
    }

    public String toEventEndpointString(Map<String, String> metadata) {
        // create event json content
        //
        // cf: http://dev.splunk.com/view/event-collector/SP-CAAAE6P
        //
        // event timestamp and metadata
        Map event = new LinkedHashMap();
        putIfPresent(event, MetadataTimeTag, String.format(Locale.US, "%.3f",
                getTime()));
        putIfPresent(event, MetadataHostTag, metadata.get(MetadataHostTag));
        putIfPresent(event, MetadataIndexTag, metadata.get(MetadataIndexTag));
        putIfPresent(event, MetadataSourceTag, metadata.get(MetadataSourceTag));
        putIfPresent(event, MetadataSourceTypeTag, metadata.get(
                MetadataSourceTypeTag));
        ObjectNode event_node = (ObjectNode)jsonMapper.valueToTree(event);
        // event body
        String event_body = "event";
        String user_record = new String(getByteArray());
        try {
            JsonNode user_node = jsonMapper.readTree(user_record);
            event_node.set(event_body, user_node);
        } catch (IOException e){
            event_node.put(event_body, user_record);
        }

        try {
            return jsonMapper.writeValueAsString(event_node);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            //TODO Handle Exception and Add Logger
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public String toRawEndpointString() {
        String user_record = new String(getByteArray());
        try {
            JsonNode user_node = jsonMapper.readTree(user_record);
            String jsonString = jsonMapper.writeValueAsString(user_node);
            return jsonString + ",";
        } catch (IOException e){
            return raw_blob_formatter();
        }
    }

}