package com.splunk.cloudfwd.impl.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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

/**
 *
 * @author eprokop
 */
public class HecErrorResponseValueObject {
    private static final ObjectMapper mapper = new ObjectMapper();
    private String text;
    private int code = -1;
    private int invalidEventNumber=-1;

    public HecErrorResponseValueObject() {
    }
    
    public HecErrorResponseValueObject(String hecText, int hecCode, int invalidEventNum) {
        this.text = hecText;
        this.code = hecCode;
        this.invalidEventNumber = invalidEventNum;
    }
    
    public String toJson() throws JsonProcessingException{
        return mapper.writeValueAsString(this);
    }
    


    @Override
    public String toString() {
        return "HecErrorResponseValueObject{" + "text=" + text + ", code=" + code + ", invalidEventNumber=" + invalidEventNumber + '}';
    }
 
    /**
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * @return the text
     */
    public String getText() {
        return text;
    }

    /**
     * @param text the text to set
     */
    public void setText(String text) {
        this.text = text;
    }

    /**
     * @param code the code to set
     */
    public void setCode(int code) {
        this.code = code;
    }

    /**
     * @return the invalidEventNumber
     */
    public int getInvalidEventNumber() {
        return invalidEventNumber;
    }

    /**
     * @param invalidEventNumber the invalidEventNumber to set
     */
    public void setInvalidEventNumber(int invalidEventNumber) {
        this.invalidEventNumber = invalidEventNumber;
    }

}
