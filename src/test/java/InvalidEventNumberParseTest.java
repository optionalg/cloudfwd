
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.impl.http.HecErrorResponseValueObject;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

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
 * @author ghendrey
 */
public class InvalidEventNumberParseTest {
    
    @Test
    public void parseInvalidEventNumber() throws IOException{
        String jsonWithInvalidEventNumber ="{\"text\":\"Invalid data format\",\"code\":6,\"invalid-event-number\":42}";
        ObjectMapper mapper = new ObjectMapper();
        HecErrorResponseValueObject r = mapper.readValue(jsonWithInvalidEventNumber,
                    HecErrorResponseValueObject.class);             
        Assert.assertEquals("Expected 42", 42, r.getInvalidEventNumber());
        Assert.assertEquals("Expected 6", 6, r.getCode());
        Assert.assertEquals("Expected 'Invalid data format'", "Invalid data format", r.getText());
    }
    
    @Test
    public void parseWithoutInvalidEventNumber() throws IOException{
        String jsonWithInvalidEventNumber ="{\"text\":\"Invalid data format\",\"code\":6}";
        ObjectMapper mapper = new ObjectMapper();
        HecErrorResponseValueObject r = mapper.readValue(jsonWithInvalidEventNumber,
                    HecErrorResponseValueObject.class);             
        Assert.assertEquals("Expected 42", -1, r.getInvalidEventNumber());
        Assert.assertEquals("Expected 6", 6, r.getCode());
        Assert.assertEquals("Expected 'Invalid data format'", "Invalid data format", r.getText());
    }    
    
}
