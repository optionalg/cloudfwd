
import com.splunk.cloudfwd.ConfigStatus;
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
public class ConfigCheckTest extends AbstractConnectionTest{
    
    @Test
    public void testCheckConfigs() throws Exception{
        for(ConfigStatus s: super.connection.checkConfigs()){
            LOG.info(s.toString());
            Assert.assertNull(s.getProblem());
        }
    }

    @Override
    protected int getNumEventsToSend() {
        return 0;
    }
    
}
