package com.amazonaws.services.kinesis.samples.FirehoseSplunkExample;

import java.io.*;
import java.util.Properties;


public class FirehoseSplunkSettings {

    protected Properties properties = new Properties();

    public FirehoseSplunkSettings() {
        this.readPropertyFile();
    }

    public void readPropertyFile() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("fh.properties").getFile());
        try {
            properties.load(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getPropertyFor(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            try {
                throw new Exception(key + "Not found in fh.properties");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return value;
    }

}
