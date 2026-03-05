package org.apache.atlas.web.integration.client;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigReader.class);
    private static Properties properties = new Properties();

    static {
        try (InputStream fis = ConfigReader.class.getClassLoader().getResourceAsStream("atlas-application.properties");) {
            if (fis != null) {
                properties.load(fis);
            } else {
                LOG.debug("atlas-application.properties not found on test classpath; using defaults");
            }
        } catch (IOException e) {
            LOG.warn("Failed to load atlas-application.properties for test client config; using defaults", e);
        }
    }

    public static String getString(String key) {
        return getString(key, null);
    }

    public static String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static Boolean getBoolean(String key) {
        String value = properties.getProperty(key);
        if (value != null) {
            return Boolean.parseBoolean(value);
        } else {
            return null;
        }
    }

    public static String[] getStringArray(String key) {
        String val = properties.getProperty(key);
        if (StringUtils.isNotEmpty(val)) {
            String[] vals = val.split(",");
            return vals;
        }
        return null;
    }
}
