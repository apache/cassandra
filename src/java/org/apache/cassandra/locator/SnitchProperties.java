/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.locator;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnitchProperties
{
    private static final Logger logger = LoggerFactory.getLogger(SnitchProperties.class);
    public static final String RACKDC_PROPERTY_FILENAME = "cassandra-rackdc.properties";

    private Properties properties;

    public SnitchProperties()
    {
        properties = new Properties();
        InputStream stream = null;
        String configURL = System.getProperty(RACKDC_PROPERTY_FILENAME);
        try
        {
            URL url;
            if (configURL == null)
                url = SnitchProperties.class.getClassLoader().getResource(RACKDC_PROPERTY_FILENAME);
            else 
            	url = new URL(configURL);
            
            stream = url.openStream(); // catch block handles potential NPE
            properties.load(stream);
        }
        catch (Exception e)
        {
            // do not throw exception here, just consider this an incomplete or an empty property file.
            logger.warn("Unable to read {}", ((configURL != null) ? configURL : RACKDC_PROPERTY_FILENAME));
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
    }

    /**
     * Get a snitch property value or return defaultValue if not defined.
     */
    public String get(String propertyName, String defaultValue)
    {
        return properties.getProperty(propertyName, defaultValue);
    }

    public boolean contains(String propertyName)
    {
        return properties.containsKey(propertyName);
    }
}
