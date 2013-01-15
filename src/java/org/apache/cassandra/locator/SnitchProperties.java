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
import java.util.Properties;

import org.apache.cassandra.io.util.FileUtils;

public class SnitchProperties
{
    public static final String RACKDC_PROPERTY_FILENAME = "cassandra-rackdc.properties";
    private static Properties properties = new Properties();

    static
    {
        InputStream stream = SnitchProperties.class.getClassLoader().getResourceAsStream(RACKDC_PROPERTY_FILENAME);
        try
        {
            properties.load(stream);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to read " + RACKDC_PROPERTY_FILENAME, e);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
    }

    /**
     * Get a snitch property value or return null if not defined.
     */
    public static String get(String propertyName, String defaultValue)
    {
        return properties.getProperty(propertyName, defaultValue);
    }
}
