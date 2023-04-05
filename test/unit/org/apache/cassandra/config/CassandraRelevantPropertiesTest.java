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

package org.apache.cassandra.config;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.assertj.core.api.Assertions;

public class CassandraRelevantPropertiesTest
{
    private static final CassandraRelevantProperties TEST_PROP = CassandraRelevantProperties.ORG_APACHE_CASSANDRA_CONF_CASSANDRA_RELEVANT_PROPERTIES_TEST;

    @Test
    public void testString()
    {
        try
        {
            System.setProperty(TEST_PROP.getKey(), "some-string");
            Assertions.assertThat(TEST_PROP.getString()).isEqualTo("some-string");
        }
        finally
        {
            System.clearProperty(TEST_PROP.getKey());
        }
    }

    @Test
    public void testBoolean()
    {
        try
        {
            System.setProperty(TEST_PROP.getKey(), "true");
            Assertions.assertThat(TEST_PROP.getBoolean()).isEqualTo(true);
            System.setProperty(TEST_PROP.getKey(), "false");
            Assertions.assertThat(TEST_PROP.getBoolean()).isEqualTo(false);
            System.setProperty(TEST_PROP.getKey(), "junk");
            Assertions.assertThat(TEST_PROP.getBoolean()).isEqualTo(false);
            System.setProperty(TEST_PROP.getKey(), "");
            Assertions.assertThat(TEST_PROP.getBoolean()).isEqualTo(false);
        }
        finally
        {
            System.clearProperty(TEST_PROP.getKey());
        }
    }

    @Test
    public void testBoolean_null()
    {
        try
        {
            TEST_PROP.getBoolean();
        }
        finally
        {
            System.clearProperty(TEST_PROP.getKey());
        }
    }

    @Test
    public void testDecimal()
    {
        try
        {
            System.setProperty(TEST_PROP.getKey(), "123456789");
            Assertions.assertThat(TEST_PROP.getInt()).isEqualTo(123456789);
        }
        finally
        {
            System.clearProperty(TEST_PROP.getKey());
        }
    }

    @Test
    public void testHexadecimal()
    {
        try
        {
            System.setProperty(TEST_PROP.getKey(), "0x1234567a");
            Assertions.assertThat(TEST_PROP.getInt()).isEqualTo(305419898);
        }
        finally
        {
            System.clearProperty(TEST_PROP.getKey());
        }
    }

    @Test
    public void testOctal()
    {
        try
        {
            System.setProperty(TEST_PROP.getKey(), "01234567");
            Assertions.assertThat(TEST_PROP.getInt()).isEqualTo(342391);
        }
        finally
        {
            System.clearProperty(TEST_PROP.getKey());
        }
    }

    @Test(expected = ConfigurationException.class)
    public void testInteger_empty()
    {
        try
        {
            System.setProperty(TEST_PROP.getKey(), "");
            Assertions.assertThat(TEST_PROP.getInt()).isEqualTo(342391);
        }
        finally
        {
            System.clearProperty(TEST_PROP.getKey());
        }
    }

    @Test(expected = NullPointerException.class)
    public void testInteger_null()
    {
        try
        {
            TEST_PROP.getInt();
        }
        finally
        {
            System.clearProperty(TEST_PROP.getKey());
        }
    }
}
