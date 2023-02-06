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
    public void testSystemPropertyisSet() {
        try
        {
            TEST_PROP.setString("test");
            Assertions.assertThat(System.getProperty(TEST_PROP.getKey())).isEqualTo("test"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        }
        finally
        {
            TEST_PROP.clearValue();
        }
    }

    @Test
    public void testString()
    {
        try
        {
            TEST_PROP.setString("some-string");
            Assertions.assertThat(TEST_PROP.getString()).isEqualTo("some-string");
        }
        finally
        {
            TEST_PROP.clearValue();
        }
    }

    @Test
    public void testBoolean()
    {
        try
        {
            TEST_PROP.setString("true");
            Assertions.assertThat(TEST_PROP.getBoolean()).isEqualTo(true);
            TEST_PROP.setString("false");
            Assertions.assertThat(TEST_PROP.getBoolean()).isEqualTo(false);
            TEST_PROP.setString("junk");
            Assertions.assertThat(TEST_PROP.getBoolean()).isEqualTo(false);
            TEST_PROP.setString("");
            Assertions.assertThat(TEST_PROP.getBoolean()).isEqualTo(false);
        }
        finally
        {
            TEST_PROP.clearValue();
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
            TEST_PROP.clearValue();
        }
    }

    @Test
    public void testDecimal()
    {
        try
        {
            TEST_PROP.setString("123456789");
            Assertions.assertThat(TEST_PROP.getInt()).isEqualTo(123456789);
        }
        finally
        {
            TEST_PROP.clearValue();
        }
    }

    @Test
    public void testHexadecimal()
    {
        try
        {
            TEST_PROP.setString("0x1234567a");
            Assertions.assertThat(TEST_PROP.getInt()).isEqualTo(305419898);
        }
        finally
        {
            TEST_PROP.clearValue();
        }
    }

    @Test
    public void testOctal()
    {
        try
        {
            TEST_PROP.setString("01234567");
            Assertions.assertThat(TEST_PROP.getInt()).isEqualTo(342391);
        }
        finally
        {
            TEST_PROP.clearValue();
        }
    }

    @Test(expected = ConfigurationException.class)
    public void testInteger_empty()
    {
        try
        {
            TEST_PROP.setString("");
            Assertions.assertThat(TEST_PROP.getInt()).isEqualTo(342391);
        }
        finally
        {
            TEST_PROP.clearValue();
        }
    }

    @Test
    public void testInteger_null()
    {
        try
        {
            TEST_PROP.getInt();
        }
        finally
        {
            TEST_PROP.clearValue();
        }
    }
}
