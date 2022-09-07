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

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class CassandraRelevantPropertiesTest
{
    private static final CassandraRelevantProperties TEST_PROP = CassandraRelevantProperties.ORG_APACHE_CASSANDRA_CONF_CASSANDRA_RELEVANT_PROPERTIES_TEST;

    @After
    public void after()
    {
        System.clearProperty(TEST_PROP.getKey());
    }

    @Test
    public void testIsPresent()
    {
        assertThat(TEST_PROP.isPresent()).isFalse();
        System.setProperty(TEST_PROP.getKey(), "some-string");
        assertThat(TEST_PROP.isPresent()).isTrue();
    }

    @Test
    public void testString()
    {
        System.setProperty(TEST_PROP.getKey(), "some-string");
        assertThat(TEST_PROP.getString()).isEqualTo("some-string");
    }

    @Test
    public void testString_null()
    {
        assertThat(TEST_PROP.getString()).isNull();
    }

    @Test
    public void testString_override_default()
    {
        assertThat(TEST_PROP.getString("other-string")).isEqualTo("other-string");
        TEST_PROP.setString("this-string");
        assertThat(TEST_PROP.getString("other-string")).isEqualTo("this-string");
    }

    @Test
    public void testBoolean()
    {
        System.setProperty(TEST_PROP.getKey(), "true");
        assertThat(TEST_PROP.getBoolean()).isTrue();
        System.setProperty(TEST_PROP.getKey(), "false");
        assertThat(TEST_PROP.getBoolean()).isFalse();
        System.setProperty(TEST_PROP.getKey(), "junk");
        assertThat(TEST_PROP.getBoolean()).isFalse();
        System.setProperty(TEST_PROP.getKey(), "");
        assertThat(TEST_PROP.getBoolean()).isFalse();
    }

    @Test
    public void testBoolean_null()
    {
        assertThat(TEST_PROP.getBoolean()).isFalse();
    }

    @Test
    public void testBoolean_override_default()
    {
        assertThat(TEST_PROP.getBoolean(true)).isTrue();
        TEST_PROP.setBoolean(false);
        assertThat(TEST_PROP.getBoolean(true)).isFalse();
    }

    @Test
    public void testDecimal()
    {
        System.setProperty(TEST_PROP.getKey(), "123456789");
        assertThat(TEST_PROP.getInt()).isEqualTo(123456789);
    }

    @Test
    public void testHexadecimal()
    {
        System.setProperty(TEST_PROP.getKey(), "0x1234567a");
        assertThat(TEST_PROP.getInt()).isEqualTo(305419898);
    }

    @Test
    public void testOctal()
    {
        System.setProperty(TEST_PROP.getKey(), "01234567");
        assertThat(TEST_PROP.getInt()).isEqualTo(342391);
    }

    @Test(expected = ConfigurationException.class)
    public void testInteger_empty()
    {
        System.setProperty(TEST_PROP.getKey(), "");
        TEST_PROP.getInt();
        fail("Expected ConfigurationException");
    }

    @Test(expected = NullPointerException.class)
    public void testInteger_null()
    {
        TEST_PROP.getInt();
        fail("Expected NullPointerException");
    }

    @Test
    public void testInteger_override_default()
    {
        assertThat(TEST_PROP.getInt(2345)).isEqualTo(2345);
        TEST_PROP.setInt(1234);
        assertThat(TEST_PROP.getInt(2345)).isEqualTo(1234);
    }

    @Test
    public void testLong()
    {
        System.setProperty(TEST_PROP.getKey(), "1234");
        assertThat(TEST_PROP.getLong()).isEqualTo(1234);
    }

    @Test(expected = ConfigurationException.class)
    public void testLong_empty()
    {
        System.setProperty(TEST_PROP.getKey(), "");
        TEST_PROP.getLong();
        fail("Expected ConfigurationException");
    }

    @Test(expected = NullPointerException.class)
    public void testLong_null()
    {
        TEST_PROP.getLong();
        fail("Expected NullPointerException");
    }

    @Test
    public void testLong_override_default()
    {
        assertThat(TEST_PROP.getLong(2345)).isEqualTo(2345);
        TEST_PROP.setLong(1234);
        assertThat(TEST_PROP.getLong(2345)).isEqualTo(1234);
    }

    @Test
    public void testDouble()
    {
        System.setProperty(TEST_PROP.getKey(), "1.567");
        assertThat(TEST_PROP.getDouble()).isEqualTo(1.567);
    }

    @Test(expected = ConfigurationException.class)
    public void testDouble_empty()
    {
        System.setProperty(TEST_PROP.getKey(), "");
        TEST_PROP.getDouble();
        fail("Expected ConfigurationException");
    }

    @Test(expected = NullPointerException.class)
    public void testDouble_null()
    {
        TEST_PROP.getDouble();
        fail("Expected NullPointerException");
    }

    @Test
    public void testDouble_override_default()
    {
        assertThat(TEST_PROP.getDouble(2.345)).isEqualTo(2.345);
        TEST_PROP.setDouble(1.234);
        assertThat(TEST_PROP.getDouble(2.345)).isEqualTo(1.234);
    }
}
