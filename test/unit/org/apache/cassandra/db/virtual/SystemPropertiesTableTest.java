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

package org.apache.cassandra.db.virtual;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

// checkstyle: suppress below 'blockSystemPropertyUsage'

public class SystemPropertiesTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final Map<String, String> ORIGINAL_ENV_MAP = System.getenv();
    private static final String TEST_PROP = "cassandra.SystemPropertiesTableTest";

    private SystemPropertiesTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void config()
    {
        table = new SystemPropertiesTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
        disablePreparedReuseForTest();
    }

    @Test
    public void testSelectAll() throws Throwable
    {
        ResultSet result = executeNet("SELECT * FROM vts.system_properties");

        for (Row r : result)
            Assert.assertEquals(System.getProperty(r.getString("name"), System.getenv(r.getString("name"))), r.getString("value"));
    }

    @Test
    public void testSelectPartition() throws Throwable
    {
        List<String> properties = Stream.concat(System.getProperties().stringPropertyNames().stream(),
                                                System.getenv().keySet().stream())
                                        .filter(name -> SystemPropertiesTable.isCassandraRelevant(name))
                                        .collect(Collectors.toList());

        for (String property : properties)
        {
            String q = "SELECT * FROM vts.system_properties WHERE name = '" + property + '\'';
            assertRowsNet(executeNet(q), new Object[] {property, System.getProperty(property, System.getenv(property))});
        }
    }

    @Test
    public void testSelectEmpty() throws Throwable
    {
        String q = "SELECT * FROM vts.system_properties WHERE name = 'EMPTY'";
        assertRowsNet(executeNet(q));
    }

    @Test
    public void testSelectProperty() throws Throwable
    {
        try
        {
            String value = "test_value";
            System.setProperty(TEST_PROP, value);
            String q = String.format("SELECT * FROM vts.system_properties WHERE name = '%s'", TEST_PROP);
            assertRowsNet(executeNet(q), new Object[] {TEST_PROP, value});
        }
        finally
        {
            System.clearProperty(TEST_PROP);
        }
    }

    @Test
    public void testSelectEnv() throws Throwable
    {
        try
        {
            String value = "test_value";
            addEnv(TEST_PROP, value);
            String q = String.format("SELECT * FROM vts.system_properties WHERE name = '%s'", TEST_PROP);
            assertRowsNet(executeNet(q), new Object[] {TEST_PROP, value});
        }
        finally
        {
            resetEnv();
        }
    }

    @Test
    public void testSelectPropertyOverEnv() throws Throwable
    {
        try
        {
            String value = "test_value";
            System.setProperty(TEST_PROP, value);
            addEnv(TEST_PROP, "wrong_value");
            String q = String.format("SELECT * FROM vts.system_properties WHERE name = '%s'", TEST_PROP);
            assertRowsNet(executeNet(q), new Object[] {TEST_PROP, value});
        }
        finally
        {
            System.clearProperty(TEST_PROP);
            resetEnv();
        }
    }

    private static void addEnv(String env, String value) throws ReflectiveOperationException
    {
        Map<String, String> envMap = Maps.newConcurrentMap();
        envMap.putAll(System.getenv());
        envMap.put(env, value);
        setEnv(envMap);
    }

    private static void resetEnv() throws ReflectiveOperationException
    {
        setEnv(ORIGINAL_ENV_MAP);
    }

    private static void setEnv(Map<String, String> newenv) throws ReflectiveOperationException
    {
        try
        {
            Class<?> cls = Class.forName("java.lang.ProcessEnvironment");
            Field field = cls.getDeclaredField("theEnvironment");
            field.setAccessible(true);
            Map<String, String> envMap = (Map<String, String>) field.get(null);
            envMap.clear();
            envMap.putAll(newenv);
            field = cls.getDeclaredField("theCaseInsensitiveEnvironment");
            field.setAccessible(true);
            envMap = (Map<String, String>) field.get(null);
            envMap.clear();
            envMap.putAll(newenv);
        }
        catch (NoSuchFieldException ignore)
        {
            Class[] classes = Collections.class.getDeclaredClasses();
            Map<String, String> envMap = System.getenv();
            for(Class cl : classes) {
                if("java.util.Collections$UnmodifiableMap".equals(cl.getName()))
                {
                    Field field = cl.getDeclaredField("m");
                    field.setAccessible(true);
                    Object obj = field.get(envMap);
                    envMap = (Map<String, String>) obj;
                    envMap.clear();
                    envMap.putAll(newenv);
                }
            }
        }
    }
}
