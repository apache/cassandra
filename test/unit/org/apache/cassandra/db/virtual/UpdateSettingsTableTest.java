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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.registry.ConfigurationRegistry;
import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.config.ConfigFields.CDC_BLOCK_WRITES;
import static org.apache.cassandra.config.ConfigFields.REPAIR_REQUEST_TIMEOUT;
import static org.apache.cassandra.db.virtual.SettingsTableTest.KS_NAME;
import static org.junit.Assert.assertEquals;

/**
 * The set of tests for {@link SettingsTable} for updating settings through virtual table. Since the Config class and
 * the {@link DatabaseDescriptor} are static, we need to return back changes to the original state after each update.
 */
public class UpdateSettingsTableTest extends CQLTester
{
    private static final List<String> updatableProperties = new ArrayList<>();
    private static final Config config = DatabaseDescriptor.getRawConfig();
    private static final Map<Class<?>, Object[]> defaultTestValues = registerTestConfigurationValues();
    private ConfigurationRegistry registry;

    @BeforeClass
    public static void setUpClass()
    {
//        ConfigurationRegistry.PROPERTY_SETTERS_LIST.forEach(w -> w.walk(new SettingSettersCollector(updatableSettings)));
        CQLTester.setUpClass();
    }

    @Before
    public void prepare()
    {
        // Creating a new instence will avoid calling listeners registered in the registry.
        List<String> allowedProperties = Arrays.asList(REPAIR_REQUEST_TIMEOUT, CDC_BLOCK_WRITES);
        registry = new ConfigurationRegistry();
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(new SettingsTable(KS_NAME, config, registry))));
        registry.keys()
                .stream()
                .filter(k -> registry.isWritable(k))
                .filter(allowedProperties::contains)
                .forEach(updatableProperties::add);
        disablePreparedReuseForTest();
    }

    @Test
    public void testUpdateSettings() throws Throwable
    {
        for (String propertyName : updatableProperties)
            doUpdateSettingAndRevertBack(String.format("UPDATE %s.settings SET value = ? WHERE name = ?;", KS_NAME), propertyName);
    }

    @Test
    public void testInsertSettings() throws Throwable
    {
        for (String propertyName : updatableProperties)
            doUpdateSettingAndRevertBack(String.format("INSERT INTO %s.settings (value, name) VALUES (?, ?);", KS_NAME), propertyName);
    }

    private void doUpdateSettingAndRevertBack(String statement, String propertyName) throws Throwable
    {
        Object oldValue = registry.get(propertyName);
        Object[] testValues = defaultTestValues.get(registry.getType(propertyName));
        Assert.assertNotNull(String.format("No test values found for setting '%s' with type '%s'",
                                           propertyName, registry.getType(propertyName)), testValues);
        Object value = getNextValue(defaultTestValues.get(registry.getType(propertyName)), oldValue);
        updateConfigurationProperty(statement, propertyName, value);
        updateConfigurationProperty(statement, propertyName, oldValue);
    }

    private void updateConfigurationProperty(String statement, String propertyName, Object value) throws Throwable
    {
        assertRowsNet(executeNet(statement, stringValue(value), propertyName));
        assertEquals(value, registry.get(propertyName));
        assertRowsNet(executeNet(String.format("SELECT * FROM %s.settings WHERE name = ?;", KS_NAME), propertyName), new Object[]{ propertyName, stringValue(value) });
    }

    private static Object getNextValue(Object[] values, Object currentValue)
    {
        if (currentValue == null)
            return values[0];

        for (int i = 0; i < values.length; i++)
        {
            if (values[i].toString().equals(currentValue.toString()))
                return values[(i + 1) % values.length];
        }
        return null;
    }

    private static @Nullable String stringValue(Object obj)
    {
        return obj == null ? null : obj.toString();
    }

    private static Map<Class<?>, Object[]> registerTestConfigurationValues()
    {
        return ImmutableMap.<Class<?>, Object[]>builder()
                           .put(Boolean.class, new Object[]{ true, false })
                           .put(boolean.class, new Object[]{ true, false })
                           .put(Integer.class, new Object[]{ 1, 2 })
                           .put(int.class, new Object[]{ 1, 2 })
                           .put(Long.class, new Object[]{ 1L, 2L })
                           .put(long.class, new Object[]{ 1L, 2L })
                           .put(String.class, new Object[]{ "TestString1", "TestString2" })
                           .put(DurationSpec.LongNanosecondsBound.class, new Object[]{ new DurationSpec.LongNanosecondsBound("100ns"), new DurationSpec.LongNanosecondsBound("200ns") })
                           .put(DurationSpec.LongMillisecondsBound.class, new Object[]{ new DurationSpec.LongMillisecondsBound("100ms"), new DurationSpec.LongMillisecondsBound("200ms") })
                           .put(DurationSpec.LongSecondsBound.class, new Object[]{ new DurationSpec.LongSecondsBound("1s"), new DurationSpec.LongSecondsBound("2s") })
                           .put(DurationSpec.IntMinutesBound.class, new Object[]{ new DurationSpec.IntMinutesBound("1m"), new DurationSpec.IntMinutesBound("2m") })
                           .put(DurationSpec.IntSecondsBound.class, new Object[]{ new DurationSpec.IntSecondsBound("1s"), new DurationSpec.IntSecondsBound("2s") })
                           .put(DurationSpec.IntMillisecondsBound.class, new Object[]{ new DurationSpec.IntMillisecondsBound("100ms"), new DurationSpec.IntMillisecondsBound("200ms") })
                           .build();
    }
}
