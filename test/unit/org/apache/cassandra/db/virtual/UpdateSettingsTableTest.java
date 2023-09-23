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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.ConfigFields;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.registry.ConfigurationRegistry;
import org.apache.cassandra.config.registry.TypeConverter;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.assertj.core.util.Streams;

import static org.apache.cassandra.db.virtual.SettingsTableTest.KS_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * The set of tests for {@link SettingsTable} for updating settings through virtual table. Since the Config class and
 * the {@link DatabaseDescriptor} are static, we need to return back changes to the original state after each update.
 */
public class UpdateSettingsTableTest extends CQLTester
{
    private static final List<String> updatableProperties = new ArrayList<>();
    private static final Map<Class<?>, Object[]> defaultTestValues = registerTestConfigurationValues();
    private ConfigurationRegistry registry;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void prepare()
    {
        // Creating a new instence will avoid calling listeners registered in the registry.
        registry = new ConfigurationRegistry(DatabaseDescriptor::getRawConfig);
        DatabaseDescriptor.applyConfigurationRegistryConstraints(registry);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(new SettingsTable(KS_NAME, registry))));
        Streams.stream(registry.keys())
               .filter(k -> registry.isWritable(k))
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
    public void testUpdateWithNull() throws Throwable
    {
        // Settings table called as apply column deletion. So, we need to test that we can delete a value.
        assertNotNull(registry.get(registry.type(ConfigFields.COORDINATOR_READ_SIZE_WARN_THRESHOLD), ConfigFields.COORDINATOR_READ_SIZE_WARN_THRESHOLD));
        updateConfigurationProperty(String.format("UPDATE %s.settings SET value = ? WHERE name = ?;", KS_NAME),
                                     ConfigFields.COORDINATOR_READ_SIZE_WARN_THRESHOLD, null);
    }

    @Test
    public void testUpdateConstraintViolation() throws Throwable
    {
        InvalidQueryException e = null;
        try
        {
            updateConfigurationProperty(String.format("UPDATE %s.settings SET value = ? WHERE name = ?;", KS_NAME),
                                        ConfigFields.PHI_CONVICT_THRESHOLD, 4.0);
        }
        catch (InvalidQueryException ex)
        {
            e = ex;
        }
        assertNotNull(e);
        assertEquals("Invalid update request; cause: 'Error updating property 'phi_convict_threshold'; cause: phi_convict_threshold must be between 5 and 16, but was 4.0'", e.getMessage());
    }

    @Test
    public void testInsertSettings() throws Throwable
    {
        for (String propertyName : updatableProperties)
            doUpdateSettingAndRevertBack(String.format("INSERT INTO %s.settings (value, name) VALUES (?, ?);", KS_NAME), propertyName);
    }

    private void doUpdateSettingAndRevertBack(String statement, String propertyName) throws Throwable
    {
        Object oldValue = registry.get(registry.type(propertyName), propertyName);
        Object[] testValues = defaultTestValues.get(registry.type(propertyName));
        assertNotNull(String.format("No test values found for setting '%s' with type '%s'",
                                           propertyName, registry.type(propertyName)), testValues);
        Object value = getNextValue(defaultTestValues.get(registry.type(propertyName)), oldValue);
        updateConfigurationProperty(statement, propertyName, value);
        updateConfigurationProperty(statement, propertyName, oldValue);
    }

    private void updateConfigurationProperty(String statement, String propertyName, @Nullable Object value) throws Throwable
    {
        assertRowsNet(executeNet(statement, TypeConverter.DEFAULT.convertNullable(value), propertyName));
        assertEquals(value, registry.get(registry.type(propertyName), propertyName));
        assertRowsNet(executeNet(String.format("SELECT * FROM %s.settings WHERE name = ?;", KS_NAME), propertyName), new Object[]{ propertyName, TypeConverter.DEFAULT.convertNullable((value)) });
    }

    private static Object getNextValue(Object[] values, Object currentValue)
    {
        if (currentValue == null)
            return values[0];

        for (int i = 0; i < values.length; i++)
        {
            if (values[i].toString().equals(currentValue.toString()))
                continue;

            return values[i];
        }
        fail("No next value found for " + currentValue);
        return null;
    }

    private static Map<Class<?>, Object[]> registerTestConfigurationValues()
    {
        return ImmutableMap.<Class<?>, Object[]>builder()
                           .put(Boolean.class, new Object[]{ true, false })
                           .put(boolean.class, new Object[]{ true, false })
                           .put(Double.class, new Object[]{ 8.0, 9.0 })
                           .put(double.class, new Object[]{ 8.0, 9.0 })
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
                           .put(DataStorageSpec.LongBytesBound.class, new Object[]{ new DataStorageSpec.LongBytesBound("100B"), new DataStorageSpec.LongBytesBound("200B") })
                           .put(DataStorageSpec.IntBytesBound.class, new Object[]{ new DataStorageSpec.IntBytesBound("100B"), new DataStorageSpec.IntBytesBound("200B") })
                           .put(DataStorageSpec.IntKibibytesBound.class, new Object[]{ new DataStorageSpec.IntKibibytesBound("100KiB"), new DataStorageSpec.IntKibibytesBound("200KiB") })
                           .put(DataStorageSpec.LongMebibytesBound.class, new Object[]{ new DataStorageSpec.LongMebibytesBound("100MiB"), new DataStorageSpec.LongMebibytesBound("200MiB") })
                           .put(DataStorageSpec.IntMebibytesBound.class, new Object[]{ new DataStorageSpec.IntMebibytesBound("100MiB"), new DataStorageSpec.IntMebibytesBound("200MiB") })
                           .put(ConsistencyLevel.class, new Object[]{ ConsistencyLevel.ONE, ConsistencyLevel.TWO })
                           .build();
    }
}
