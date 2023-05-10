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
import java.util.HashMap;
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
import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.TypeConverter;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;

import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.MEBIBYTES;
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
    private static final Map<String, Class<?>> propertyTypes = new HashMap<>();
    private static final Map<Class<?>, Object[]> defaultTestValues = registerTestConfigurationValues();

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.accept((key, type, readOnly) -> {
            propertyTypes.put(key, type);
            if (!readOnly)
                updatableProperties.add(key);
        });
    }

    @Before
    public void prepare()
    {
        // Creating a new instence will avoid calling listeners registered in the registry.
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(new SettingsTable(KS_NAME))));
        disablePreparedReuseForTest();
    }

    @Test
    public void testUpdateAllSettings() throws Throwable
    {
        for (String propertyName : updatableProperties)
            doUpdateSettingAndRevertBack(String.format("UPDATE %s.settings SET value = ? WHERE name = ?;", KS_NAME), propertyName);
    }

    @Test
    public void testUpdateRepairSessionSpaceToNull() throws Throwable
    {
        String nonDefaultValue = new DataStorageSpec.IntMebibytesBound(10L, MEBIBYTES).toString();
        assertRowsNet(executeNet(String.format("UPDATE %s.settings SET value = ? WHERE name = ?;", KS_NAME),
                                 nonDefaultValue, ConfigFields.REPAIR_SESSION_SPACE));
        assertEquals(nonDefaultValue, DatabaseDescriptor.getStringProperty(ConfigFields.REPAIR_SESSION_SPACE));
        // Try to use null as a value.
        String expectedIfNullGiven = new DataStorageSpec.IntMebibytesBound(DatabaseDescriptor.SPACE_UPPER_BOUND_MB).toString();
        assertRowsNet(executeNet(String.format("UPDATE %s.settings SET value = ? WHERE name = ?;", KS_NAME),
                                 null, ConfigFields.REPAIR_SESSION_SPACE));
        assertRowsNet(executeNet(String.format("SELECT * FROM %s.settings WHERE name = ?;", KS_NAME), ConfigFields.REPAIR_SESSION_SPACE),
                      new Object[]{ ConfigFields.REPAIR_SESSION_SPACE, expectedIfNullGiven });
        String sessionSpace = DatabaseDescriptor.getStringProperty(ConfigFields.REPAIR_SESSION_SPACE);
        assertNotNull(sessionSpace);
        assertEquals(expectedIfNullGiven, sessionSpace);
        assertNotNull(DatabaseDescriptor.getRawConfig().repair_session_space);
        assertEquals(expectedIfNullGiven, DatabaseDescriptor.getRawConfig().repair_session_space.toString());
    }

    @Test
    public void testUpdateSettingsValidationFail() throws Throwable
    {
        InvalidQueryException e = null;
        try
        {
            updateConfigurationProperty(String.format("UPDATE %s.settings SET value = ? WHERE name = ?;", KS_NAME),
                                        ConfigFields.STREAM_THROUGHPUT_OUTBOUND, Integer.MAX_VALUE + 1L);
        }
        catch (InvalidQueryException ex)
        {
            e = ex;
        }
        assertNotNull(e);
        assertEquals("Invalid update request for property 'stream_throughput_outbound'. Invalid data rate: " +
                     "2147483648 Accepted units: MiB/s, KiB/s, B/s where case matters and only non-negative values are valid",
                     e.getMessage());
    }

    @Test
    public void testInsertSettingsNewValues() throws Throwable
    {
        for (String propertyName : updatableProperties)
            doUpdateSettingAndRevertBack(String.format("INSERT INTO %s.settings (value, name) VALUES (?, ?);", KS_NAME), propertyName);
    }

    private void doUpdateSettingAndRevertBack(String statement, String propertyName) throws Throwable
    {
        Object oldValue = DatabaseDescriptor.getProperty(propertyName);
        Object[] testValues = defaultTestValues.get(propertyTypes.get(propertyName));
        assertNotNull(String.format("No test values found for setting '%s' with type '%s'",
                                    propertyName, propertyTypes.get(propertyName)), testValues);
        Object value = getNextValue(testValues, oldValue);
        updateConfigurationProperty(statement, propertyName, value);
        updateConfigurationProperty(statement, propertyName, oldValue);
    }

    private void updateConfigurationProperty(String statement, String propertyName, @Nullable Object value) throws Throwable
    {
        assertRowsNet(executeNet(statement, TypeConverter.TO_STRING.convertNullable(value), propertyName));
        assertEquals(value, DatabaseDescriptor.getProperty(propertyName));
        assertRowsNet(executeNet(String.format("SELECT * FROM %s.settings WHERE name = ?;", KS_NAME), propertyName), new Object[]{ propertyName, TypeConverter.TO_STRING.convertNullable((value)) });
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
                           .put(DataRateSpec.LongBytesPerSecondBound.class, new Object[]{ new DataRateSpec.LongBytesPerSecondBound("63MiB/s"), new DataRateSpec.LongBytesPerSecondBound("62MiB/s") })
                           .put(ConsistencyLevel.class, new Object[]{ ConsistencyLevel.ONE, ConsistencyLevel.TWO })
                           .build();
    }
}
