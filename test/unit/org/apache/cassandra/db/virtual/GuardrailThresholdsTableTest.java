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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.guardrails.GuardrailTester;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.GuardrailsProxy;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.db.virtual.GuardrailThresholdsTable.NAME_COLUMN;
import static org.apache.cassandra.db.virtual.GuardrailThresholdsTable.TABLE_NAME;
import static org.apache.cassandra.db.virtual.GuardrailThresholdsTable.VALUE_COLUMN;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GuardrailThresholdsTableTest extends GuardrailTester
{
    private static final String KS_NAME = "vts";

    private static GuardrailsProxy cache;

    @BeforeClass
    public static void before()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, List.of(new GuardrailThresholdsTable(KS_NAME))));
        GuardrailsProxy.instance.serverInitialisation();
        cache = GuardrailsProxy.instance;
    }

    @After
    public void reset()
    {
        Guardrails.instance.setTablesThreshold(-1, -1);
        Guardrails.instance.setMinimumTimestampThreshold(null, null);
        Guardrails.instance.setCollectionSizeThreshold(null, null);
    }

    @Test
    public void testAllThresholdGuardrailsRegistered()
    {
        List<GuardrailRow> guardrailRows = getGuardrailRows();
        assertEquals(cache.getThresholdsGetters().size(), guardrailRows.size());

        for (GuardrailRow row : guardrailRows)
        {
            List<Method> getters = cache.getThresholdsGetters().get(row.name);
            Object failValue = cache.invoke(getters.get(0));
            Object warnValue = cache.invoke(getters.get(1));
            assertEquals(failValue == null ? null : failValue.toString(), "".equals(row.fail) ? null : row.fail);
            assertEquals(warnValue == null ? null : warnValue.toString(), "".equals(row.warn) ? null : row.warn);
        }
    }

    @Test
    public void testAllTupleElementsRequired()
    {
        assertThatThrownBy(() -> execute(format("update %s.%s set value = null where name = 'tables_threshold'", KS_NAME, TABLE_NAME)))
        .describedAs("It should not be possible to set value of threshold as null")
        .hasMessageContaining(format("Column deletion is not supported by table %s.%s", KS_NAME, TABLE_NAME))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testThresholdsCantViolateEachOther()
    {
        assertThatThrownBy(() -> execute(format("update %s.%s set value = ('30', '50') where name = 'tables_threshold'", KS_NAME, TABLE_NAME)))
        .describedAs("warn threshold can not be bigger than fail threshold")
        .hasMessageContaining("The warn threshold 50 for tables_warn_threshold should be lower than the fail threshold 30")
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testUpdatingMissingGuardrail()
    {
        assertThatThrownBy(() -> execute(format("update %s.%s set value = ('50', '30') where name = 'doesnotexist'", KS_NAME, TABLE_NAME)))
        .describedAs("it should not be possible to set thresholds for non-existing guardrail")
        .hasMessageContaining("there is no associated setter for guardrail with name doesnotexist")
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testSelectingMissingGuardrail()
    {
        assertThatThrownBy(() -> execute(format("select value from %s.%s where name = 'doesnotexist'", KS_NAME, TABLE_NAME)))
        .describedAs("it should not be possible to select thresholds for non-existing guardrail")
        .hasMessageContaining("there is no associated getter for guardrail with name doesnotexist")
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testChangedThresholds()
    {
        execute(format("update %s.%s set value = ('50', '30') where name = 'tables_threshold'", KS_NAME, TABLE_NAME));
        assertEquals(30, Guardrails.instance.getTablesWarnThreshold());
        assertEquals(30, Guardrails.instance.getTablesWarnThreshold());
    }

    @Test
    public void testDurationBoundThresholdGuardrail()
    {
        assertNull(Guardrails.instance.getMinimumTimestampWarnThreshold());
        assertNull(Guardrails.instance.getMinimumTimestampFailThreshold());

        execute(format("update %s.%s set value = ('1200000000us', '1200000000us') where name = 'minimum_timestamp_threshold'", KS_NAME, TABLE_NAME));

        assertEquals("1200000000us", Guardrails.instance.getMinimumTimestampFailThreshold());
        assertEquals("1200000000us", Guardrails.instance.getMinimumTimestampWarnThreshold());

        Guardrails.instance.setMinimumTimestampThreshold("9m", "30m");

        assertEquals("9m", Guardrails.instance.getMinimumTimestampWarnThreshold());
        assertEquals("30m", Guardrails.instance.getMinimumTimestampFailThreshold());

        GuardrailRow row = getGuardrailRow(format("select * from %s.%s where name = 'minimum_timestamp_threshold'", KS_NAME, TABLE_NAME)).get(0);
        assertEquals("minimum_timestamp_threshold", row.name);
        assertEquals("9m", row.warn);
        assertEquals("30m", row.fail);

        execute(format("update %s.%s set value = ('30m', null) where name = 'minimum_timestamp_threshold'", KS_NAME, TABLE_NAME));
        assertNull(Guardrails.instance.getMinimumTimestampWarnThreshold());
        assertEquals("30m", Guardrails.instance.getMinimumTimestampFailThreshold());
        assertNull(Guardrails.instance.getMinimumTimestampWarnThreshold());

        execute(format("update %s.%s set value = (null, null) where name = 'minimum_timestamp_threshold'", KS_NAME, TABLE_NAME));
        assertNull(Guardrails.instance.getMinimumTimestampWarnThreshold());
        assertNull(Guardrails.instance.getMinimumTimestampFailThreshold());
    }

    @Test
    public void testSizeBoundThresholdGuardrail()
    {
        execute(format("update %s.%s set value = ('131072B', '65535B') where name = 'collection_size_threshold'", KS_NAME, TABLE_NAME));

        assertEquals("131072B", Guardrails.instance.getCollectionSizeFailThreshold());
        assertEquals("65535B", Guardrails.instance.getCollectionSizeWarnThreshold());

        Guardrails.instance.setCollectionSizeThreshold("128KiB", "256KiB");

        assertEquals("128KiB", Guardrails.instance.getCollectionSizeWarnThreshold());
        assertEquals("256KiB", Guardrails.instance.getCollectionSizeFailThreshold());

        GuardrailRow row = getGuardrailRow(format("select * from %s.%s where name = 'collection_size_threshold'", KS_NAME, TABLE_NAME)).get(0);
        assertEquals("collection_size_threshold", row.name);
        assertEquals("128KiB", row.warn);
        assertEquals("256KiB", row.fail);

        execute(format("update %s.%s set value = ('262144B', null) where name = 'collection_size_threshold'", KS_NAME, TABLE_NAME));
        assertNull(Guardrails.instance.getCollectionSizeWarnThreshold());
        assertEquals("256KiB", Guardrails.instance.getCollectionSizeFailThreshold());

        execute(format("update %s.%s set value = (null, null) where name = 'collection_size_threshold'", KS_NAME, TABLE_NAME));
        assertNull(Guardrails.instance.getCollectionSizeWarnThreshold());
        assertNull(Guardrails.instance.getCollectionSizeFailThreshold());

        execute(format("update %s.%s set value = ('131072B', '65535B') where name = 'collection_size_threshold'", KS_NAME, TABLE_NAME));
        assertEquals("131072B", Guardrails.instance.getCollectionSizeFailThreshold());
        assertEquals("65535B", Guardrails.instance.getCollectionSizeWarnThreshold());
        execute(format("update %s.%s set value = ('', '') where name = 'collection_size_threshold'", KS_NAME, TABLE_NAME));
        assertNull(Guardrails.instance.getCollectionSizeWarnThreshold());
        assertNull(Guardrails.instance.getCollectionSizeFailThreshold());

    }

    private List<GuardrailRow> getGuardrailRow(String query)
    {
        UntypedResultSet result = execute(query);

        return result.stream().map(row -> {
            String name = row.getString(NAME_COLUMN);
            List<Object> tuple = row.getTuple(VALUE_COLUMN, UTF8Type.instance, UTF8Type.instance);
            return new GuardrailRow(name, (String) tuple.get(0), (String) tuple.get(1));
        }).collect(toList());
    }

    private List<GuardrailRow> getGuardrailRows()
    {
        return getGuardrailRow(format("select * from %s.%s", KS_NAME, TABLE_NAME));
    }

    private static class GuardrailRow
    {
        public final String name;
        public final String fail;
        public final String warn;

        public GuardrailRow(String name, String fail, String warn)
        {
            this.name = name;
            this.fail = fail;
            this.warn = warn;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GuardrailRow that = (GuardrailRow) o;
            return warn.equals(that.warn) && fail.equals(that.fail) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, warn, fail);
        }
    }
}
