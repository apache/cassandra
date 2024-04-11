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

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.guardrails.GuardrailTester;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.db.virtual.GuardrailThresholdsTable.NAME_COLUMN;
import static org.apache.cassandra.db.virtual.GuardrailThresholdsTable.TABLE_NAME;
import static org.apache.cassandra.db.virtual.GuardrailThresholdsTable.VALUE_COLUMN;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class GuardrailThresholdsTableTest extends GuardrailTester
{
    private static final String KS_NAME = "vts";

    @BeforeClass
    public static void before()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, List.of(new GuardrailThresholdsTable(KS_NAME))));
    }

    @Test
    public void testAllThresholdGuardrailsRegistered()
    {
        List<GuardrailRow> guardrailRows = getGuardrailRows();
        assertEquals(Guardrails.getThresholdGuardails().size(), guardrailRows.size());

        for (GuardrailRow row : guardrailRows)
        {
            Pair<BiConsumer<Number, Number>, Pair<Supplier<Number>, Supplier<Number>>> mapping = Guardrails.getThresholdGuardails().get(row.name);
            assertNotNull(mapping);

            assertEquals(mapping.right.left.get().longValue(), row.warn);
            assertEquals(mapping.right.right.get().longValue(), row.fail);
        }
    }

    @Test
    public void testAllTupleElementsRequired()
    {
        assertThatThrownBy(() -> execute(format("update %s.%s set value = (null, 20) where name = 'tables'", KS_NAME, TABLE_NAME)))
        .describedAs("It should not be possible to set one threshold as null")
        .hasMessageContaining("Both elements of a tuple must not be null.")
        .isInstanceOf(InvalidRequestException.class);

        assertThatThrownBy(() -> execute(format("update %s.%s set value = (20, null) where name = 'tables'", KS_NAME, TABLE_NAME)))
        .describedAs("It should not be possible to set either threshold as null")
        .hasMessageContaining("Both elements of a tuple must not be null.")
        .isInstanceOf(InvalidRequestException.class);

        assertThatThrownBy(() -> execute(format("update %s.%s set value = (null, null) where name = 'tables'", KS_NAME, TABLE_NAME)))
        .describedAs("It should not be possible to set either threshold as null")
        .hasMessageContaining("Both elements of a tuple must not be null.")
        .isInstanceOf(InvalidRequestException.class);

        assertThatThrownBy(() -> execute(format("update %s.%s set value = null where name = 'tables'", KS_NAME, TABLE_NAME)))
        .describedAs("It should not be possible to set either threshold as null")
        .hasMessageContaining(format("Column deletion is not supported by table %s.%s", KS_NAME, TABLE_NAME))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testThresholdsCantViolateEachOther()
    {
        assertThatThrownBy(() -> execute(format("update %s.%s set value = (50, 30) where name = 'tables'", KS_NAME, TABLE_NAME)))
        .describedAs("warn threshold can not be bigger than fail threshold")
        .hasMessageContaining("The warn threshold 50 for tables_warn_threshold should be lower than the fail threshold 30")
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testUpdatingMissingGuardrail()
    {
        assertThatThrownBy(() -> execute(format("update %s.%s set value = (50, 30) where name = 'doesnotexist'", KS_NAME, TABLE_NAME)))
        .describedAs("it should not be possible to set thresholds for non-existing guardrail")
        .hasMessageContaining("there is no such guardrail with name 'doesnotexist'")
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testChangedThresholds()
    {
        assertEquals(-1, Guardrails.instance.getTablesWarnThreshold());
        assertEquals(-1, Guardrails.instance.getTablesFailThreshold());

        execute(format("update %s.%s set value = (30, 50) where name = 'tables'", KS_NAME, TABLE_NAME));

        assertEquals(30, Guardrails.instance.getTablesWarnThreshold());
        assertEquals(50, Guardrails.instance.getTablesFailThreshold());
    }

    @Test
    public void testDurationBoundThresholdGuardrail()
    {
        assertNull(Guardrails.instance.getMinimumTimestampWarnThreshold());
        assertNull(Guardrails.instance.getMinimumTimestampFailThreshold());

        execute(format("update %s.%s set value = (600000000, 1200000000) where name = 'minimum_timestamp'", KS_NAME, TABLE_NAME));

        assertEquals("600000000us", Guardrails.instance.getMinimumTimestampWarnThreshold());
        assertEquals("1200000000us", Guardrails.instance.getMinimumTimestampFailThreshold());

        Guardrails.instance.setMinimumTimestampThreshold("9m", "30m");

        assertEquals("9m", Guardrails.instance.getMinimumTimestampWarnThreshold());
        assertEquals("30m", Guardrails.instance.getMinimumTimestampFailThreshold());

        GuardrailRow row = getGuardrailRow(format("select * from %s.%s where name = 'minimum_timestamp'", KS_NAME, TABLE_NAME)).get(0);
        assertEquals("minimum_timestamp", row.name);
        assertEquals(540000000, row.warn);
        assertEquals(1800000000, row.fail);

        execute(format("update %s.%s set value = (-1, 1800000000) where name = 'minimum_timestamp'", KS_NAME, TABLE_NAME));
        assertNull(Guardrails.instance.getMinimumTimestampWarnThreshold());
        assertEquals("30m", Guardrails.instance.getMinimumTimestampFailThreshold());

        execute(format("update %s.%s set value = (-1, -1) where name = 'minimum_timestamp'", KS_NAME, TABLE_NAME));
        assertNull(Guardrails.instance.getMinimumTimestampWarnThreshold());
        assertNull(Guardrails.instance.getMinimumTimestampFailThreshold());
    }

    @Test
    public void testSizeBoundThresholdGuardrail()
    {
        assertNull(Guardrails.instance.getCollectionSizeWarnThreshold());
        assertNull(Guardrails.instance.getCollectionSizeFailThreshold());

        execute(format("update %s.%s set value = (65535, 131072) where name = 'collection_size'", KS_NAME, TABLE_NAME));

        assertEquals("65535B", Guardrails.instance.getCollectionSizeWarnThreshold());
        assertEquals("131072B", Guardrails.instance.getCollectionSizeFailThreshold());

        Guardrails.instance.setCollectionSizeThreshold("128KiB", "256KiB");

        assertEquals("128KiB", Guardrails.instance.getCollectionSizeWarnThreshold());
        assertEquals("256KiB", Guardrails.instance.getCollectionSizeFailThreshold());

        GuardrailRow row = getGuardrailRow(format("select * from %s.%s where name = 'collection_size'", KS_NAME, TABLE_NAME)).get(0);
        assertEquals("collection_size", row.name);
        assertEquals(131072, row.warn);
        assertEquals(262144, row.fail);

        execute(format("update %s.%s set value = (-1, 262144) where name = 'collection_size'", KS_NAME, TABLE_NAME));
        assertNull(Guardrails.instance.getCollectionSizeWarnThreshold());
        assertEquals("256KiB", Guardrails.instance.getCollectionSizeFailThreshold());

        execute(format("update %s.%s set value = (-1, -1) where name = 'collection_size'", KS_NAME, TABLE_NAME));
        assertNull(Guardrails.instance.getCollectionSizeWarnThreshold());
        assertNull(Guardrails.instance.getCollectionSizeFailThreshold());
    }

    private List<GuardrailRow> getGuardrailRow(String query)
    {
        UntypedResultSet result = execute(query);

        return result.stream().map(row -> {
            String name = row.getString(NAME_COLUMN);
            List<Object> tuple = row.getTuple(VALUE_COLUMN, LongType.instance, LongType.instance);
            return new GuardrailRow(name, (Long) tuple.get(0), (Long) tuple.get(1));
        }).collect(toList());
    }

    private List<GuardrailRow> getGuardrailRows()
    {
        return getGuardrailRow(format("select * from %s.%s", KS_NAME, TABLE_NAME));
    }

    private static class GuardrailRow
    {
        public final String name;
        public final long warn;
        public final long fail;

        public GuardrailRow(String name, long warn, long fail)
        {
            this.name = name;
            this.warn = warn;
            this.fail = fail;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GuardrailRow that = (GuardrailRow) o;
            return warn == that.warn && fail == that.fail && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, warn, fail);
        }
    }
}
