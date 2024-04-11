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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.guardrails.GuardrailTester;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.db.virtual.GuardrailEnableFlagsTable.NAME_COLUMN;
import static org.apache.cassandra.db.virtual.GuardrailEnableFlagsTable.TABLE_NAME;
import static org.apache.cassandra.db.virtual.GuardrailEnableFlagsTable.VALUE_COLUMN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GuardrailEnableFlagsTableTest extends GuardrailTester
{
    private static final String KS_NAME = "vts";

    @BeforeClass
    public static void before()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, List.of(new GuardrailEnableFlagsTable(KS_NAME))));
    }

    @Test
    public void testAllEnableGuardrailsRegistered()
    {
        List<GuardrailRow> guardrailRows = getGuardrailRows();
        assertEquals(Guardrails.getFlagGuardrails().size(), guardrailRows.size());

        for (GuardrailRow row : guardrailRows)
        {
            Pair<Consumer<Boolean>, BooleanSupplier> mapping = Guardrails.getFlagGuardrails().get(row.name);
            assertNotNull(mapping);

            assertEquals(mapping.right.getAsBoolean(), row.value);
        }
    }

    @Test
    public void testAllGuardrailsAreUpdateable()
    {
        List<GuardrailRow> originalState = getGuardrailRows();

        try
        {
            disableAllGuardrails();

            for (GuardrailRow row : getGuardrailRows())
                assertFalse(format("it was not possbile to set %s guardrail to 'false'", row.name), row.value);

            enableAllGuardrails();

            for (GuardrailRow row : getGuardrailRows())
                assertTrue(format("it was not possbile to set %s guardrail to 'true'", row.name), row.value);
        }
        finally
        {
            for (GuardrailRow row : originalState)
                setGuardrail(row.name, row.value);

            assertThat(getGuardrailRows()).containsExactlyInAnyOrderElementsOf(originalState);
        }
    }

    @Test
    public void testUpdatingMissingGuardrail()
    {
        assertThatThrownBy(() -> execute(format("update %s.%s set value = false where name = 'doesnotexist'", KS_NAME, TABLE_NAME)))
        .describedAs("it should not be possible to set thresholds for non-existing guardrail")
        .hasMessageContaining("there is no such guardrail with name 'doesnotexist'")
        .isInstanceOf(InvalidRequestException.class);
    }

    private void setGuardrail(String name, boolean value)
    {
        execute(format("update %s.%s set value = %s where name = '%s'", KS_NAME, TABLE_NAME, value, name));
    }

    private void disableAllGuardrails()
    {
        for (String guardrailName : Guardrails.getFlagGuardrails().keySet())
            execute(format("update %s.%s set value = false where name = '%s'", KS_NAME, TABLE_NAME, guardrailName));
    }

    private void enableAllGuardrails()
    {
        for (String guardrailName : Guardrails.getFlagGuardrails().keySet())
            execute(format("update %s.%s set value = true where name = '%s'", KS_NAME, TABLE_NAME, guardrailName));
    }

    private List<GuardrailRow> getGuardrailRows()
    {
        UntypedResultSet result = execute(format("select * from %s.%s", KS_NAME, TABLE_NAME));

        return result.stream().map(row -> {
            String name = row.getString(NAME_COLUMN);
            boolean value = row.getBoolean(VALUE_COLUMN);
            return new GuardrailRow(name, value);
        }).collect(toList());
    }

    private static class GuardrailRow
    {
        public final String name;
        public final boolean value;

        public GuardrailRow(String name, boolean value)
        {
            this.name = name;
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GuardrailRow that = (GuardrailRow) o;
            return value == that.value && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, value);
        }
    }
}
