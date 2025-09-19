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
import java.util.Set;

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
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.virtual.GuardrailEnableFlagsTable.NAME_COLUMN;
import static org.apache.cassandra.db.virtual.GuardrailValuesTable.TABLE_NAME;
import static org.apache.cassandra.db.virtual.GuardrailValuesTable.VALUE_COLUMN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GuardrailValuesTableTest extends GuardrailTester
{
    private static final String KS_NAME = "vts";

    private static GuardrailsProxy cache;

    @BeforeClass
    public static void before()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, List.of(new GuardrailValuesTable(KS_NAME))));
        GuardrailsProxy.instance.serverInitialisation();
        cache = GuardrailsProxy.instance;
    }

    @Test
    public void testGuardrails()
    {
        try
        {
            verifyTableContent();

            Set<String> warned = Set.of(EACH_QUORUM.name(), ALL.name());
            Set<String> disallowed = Set.of(QUORUM.name());
            Guardrails.instance.setReadConsistencyLevelsWarned(warned);
            Guardrails.instance.setReadConsistencyLevelsDisallowed(disallowed);

            verifyTableContent();

            Guardrails.instance.setWriteConsistencyLevelsWarned(warned);
            Guardrails.instance.setWriteConsistencyLevelsDisallowed(disallowed);

            verifyTableContent();

            Set<String> propertiesDisallowed = Set.of("crc_check_chance", "min_index_interval");
            Set<String> propertiesWarned = Set.of("extensions");
            Set<String> propertiesIgnored = Set.of("caching", "read_repair");

            Guardrails.instance.setTablePropertiesDisallowed(propertiesDisallowed);
            Guardrails.instance.setTablePropertiesWarned(propertiesWarned);
            Guardrails.instance.setTablePropertiesIgnored(propertiesIgnored);

            verifyTableContent();

            clearProperties("table_properties_disallowed");
            clearProperties("table_properties_warned");
            clearProperties("table_properties_ignored");

            update("table_properties_disallowed", propertiesDisallowed);
            update("table_properties_warned", propertiesWarned);
            update("table_properties_ignored", propertiesIgnored);

            GuardrailRow tablePropertiesDisallowedRow = getGuardrailRow("table_properties_disallowed");
            GuardrailRow tablePropertiesWarnedRow = getGuardrailRow("table_properties_warned");
            GuardrailRow tablePropertiesIgnoredRow = getGuardrailRow("table_properties_ignored");
            assertThat(tablePropertiesDisallowedRow.value).isNotEmpty();
            assertThat(tablePropertiesWarnedRow.value).isNotEmpty();
            assertThat(tablePropertiesIgnoredRow.value).isNotEmpty();

            assertThat(tablePropertiesDisallowedRow.value).containsExactlyInAnyOrderElementsOf(propertiesDisallowed);
            assertThat(tablePropertiesWarnedRow.value).containsExactlyInAnyOrderElementsOf(propertiesWarned);
            assertThat(tablePropertiesIgnoredRow.value).containsExactlyInAnyOrderElementsOf(propertiesIgnored);
        }
        finally
        {
            clearProperties();
        }
    }

    @Test
    public void testInvalidValuesForConsistencyRule()
    {
        Set<String> values = Set.of("NONEXISTINGLEVEL");
        assertThatThrownBy(() -> update("read_consistency_levels_warned", values))
        .hasMessageContaining("No enum constant org.apache.cassandra.db.ConsistencyLevel.NONEXISTINGLEVEL")
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testUpdatingMissingGuardrail()
    {
        assertThatThrownBy(() -> execute(format("update %s.%s set value = {'abc'} where name = 'doesnotexist'", KS_NAME, TABLE_NAME)))
        .describedAs("it should not be possible to set thresholds for non-existing guardrail")
        .hasMessageContaining("there is no associated setter for guardrail with name doesnotexist")
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testSelectingMissingGuardrail()
    {
        assertThatThrownBy(() -> execute(format("select value from %s.%s where name = 'doesnotexist'", KS_NAME, TABLE_NAME)))
        .describedAs("it should not be possible to set thresholds for non-existing guardrail")
        .hasMessageContaining("there is no associated getter for guardrail with name doesnotexist")
        .isInstanceOf(InvalidRequestException.class);
    }

    private void clearProperties()
    {
        for (String name : cache.getValuesGetters().keySet())
            clearProperties(name);
    }

    private void clearProperties(String guardrailName)
    {
        update(guardrailName, Set.of());
        GuardrailRow row = getGuardrailRow(guardrailName);
        assertThat(row.value).isEmpty();
    }

    private void update(String guardrailName, Set<String> value)
    {
        execute(format("update %s.%s set value = %s where name = '%s'",
                       KS_NAME,
                       TABLE_NAME,
                       getSetForQuery(value),
                       guardrailName));
    }

    private String getSetForQuery(Set<String> set)
    {
        StringBuilder sb = new StringBuilder();

        String[] array = set.toArray(new String[0]);

        sb.append('{');
        for (int i = 0; i < array.length; i++)
        {
            sb.append('\'');
            sb.append(array[i]);
            sb.append('\'');

            if (i + 1 != array.length)
                sb.append(", ");
        }
        sb.append('}');

        return sb.toString();
    }

    private void verifyTableContent()
    {
        List<GuardrailRow> guardrailRows = getGuardrailRows();
        assertEquals(cache.getValuesGetters().size(), guardrailRows.size());

        for (GuardrailRow row : guardrailRows)
        {
            Method method = cache.getValuesGetters().get(row.name).get(0);
            Object result = cache.invoke(method);
            assert result instanceof Set;

            Set<String> cacheResult = (Set<String>) result;
            assertNotNull(cacheResult);

            assertThat(cacheResult).containsExactlyInAnyOrderElementsOf(row.value);
        }
    }

    private GuardrailRow getGuardrailRow(String guardrailName)
    {
        return getGuardrailRows().stream().filter(r -> r.name.equals(guardrailName)).findFirst().orElse(null);
    }

    private List<GuardrailRow> getGuardrailRows()
    {
        UntypedResultSet result = execute(format("select * from %s.%s", KS_NAME, TABLE_NAME));

        return result.stream().map(row -> {
            String name = row.getString(NAME_COLUMN);
            Set<String> value = row.getSet(VALUE_COLUMN, UTF8Type.instance);
            return new GuardrailRow(name, value);
        }).collect(toList());
    }

    private static class GuardrailRow
    {
        public final String name;
        public final Set<String> value;

        public GuardrailRow(String name, Set<String> warned)
        {
            this.name = name;
            this.value = warned;
        }
    }
}
