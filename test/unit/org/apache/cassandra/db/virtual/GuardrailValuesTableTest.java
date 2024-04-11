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
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.guardrails.GuardrailTester;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Guardrails.ValuesGuardrailsMapper;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.virtual.GuardrailEnableFlagsTable.NAME_COLUMN;
import static org.apache.cassandra.db.virtual.GuardrailValuesTable.DISALLOWED_COLUMN;
import static org.apache.cassandra.db.virtual.GuardrailValuesTable.IGNORED_COLUMN;
import static org.apache.cassandra.db.virtual.GuardrailValuesTable.TABLE_NAME;
import static org.apache.cassandra.db.virtual.GuardrailValuesTable.WARNED_COLUMN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GuardrailValuesTableTest extends GuardrailTester
{
    private static final String KS_NAME = "vts";

    @BeforeClass
    public static void before()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, List.of(new GuardrailValuesTable(KS_NAME))));
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

            clearProperties(Guardrails.tableProperties.name);
            update(Guardrails.tableProperties.name, propertiesDisallowed, propertiesWarned, propertiesIgnored);

            GuardrailRow tablePropertiesRow = getGuardrailRow(Guardrails.tableProperties.name);
            assertThat(tablePropertiesRow.disallowed).isNotEmpty();
            assertThat(tablePropertiesRow.warned).isNotEmpty();
            assertThat(tablePropertiesRow.ignored).isNotEmpty();

            assertThat(tablePropertiesRow.disallowed).containsExactlyInAnyOrderElementsOf(propertiesDisallowed);
            assertThat(tablePropertiesRow.warned).containsExactlyInAnyOrderElementsOf(propertiesWarned);
            assertThat(tablePropertiesRow.ignored).containsExactlyInAnyOrderElementsOf(propertiesIgnored);
        }
        finally
        {
            clearProperties();
        }
    }

    @Test
    public void testInvalidValuesForConsistencyRule()
    {
        Set<String> disallowed = Set.of("NONEXISTINGLEVEL");
        assertThatThrownBy(() -> update(Guardrails.readConsistencyLevels.name, disallowed, Set.of(), Set.of()))
        .hasMessageContaining("No enum constant org.apache.cassandra.db.ConsistencyLevel.NONEXISTINGLEVEL")
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testUnsettableValues()
    {
        // there is no "ignored" setter for read / write consistency levels
        // let's test what happens when we want to set it
        try
        {
            Set<String> disallowed = Set.of(QUORUM.name());
            Set<String> warned = Set.of(EACH_QUORUM.name());
            Set<String> ignored = Set.of(ALL.name());

            update(Guardrails.readConsistencyLevels.name, disallowed, warned, ignored);
            update(Guardrails.writeConsistencyLevels.name, disallowed, warned, ignored);

            GuardrailRow readRow = getGuardrailRow(Guardrails.readConsistencyLevels.name);
            assertThat(readRow.disallowed).containsExactlyInAnyOrderElementsOf(disallowed);
            assertThat(readRow.warned).containsExactlyInAnyOrderElementsOf(warned);

            GuardrailRow writeRow = getGuardrailRow(Guardrails.readConsistencyLevels.name);
            assertThat(writeRow.disallowed).containsExactlyInAnyOrderElementsOf(disallowed);
            assertThat(writeRow.warned).containsExactlyInAnyOrderElementsOf(warned);

            // empty! because setting ignored is a no-op
            assertThat(readRow.ignored).isEmpty();
            assertThat(writeRow.ignored).isEmpty();
        }
        finally
        {
            clearProperties();
        }
    }

    @Test
    public void testUpdatingMissingGuardrail()
    {
        assertThatThrownBy(() -> execute(format("update %s.%s set warned = {'abc'} where name = 'doesnotexist'", KS_NAME, TABLE_NAME)))
        .describedAs("it should not be possible to set thresholds for non-existing guardrail")
        .hasMessageContaining("there is no such guardrail with name 'doesnotexist'")
        .isInstanceOf(InvalidRequestException.class);
    }


    private void clearProperties()
    {
        for (String name : Guardrails.getValueGuardrails().keySet())
            clearProperties(name);
    }

    private void clearProperties(String guardrailName)
    {
        update(guardrailName, Set.of(), Set.of(), Set.of());

        GuardrailRow row = getGuardrailRow(guardrailName);

        assertThat(row.disallowed).isEmpty();
        assertThat(row.warned).isEmpty();
        assertThat(row.ignored).isEmpty();
    }

    private void update(String guardrailName, Set<String> disallowed, Set<String> warned, Set<String> ignored)
    {
        execute(format("update %s.%s set warned = %s, disallowed = %s, ignored = %s where name = '%s'",
                       KS_NAME,
                       TABLE_NAME,
                       getSetForQuery(warned),
                       getSetForQuery(disallowed),
                       getSetForQuery(ignored),
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
        assertEquals(Guardrails.getValueGuardrails().size(), guardrailRows.size());

        for (GuardrailRow row : guardrailRows)
        {
            ValuesGuardrailsMapper mapping = Guardrails.getValueGuardrails().get(row.name);
            assertNotNull(mapping);

            assertThat(mapping.warnedValuesSupplier.get()).containsExactlyInAnyOrderElementsOf(row.warned);
            assertThat(mapping.ignoredValuesSupplier.get()).containsExactlyInAnyOrderElementsOf(row.ignored);
            assertThat(mapping.disallowedValuesSupplier.get()).containsExactlyInAnyOrderElementsOf(row.disallowed);
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
            List<String> warned = new ArrayList<>(row.getSet(WARNED_COLUMN, UTF8Type.instance));
            List<String> ignored = new ArrayList<>(row.getSet(IGNORED_COLUMN, UTF8Type.instance));
            List<String> disallowed = new ArrayList<>(row.getSet(DISALLOWED_COLUMN, UTF8Type.instance));
            return new GuardrailRow(name, warned, ignored, disallowed);
        }).collect(toList());
    }

    private static class GuardrailRow
    {
        public final String name;
        public final List<String> warned;
        public final List<String> ignored;
        public final List<String> disallowed;

        public GuardrailRow(String name, List<String> warned, List<String> ignored, List<String> disallowed)
        {
            this.name = name;
            this.warned = warned;
            this.ignored = ignored;
            this.disallowed = disallowed;
        }
    }
}
