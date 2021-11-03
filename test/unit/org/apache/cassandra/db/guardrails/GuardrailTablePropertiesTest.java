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

package org.apache.cassandra.db.guardrails;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.statements.schema.TableAttributes;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Tests the guardrail for table properties, {@link Guardrails#tableProperties}.
 */
public class GuardrailTablePropertiesTest extends GuardrailTester
{
    private static final String CREATE_TABLE = "CREATE TABLE %s.%s(pk int, ck int, v int, PRIMARY KEY(pk, ck)) %s";
    private static final String CREATE_VIEW = "CREATE MATERIALIZED VIEW %s.%s as SELECT * FROM %s.%s " +
                                              "WHERE pk IS NOT null and ck IS NOT null PRIMARY KEY(ck, pk) %s";
    private static final String ALTER_VIEW = "ALTER MATERIALIZED VIEW %s.%s WITH %s";

    @Before
    public void before()
    {
        // only allow "gc_grace_seconds" and "comments"
        Set<String> allowed = new HashSet<>(Arrays.asList("gc_grace_seconds", "comment"));
        guardrails().setTablePropertiesDisallowed(TableAttributes.validKeywords()
                                                                 .stream()
                                                                 .filter(p -> !allowed.contains(p))
                                                                 .map(String::toUpperCase)
                                                                 .collect(Collectors.toSet()));
        // but actually warn about "comment"
        guardrails().setTablePropertiesIgnored(Collections.singleton("comment"));
    }

    @Test
    public void testConfigValidation()
    {
        String message = "Invalid value for %s properties: null is not allowed";
        assertInvalidProperty(Guardrails::setTablePropertiesIgnored, (Set<String>) null, message, "ignored");
        assertInvalidProperty(Guardrails::setTablePropertiesDisallowed, (Set<String>) null, message, "disallowed");

        assertValidProperty(Collections.emptySet());
        assertValidProperty(TableAttributes.allKeywords());
        assertInvalidProperty(Collections.singleton("invalid"), Collections.singleton("invalid"));
        assertInvalidProperty(ImmutableSet.of("comment", "invalid1", "invalid2"), ImmutableSet.of("invalid1", "invalid2"));
        assertInvalidProperty(ImmutableSet.of("invalid1", "invalid2", "comment"), ImmutableSet.of("invalid1", "invalid2"));
        assertInvalidProperty(ImmutableSet.of("invalid1", "comment", "invalid2"), ImmutableSet.of("invalid1", "invalid2"));
    }

    private void assertValidProperty(Set<String> properties)
    {
        assertValidProperty(Guardrails::setTablePropertiesIgnored, properties);
        assertValidProperty(Guardrails::setTablePropertiesDisallowed, properties);
    }

    private void assertInvalidProperty(Set<String> properties, Set<String> rejected)
    {
        String message = "Invalid value for %s properties: '%s' do not parse as valid table properties";
        assertInvalidProperty(Guardrails::setTablePropertiesIgnored, properties, message, "ignored", rejected);
        assertInvalidProperty(Guardrails::setTablePropertiesDisallowed, properties, message, "disallowed", rejected);
    }

    @Test
    public void testTableProperties() throws Throwable
    {
        // most table properties are not allowed
        assertValid(this::createTableWithProperties);
        assertAborts(() -> createTableWithProperties("with id = " + UUID.randomUUID()), "[id]");
        assertAborts(() -> createTableWithProperties("with compression = { 'enabled': 'false' }"), "[compression]");
        assertAborts(() -> createTableWithProperties("with compression = { 'enabled': 'false' } AND id = " + UUID.randomUUID()), "[compression, id]");
        assertAborts(() -> createTableWithProperties("with compaction = { 'class': 'SizeTieredCompactionStrategy' }"), "[compaction]");
        assertAborts(() -> createTableWithProperties("with gc_grace_seconds = 1000 and compression = { 'enabled': 'false' }"), "[compression]");

        // though gc_grace_seconds alone is
        assertValid(() -> createTableWithProperties("with gc_grace_seconds = 1000"));

        // and comment is "ignored". So it should warn, and getting the comment on the created table should be empty,
        // not the one we set.
        AtomicReference<String> tableName = new AtomicReference<>();
        assertWarns(() -> tableName.set(createTableWithProperties("with comment = 'my table'")), "[comment]");
        assertEquals("", executeNet("SELECT comment FROM system_schema.tables WHERE keyspace_name=? AND table_name=?",
                                    keyspace(),
                                    tableName.get()).one().getString("comment"));

        // alter column is allowed
        assertValid(this::createTableWithProperties);
        assertValid("ALTER TABLE %s ADD v1 int");
        assertValid("ALTER TABLE %s DROP v1");
        assertValid("ALTER TABLE %s RENAME pk to pk1");
    }

    @Test
    public void testViewProperties() throws Throwable
    {
        // view properties is not allowed
        createTableWithProperties();
        assertValid(() -> createViewWithProperties(""));
        assertAborts(() -> createViewWithProperties("with compression = { 'enabled': 'false' }"), "[compression]");
        assertValid(() -> createViewWithProperties("with gc_grace_seconds = 1000"));

        // alter mv properties except "gc_grace_seconds" is not allowed
        assertValid(() -> alterViewWithProperties("gc_grace_seconds = 1000"));
        assertAborts(() -> alterViewWithProperties("compaction = { 'class': 'SizeTieredCompactionStrategy' } AND default_time_to_live = 1"),
                     "[compaction, default_time_to_live]");
        assertAborts(() -> alterViewWithProperties("compaction = { 'class': 'SizeTieredCompactionStrategy' } AND crc_check_chance = 1"),
                     "[compaction, crc_check_chance]");
    }

    @Test
    public void testInvalidTableProperties()
    {
        assertConfigFails(c -> c.setTablePropertiesDisallowed("ID1", "gc_grace_seconds"), "[id1]");
        assertConfigFails(c -> c.setTablePropertiesDisallowed("ID2", "Gc_Grace_Seconds"), "[id2]");
        assertConfigFails(c -> c.setTablePropertiesIgnored("ID3", "gc_grace_seconds"), "[id3]");
        assertConfigFails(c -> c.setTablePropertiesIgnored("ID4", "Gc_Grace_Seconds"), "[id4]");
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        testExcludedUsers(
        () -> format(CREATE_TABLE, keyspace(), createTableName(), "WITH compaction = { 'class': 'SizeTieredCompactionStrategy' }"),
        () -> format(CREATE_TABLE, keyspace(), createTableName(), "WITH gc_grace_seconds = 1000"),
        () -> "ALTER TABLE %s WITH gc_grace_seconds = 1000 and default_time_to_live = 1000",
        () -> "ALTER TABLE %s WITH compaction = { 'class': 'SizeTieredCompactionStrategy' }",
        () -> format(CREATE_VIEW, keyspace(), createViewName(), keyspace(), currentTable(), "with compression = { 'enabled': 'false' }"),
        () -> format(ALTER_VIEW, keyspace(), currentView(), "compaction = { 'class': 'SizeTieredCompactionStrategy' }"),
        () -> format(ALTER_VIEW, keyspace(), currentView(), "gc_grace_seconds = 1000"),
        () -> format(ALTER_VIEW, keyspace(), currentView(), "gc_grace_seconds = 1000 and crc_check_chance = 1"),
        () -> format(ALTER_VIEW, keyspace(), currentView(), "compaction = { 'class': 'SizeTieredCompactionStrategy' }"));
    }

    private void createTableWithProperties()
    {
        createTableWithProperties("");
    }

    private String createTableWithProperties(String withClause)
    {
        String name = createTableName();
        execute(userClientState, format(CREATE_TABLE, keyspace(), name, withClause));
        return name;
    }

    private void createViewWithProperties(String withClause)
    {
        execute(userClientState, format(CREATE_VIEW, keyspace(), createViewName(), keyspace(), currentTable(), withClause));
    }

    private void alterViewWithProperties(String withClause)
    {
        execute(userClientState, format(ALTER_VIEW, keyspace(), currentView(), withClause));
    }
}
