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

package org.apache.cassandra.guardrails;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.Schema;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class GuardrailsOnTableTest extends GuardrailTester
{
    private static final String CREATE_TABLE = "CREATE TABLE %s.%s(pk int, ck int, v int, PRIMARY KEY(pk, ck)) %s";
    private static final String CREATE_VIEW = "CREATE MATERIALIZED VIEW %s.%s as select * from %s.%s where pk is not null and ck is not null primary key (ck, pk) %s";
    private static final String ALTER_VIEW = "ALTER MATERIALIZED VIEW %s.%s WITH %s";
    private static long defaultTablesSoftLimit;
    private static long defaultTableHardLimit;
    private static int defaultMVPerTableFailureThreshold;
    private static Set<String> defaultTablePropertiesDisallowed;
    private static Set<String> defaultTablePropertiesIgnored;

    @Before
    public void before()
    {
        defaultTablesSoftLimit = DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold;
        defaultTableHardLimit = DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold;
        defaultMVPerTableFailureThreshold = DatabaseDescriptor.getGuardrailsConfig().materialized_view_per_table_failure_threshold;
        defaultTablePropertiesDisallowed = DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed;
        defaultTablePropertiesIgnored = DatabaseDescriptor.getGuardrailsConfig().table_properties_ignored;

        defaultMVPerTableFailureThreshold = 100;
        // only allow "gc_grace_seconds" and "comments"
        Set<String> allowed = new HashSet<>(Arrays.asList("gc_grace_seconds", "comment"));
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed =
        TableAttributes.validKeywords.stream()
                                     .filter(p -> !allowed.contains(p))
                                     .map(String::toUpperCase)
                                     .collect(ImmutableSet.toImmutableSet());
        // but actually ignore "comment"
        DatabaseDescriptor.getGuardrailsConfig().table_properties_ignored = ImmutableSet.of("comment");
    }

    @After
    public void after()
    {
        DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold = defaultTablesSoftLimit;
        DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold = defaultTableHardLimit;
        DatabaseDescriptor.getGuardrailsConfig().materialized_view_per_table_failure_threshold = defaultMVPerTableFailureThreshold;
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed = defaultTablePropertiesDisallowed;
        DatabaseDescriptor.getGuardrailsConfig().table_properties_ignored = defaultTablePropertiesIgnored;
    }

    @Test
    public void testTableLimit() throws Throwable
    {
        // check previous async dropping schema tasks have been finished...
        int waitInSeconds = 30;
        while (schemaCleanup.getActiveCount() > 0 && waitInSeconds-- >= 0)
        {
            Thread.sleep(1000);
        }

        int currentTables = Schema.instance.getNonInternalKeyspaces().stream().map(Keyspace::open)
                                           .mapToInt(keyspace -> keyspace.getColumnFamilyStores().size()).sum();
        long warn = currentTables + 1;
        long fail = currentTables + 3;
        DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold = warn;
        DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold = fail;

        assertValid(this::create);
        assertWarns(this::create, format("current number of tables %d exceeds warning threshold of %d", currentTables + 2, warn));
        assertWarns(this::create, format("current number of tables %d exceeds warning threshold of %d", currentTables + 3, warn));
        assertFails(this::create, format("Cannot have more than %s tables, failed to create table", fail));

        // test super user
        String ks1 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        for (int i = 0; i <= fail + 1; i++)
        {
            assertSuperuserIsExcluded(format(CREATE_TABLE, ks1, "t" + i, ""));
        }

        // test internal queries
        String ks2 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        for (int i = 0; i <= fail + 1; i++)
        {
            assertInternalQueriesAreExcluded(format(CREATE_TABLE, ks2, "t" + i, ""));
        }
    }

    @Test
    public void testTableProperties() throws Throwable
    {
        // most table properties are not allowed
        assertValid(this::create);
        assertFails(() -> create("with id = " + UUID.randomUUID()), "[id]");
        assertFails(() -> create("with compression = { 'enabled': 'false' }"), "[compression]");
        assertFails(() -> create("with compression = { 'enabled': 'false' } AND id = " + UUID.randomUUID()), "[compression, id]");
        assertFails(() -> create("with compaction = { 'class': 'SizeTieredCompactionStrategy' }"), "[compaction]");
        assertFails(() -> create("with gc_grace_seconds = 1000 and compression = { 'enabled': 'false' }"), "[compression]");

        // though gc_grace_seconds alone is
        assertValid(() -> create("with gc_grace_seconds = 1000"));

        // and comment is "ignored". So it should warn, and getting the comment on the created table should be empty,
        // not the one we set.
        AtomicReference<String> tableName = new AtomicReference<>();
        assertWarns(() -> tableName.set(create("with comment = 'my table'")), "[comment]");
        com.datastax.driver.core.ResultSet rs =
        executeNet("SELECT comment FROM system_schema.tables WHERE keyspace_name=? AND table_name=?",
                   keyspace(),
                   tableName.get());
        com.datastax.driver.core.Row r = rs.one();
        assertEquals("", r.getString("comment"));

        // alter column is allowed
        assertValid(this::create);
        assertValid("ALTER TABLE %s ADD v1 int");
        assertValid("ALTER TABLE %s DROP v1");
        assertValid("ALTER TABLE %s RENAME pk to pk1");

        // alter table properties except "gc_grace_seconds" is not allowed
        assertValid("ALTER TABLE %s WITH gc_grace_seconds = 1000");
        assertFails("[compaction, default_time_to_live]",
                    "ALTER TABLE %s WITH compaction = { 'class': 'SizeTieredCompactionStrategy' } AND default_time_to_live = 1");
        assertFails("[compaction, crc_check_chance]",
                    "ALTER TABLE %s WITH compaction = { 'class': 'SizeTieredCompactionStrategy' } AND crc_check_chance = 1");

        // skip table properties guardrails for super user
        assertSuperuserIsExcluded(
        format(CREATE_TABLE, keyspace(), createTableName(), "WITH compaction = { 'class': 'SizeTieredCompactionStrategy' }"),
        format(CREATE_TABLE, keyspace(), createTableName(), "WITH gc_grace_seconds = 1000"),
        "ALTER TABLE %s WITH gc_grace_seconds = 1000 and default_time_to_live = 1000",
        "ALTER TABLE %s WITH compaction = { 'class': 'SizeTieredCompactionStrategy' }");

        // skip table properties guardrails for internal queries
        assertInternalQueriesAreExcluded(
        format(CREATE_TABLE, keyspace(), createTableName(), "WITH compaction = { 'class': 'SizeTieredCompactionStrategy' }"),
        format(CREATE_TABLE, keyspace(), createTableName(), "WITH gc_grace_seconds = 1000"),
        "ALTER TABLE %s WITH gc_grace_seconds = 1000 and default_time_to_live = 1000",
        "ALTER TABLE %s WITH compaction = { 'class': 'SizeTieredCompactionStrategy' }");
    }

    @Test
    public void testViewProperties() throws Throwable
    {
        // view properties is not allowed
        assertValid(this::create);
        assertValid(() -> createMV(""));
        assertFails(() -> createMV("with compression = { 'enabled': 'false' }"), "[compression]");
        assertValid(() -> createMV("with gc_grace_seconds = 1000"));
        String viewName = currentView();

        // alter mv properties except "gc_grace_seconds" is not allowed
        assertValid(format(ALTER_VIEW, keyspace(), viewName, "gc_grace_seconds = 1000"));
        assertFails("[compaction, default_time_to_live]",
                    format(ALTER_VIEW, keyspace(), viewName, "compaction = { 'class': 'SizeTieredCompactionStrategy' } AND default_time_to_live = 1"));
        assertFails("[compaction, crc_check_chance]",
                    format(ALTER_VIEW, keyspace(), viewName, "compaction = { 'class': 'SizeTieredCompactionStrategy' } AND crc_check_chance = 1"));

        // skip table properties guardrails for super user
        assertSuperuserIsExcluded(
        format(ALTER_VIEW, keyspace(), viewName, "compaction = { 'class': 'SizeTieredCompactionStrategy' }"),
        format(ALTER_VIEW, keyspace(), viewName, "gc_grace_seconds = 1000"),
        format(ALTER_VIEW, keyspace(), viewName, "gc_grace_seconds = 1000 and crc_check_chance = 1"),
        format(ALTER_VIEW, keyspace(), viewName, "compaction = { 'class': 'SizeTieredCompactionStrategy' }"));

        // skip table properties guardrails for internal queries
        assertInternalQueriesAreExcluded(
        format(ALTER_VIEW, keyspace(), viewName, "compaction = { 'class': 'SizeTieredCompactionStrategy' }"),
        format(ALTER_VIEW, keyspace(), viewName, "gc_grace_seconds = 1000"),
        format(ALTER_VIEW, keyspace(), viewName, "gc_grace_seconds = 1000 and crc_check_chance = 1"),
        format(ALTER_VIEW, keyspace(), viewName, "compaction = { 'class': 'SizeTieredCompactionStrategy' }"));
    }

    @Test
    public void testInvalidTableProperties()
    {
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();

        config.table_properties_disallowed = ImmutableSet.of("ID1", "gc_grace_seconds");
        assertConfigFails(config::validate, "[id1]");

        config.table_properties_disallowed = ImmutableSet.of("ID", "Gc_Grace_Seconds");
        config.validate();
    }

    private void create() throws Throwable
    {
        create("");
    }

    private String create(String withClause) throws Throwable
    {
        String name = createTableName();
        executeNet(format(CREATE_TABLE, keyspace(), name, withClause));
        return name;
    }

    private void createMV(String withClause) throws Throwable
    {
        executeNet(format(CREATE_VIEW, keyspace(), createViewName(), keyspace(), currentTable(), withClause));
    }
}
