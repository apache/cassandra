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
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.Schema;

import static java.lang.String.format;

public class GuardrailsOnTableTest extends GuardrailTester
{
    private static final String CREATE_TABLE = "CREATE TABLE %s.%s(pk int, ck int, v int, PRIMARY KEY(pk, ck)) %s";
    private static final String CREATE_VIEW = "CREATE MATERIALIZED VIEW %s.%s as select * from %s.%s where pk is not null and ck is not null primary key (ck, pk) %s";
    private static final String ALTER_VIEW = "ALTER MATERIALIZED VIEW %s.%s WITH %s";
    private static long defaultTablesSoftLimit;
    private static long defaultTableHardLimit;
    private static long defaultMVPerTableFailureThreshold;
    private static Set<String> defaultTablePropertiesDisallowed;

    @Before
    public void before()
    {
        defaultTablesSoftLimit = DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold;
        defaultTableHardLimit = DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold;
        defaultMVPerTableFailureThreshold = DatabaseDescriptor.getGuardrailsConfig().materialized_view_per_table_failure_threshold;
        defaultTablePropertiesDisallowed = DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed;

        // only allow "gc_grace_seconds"
        defaultMVPerTableFailureThreshold = 100;
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed =
        TableAttributes.validKeywords.stream()
                                     .filter(p -> !p.equals("gc_grace_seconds"))
                                     .map(String::toUpperCase)
                                     .collect(Collectors.toSet());
    }

    @After
    public void after()
    {
        DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold = defaultTablesSoftLimit;
        DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold = defaultTableHardLimit;
        DatabaseDescriptor.getGuardrailsConfig().materialized_view_per_table_failure_threshold = defaultMVPerTableFailureThreshold;
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed = defaultTablePropertiesDisallowed;
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

        int currentTables = Schema.instance.getUserKeyspaces().stream().map(ksm -> Keyspace.open(ksm.name))
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
        // table properties is not allowed
        assertValid(this::create);
        assertFails(() -> create("with id = " + UUID.randomUUID()), "[id]");
        assertFails(() -> create("with compression = { 'enabled': 'false' }"), "[compression]");
        assertFails(() -> create("with compression = { 'enabled': 'false' } AND id = " + UUID.randomUUID()), "[compression, id]");
        assertFails(() -> create("with compaction = { 'class': 'SizeTieredCompactionStrategy' }"), "[compaction]");
        assertFails(() -> create("with gc_grace_seconds = 1000 and compression = { 'enabled': 'false' }"), "[compression]");
        assertValid(() -> create("with gc_grace_seconds = 1000"));

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

        config.table_properties_disallowed = new HashSet<>(Arrays.asList("ID1", "gc_grace_seconds"));
        assertConfigFails(config::validate, "[id1]");

        config.table_properties_disallowed = new HashSet<>(Arrays.asList("ID", "Gc_Grace_Seconds"));
        config.validate();
    }

    private void create() throws Throwable
    {
        create("");
    }

    private void create(String withClause) throws Throwable
    {
        executeNet(format(CREATE_TABLE, keyspace(), createTableName(), withClause));
    }

    private void createMV(String withClause) throws Throwable
    {
        executeNet(format(CREATE_VIEW, keyspace(), createViewName(), keyspace(), currentTable(), withClause));
    }
}
