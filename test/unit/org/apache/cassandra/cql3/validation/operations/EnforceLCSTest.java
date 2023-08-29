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

package org.apache.cassandra.cql3.validation.operations;

import java.util.LinkedHashMap;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.transport.ProtocolVersion;

public class EnforceLCSTest extends CQLTester
{
    @Test
    public void testAlterOnCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "mutate compaction strategy",
                                  InvalidQueryException.class,
                                  formatQuery("ALTER TABLE %s WITH compaction={'class': 'LeveledCompactionStrategy'};"));
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "mutate compaction strategy",
                                  InvalidQueryException.class,
                                  formatQuery("ALTER TABLE %s WITH compaction={'class': 'LeveledCompactionStrategy'};"));
        // mutation can only be performed if enforcement level is set to none
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.none);
        execute(formatQuery("ALTER TABLE %s WITH compaction={'class': 'LeveledCompactionStrategy'};"));
        assertCompactionStrategy("LeveledCompactionStrategy");
    }

    @Test
    public void testNonSpecifiedCompactionForCreate() throws Throwable
    {
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertCompactionStrategy("LeveledCompactionStrategy", KEYSPACE, table1);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertCompactionStrategy("LeveledCompactionStrategy", KEYSPACE, table2);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.none);
        String table3 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertCompactionStrategy(CompactionParams.DEFAULT.klass().getName(), KEYSPACE, table3);
    }

    @Test
    public void testSpecifiedCompactionForCreate() throws Throwable
    {
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "LCS enforcement is enabled",
                                  InvalidQueryException.class,
                                  "CREATE TABLE " + KEYSPACE + "." + createTableName() +
                                  " (id text PRIMARY KEY, content text) WITH compaction={'class': 'SizeTieredCompactionStrategy'};");

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH compaction={'class': 'SizeTieredCompactionStrategy'};");
        assertCompactionStrategy("LeveledCompactionStrategy", KEYSPACE, table1);

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.none);
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) WITH compaction={'class': 'SizeTieredCompactionStrategy'};");
        assertCompactionStrategy("SizeTieredCompactionStrategy", KEYSPACE, table2);
    }

    private void assertCompactionStrategy(String expected) throws Throwable
    {
        assertCompactionStrategy(expected, KEYSPACE, currentTable());
    }

    @SuppressWarnings (value="unchecked")
    private void assertCompactionStrategy(String expected, String keyspace, String table) throws Throwable
    {
        expected = expected.contains(".")
                 ? expected
                 : "org.apache.cassandra.db.compaction." + expected;

        Object[][] results = getRows(execute("SELECT compaction FROM system_schema.tables WHERE keyspace_name=? AND table_name=?;", KEYSPACE, currentTable()));
        // should have exact one matching record
        if (results.length == 0 || results[0].length == 0) {
            Assert.fail(String.format("Can't get matched row in system_schmea.tables. Expected 1 row for %s.%s.", keyspace, table));
        }
        try
        {
            LinkedHashMap<String, String> csOption = (LinkedHashMap<String, String>) results[0][0];
            String compactionStrategy = csOption.get(CompactionParams.Option.CLASS.toString());
            Assert.assertEquals(expected, compactionStrategy);
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }
}
