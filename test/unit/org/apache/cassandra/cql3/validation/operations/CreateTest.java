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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.SkipListMemtable;
import org.apache.cassandra.db.memtable.TestMemtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.Duration.*;
import static org.junit.Assert.*;

public class CreateTest extends CQLTester
{
    @Test
    public void testCreateTableWithNameCapitalPAndColumnDuration() throws Throwable
    {
        // CASSANDRA-17919
        createTable(KEYSPACE, "CREATE TABLE %s (a INT PRIMARY KEY, b DURATION);", "P");
        execute("INSERT INTO %s (a, b) VALUES (1, PT0S)");
        assertRows(execute("SELECT * FROM %s"), row(1, Duration.newInstance(0, 0, 0)));
    }

    @Test
    public void testCreateKeyspaceWithNameCapitalP() throws Throwable
    {
        // CASSANDRA-17919
        executeFormattedQuery("CREATE KEYSPACE IF NOT EXISTS P WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeFormattedQuery("DROP KEYSPACE P");
    }

    @Test
    public void testCreateTableWithSmallintColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b smallint, c smallint, primary key (a, b));");
        execute("INSERT INTO %s (a, b, c) VALUES ('1', 1, 2)");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "2", Short.MAX_VALUE, Short.MIN_VALUE);

        assertRows(execute("SELECT * FROM %s"),
                   row("2", Short.MAX_VALUE, Short.MIN_VALUE),
                   row("1", (short) 1, (short) 2));

        assertInvalidMessage("Expected 2 bytes for a smallint (4)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", 1, 2);
        assertInvalidMessage("Expected 2 bytes for a smallint (0)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", (short) 1, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Test
    public void testCreateTinyintColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b tinyint, c tinyint, primary key (a, b));");
        execute("INSERT INTO %s (a, b, c) VALUES ('1', 1, 2)");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "2", Byte.MAX_VALUE, Byte.MIN_VALUE);

        assertRows(execute("SELECT * FROM %s"),
                   row("2", Byte.MAX_VALUE, Byte.MIN_VALUE),
                   row("1", (byte) 1, (byte) 2));

        assertInvalidMessage("Expected 1 byte for a tinyint (4)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", 1, 2);

        assertInvalidMessage("Expected 1 byte for a tinyint (0)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", (byte) 1, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Test
    public void testCreateTableWithDurationColumns() throws Throwable
    {
        assertInvalidMessage("duration type is not supported for PRIMARY KEY column 'a'",
                             "CREATE TABLE cql_test_keyspace.table0 (a duration PRIMARY KEY, b int);");

        assertInvalidMessage("duration type is not supported for PRIMARY KEY column 'b'",
                             "CREATE TABLE cql_test_keyspace.table0 (a text, b duration, c duration, primary key (a, b));");

        assertInvalidMessage("duration type is not supported for PRIMARY KEY column 'b'",
                             "CREATE TABLE cql_test_keyspace.table0 (a text, b duration, c duration, primary key (a, b)) with clustering order by (b DESC);");

        createTable("CREATE TABLE %s (a int, b int, c duration, primary key (a, b));");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 1y2mo)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, -1y2mo)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 1Y2MO)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, 2w)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 5, 2d10h)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 6, 30h20m)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 7, 20m)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 8, 567ms)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 9, 1950us)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 10, 1950µs)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 11, 1950000NS)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 12, -1950000ns)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 13, 1y3mo2h10m)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 14, -P1Y2M)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 15, P2D)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 16, PT20M)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 17, P2W)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 18, P1Y3MT2H10M)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 19, P0000-00-00T30:20:00)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 20, P0001-03-00T02:10:00)");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 21, duration(12, 10, 0));
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 22, duration(-12, -10, 0));

        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, Duration.newInstance(14, 0, 0)),
                   row(1, 2, Duration.newInstance(-14, 0, 0)),
                   row(1, 3, Duration.newInstance(14, 0, 0)),
                   row(1, 4, Duration.newInstance(0, 14, 0)),
                   row(1, 5, Duration.newInstance(0, 2, 10 * NANOS_PER_HOUR)),
                   row(1, 6, Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE)),
                   row(1, 7, Duration.newInstance(0, 0, 20 * NANOS_PER_MINUTE)),
                   row(1, 8, Duration.newInstance(0, 0, 567 * NANOS_PER_MILLI)),
                   row(1, 9, Duration.newInstance(0, 0, 1950 * NANOS_PER_MICRO)),
                   row(1, 10, Duration.newInstance(0, 0, 1950 * NANOS_PER_MICRO)),
                   row(1, 11, Duration.newInstance(0, 0, 1950000)),
                   row(1, 12, Duration.newInstance(0, 0, -1950000)),
                   row(1, 13, Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE)),
                   row(1, 14, Duration.newInstance(-14, 0, 0)),
                   row(1, 15, Duration.newInstance(0, 2, 0)),
                   row(1, 16, Duration.newInstance(0, 0, 20 * NANOS_PER_MINUTE)),
                   row(1, 17, Duration.newInstance(0, 14, 0)),
                   row(1, 18, Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE)),
                   row(1, 19, Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE)),
                   row(1, 20, Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE)),
                   row(1, 21, Duration.newInstance(12, 10, 0)),
                   row(1, 22, Duration.newInstance(-12, -10, 0)));

        assertInvalidMessage("Slice restrictions are not supported on duration columns",
                             "SELECT * FROM %s WHERE c > 1y ALLOW FILTERING");

        assertInvalidMessage("Slice restrictions are not supported on duration columns",
                             "SELECT * FROM %s WHERE c <= 1y ALLOW FILTERING");

        assertInvalidMessage("Expected at least 3 bytes for a duration (1)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, (byte) 1);
        assertInvalidMessage("Expected at least 3 bytes for a duration (0)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        assertInvalidMessage("Invalid duration. The total number of days must be less or equal to 2147483647",
                             "INSERT INTO %s (a, b, c) VALUES (1, 2, " + Long.MAX_VALUE + "d)");

        assertInvalidMessage("The duration months, days and nanoseconds must be all of the same sign (2, -2, 0)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, duration(2, -2, 0));

        assertInvalidMessage("The duration months, days and nanoseconds must be all of the same sign (-2, 0, 2000000)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, duration(-2, 0, 2000000));

        assertInvalidMessage("The duration months must be a 32 bits integer but was: 9223372036854775807",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, duration(9223372036854775807L, 1, 0));

        assertInvalidMessage("The duration days must be a 32 bits integer but was: 9223372036854775807",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, 1, duration(0, 9223372036854775807L, 0));

        // Test with duration column name
        createTable("CREATE TABLE %s (a text PRIMARY KEY, duration duration);");

        // Test duration within Map
        assertInvalidMessage("Durations are not allowed as map keys: map<duration, text>",
                             "CREATE TABLE cql_test_keyspace.table0(pk int PRIMARY KEY, m map<duration, text>)");

        createTable("CREATE TABLE %s(pk int PRIMARY KEY, m map<text, duration>)");
        execute("INSERT INTO %s (pk, m) VALUES (1, {'one month' : 1mo, '60 days' : 60d})");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, map("one month", Duration.from("1mo"), "60 days", Duration.from("60d"))));

        assertInvalidMessage("duration type is not supported for PRIMARY KEY column 'm'",
                "CREATE TABLE cql_test_keyspace.table0(m frozen<map<text, duration>> PRIMARY KEY, v int)");

        assertInvalidMessage("duration type is not supported for PRIMARY KEY column 'm'",
                             "CREATE TABLE cql_test_keyspace.table0(pk int, m frozen<map<text, duration>>, v int, PRIMARY KEY (pk, m))");

        // Test duration within Set
        assertInvalidMessage("Durations are not allowed inside sets: set<duration>",
                             "CREATE TABLE cql_test_keyspace.table0(pk int PRIMARY KEY, s set<duration>)");

        assertInvalidMessage("Durations are not allowed inside sets: frozen<set<duration>>",
                             "CREATE TABLE cql_test_keyspace.table0(s frozen<set<duration>> PRIMARY KEY, v int)");

        // Test duration within List
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, l list<duration>)");
        execute("INSERT INTO %s (pk, l) VALUES (1, [1mo, 60d])");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, list(Duration.from("1mo"), Duration.from("60d"))));

        assertInvalidMessage("duration type is not supported for PRIMARY KEY column 'l'",
                             "CREATE TABLE cql_test_keyspace.table0(l frozen<list<duration>> PRIMARY KEY, v int)");

        // Test duration within Tuple
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, t tuple<int, duration>)");
        execute("INSERT INTO %s (pk, t) VALUES (1, (1, 1mo))");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, tuple(1, Duration.from("1mo"))));

        assertInvalidMessage("duration type is not supported for PRIMARY KEY column 't'",
                             "CREATE TABLE cql_test_keyspace.table0(t frozen<tuple<int, duration>> PRIMARY KEY, v int)");

        // Test duration within UDT
        String typename = createType("CREATE TYPE %s (a duration)");
        String myType = KEYSPACE + '.' + typename;
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, u " + myType + ")");
        execute("INSERT INTO %s (pk, u) VALUES (1, {a : 1mo})");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, userType("a", Duration.from("1mo"))));

        assertInvalidMessage("duration type is not supported for PRIMARY KEY column 'u'",
                             "CREATE TABLE cql_test_keyspace.table0(pk int, u frozen<" + myType + ">, v int, PRIMARY KEY(pk, u))");

        // Test duration with several level of depth
        assertInvalidMessage("duration type is not supported for PRIMARY KEY column 'm'",
                "CREATE TABLE cql_test_keyspace.table0(pk int, m frozen<map<text, list<tuple<int, duration>>>>, v int, PRIMARY KEY (pk, m))");
    }

    private ByteBuffer duration(long months, long days, long nanoseconds) throws IOException
    {
        try(DataOutputBuffer output = new DataOutputBuffer())
        {
            output.writeVInt(months);
            output.writeVInt(days);
            output.writeVInt(nanoseconds);
            return output.buffer();
        }
    }

    /**
     * Creation and basic operations on a static table,
     * migrated from cql_tests.py:TestCQL.static_cf_test()
     */
    @Test
    public void testStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int)");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, ?, ?, ?)", id1, "Frodo", "Baggins", 32);
        execute("UPDATE %s SET firstname = ?, lastname = ?, age = ? WHERE userid = ?", "Samwise", "Gamgee", 33, id2);

        assertRows(execute("SELECT firstname, lastname FROM %s WHERE userid = ?", id1),
                   row("Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s WHERE userid = ?", id1),
                   row(id1, 32, "Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 33, "Samwise", "Gamgee"),
                   row(id1, 32, "Frodo", "Baggins")
        );

        String batch = "BEGIN BATCH "
                       + "INSERT INTO %1$s (userid, age) VALUES (?, ?) "
                       + "UPDATE %1$s SET age = ? WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "APPLY BATCH";

        execute(batch, id1, 36, 37, id2, id1, id2);

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 37, null, null),
                   row(id1, 36, null, null));
    }

   /**
     * Creation and basic operations on a composite table,
     * migrated from cql_tests.py:TestCQL.sparse_cf_test()
     */
    @Test
    public void testSparseCompositeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid, posted_month int, posted_day int, body text, posted_by text, PRIMARY KEY (userid, posted_month, posted_day))");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, posted_month, posted_day, body, posted_by) VALUES (?, 1, 12, 'Something else', 'Frodo Baggins')", id1);
        execute("INSERT INTO %s (userid, posted_month, posted_day, body, posted_by) VALUES (?, 1, 24, 'Something something', 'Frodo Baggins')", id1);
        execute("UPDATE %s SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = ? AND posted_month = 1 AND posted_day = 3", id2);
        execute("UPDATE %s SET body = 'Yet one more message' WHERE userid = ? AND posted_month = 1 and posted_day = 30", id1);

        assertRows(execute("SELECT body, posted_by FROM %s WHERE userid = ? AND posted_month = 1 AND posted_day = 24", id1),
                   row("Something something", "Frodo Baggins"));

        assertRows(execute("SELECT posted_day, body, posted_by FROM %s WHERE userid = ? AND posted_month = 1 AND posted_day > 12", id1),
                   row(24, "Something something", "Frodo Baggins"),
                   row(30, "Yet one more message", null));

        assertRows(execute("SELECT posted_day, body, posted_by FROM %s WHERE userid = ? AND posted_month = 1", id1),
                   row(12, "Something else", "Frodo Baggins"),
                   row(24, "Something something", "Frodo Baggins"),
                   row(30, "Yet one more message", null));
    }

    /**
     * Check invalid create table statements,
     * migrated from cql_tests.py:TestCQL.create_invalid_test()
     */
    @Test
    public void testInvalidCreateTableStatements() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE TABLE test ()");

        assertInvalid("CREATE TABLE test (c1 text, c2 text, c3 text)");
        assertInvalid("CREATE TABLE test (key1 text PRIMARY KEY, key2 text PRIMARY KEY)");

        assertInvalid("CREATE TABLE test (key text PRIMARY KEY, key int)");
        assertInvalid("CREATE TABLE test (key text PRIMARY KEY, c int, c text)");
    }

    /**
     * Check obsolete properties from CQL2 are rejected
     * migrated from cql_tests.py:TestCQL.invalid_old_property_test()
     */
    @Test
    public void testObsoleteTableProperties() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE TABLE cql_test_keyspace.table0 (foo text PRIMARY KEY, c int) WITH default_validation=timestamp");

        createTable("CREATE TABLE %s (foo text PRIMARY KEY, c int)");
        assertInvalidThrow(SyntaxException.class, "ALTER TABLE %s WITH default_validation=int");
    }

    /**
     * Test create and drop keyspace
     * migrated from cql_tests.py:TestCQL.keyspace_test()
     */
    @Test
    public void testKeyspace() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE KEYSPACE %s testXYZ ");

        execute("CREATE KEYSPACE testXYZ WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        assertInvalid(
                     "CREATE KEYSPACE My_much_much_too_long_identifier_that_should_not_work WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        execute("DROP KEYSPACE testXYZ");
        assertInvalidThrow(InvalidRequestException.class, "DROP KEYSPACE non_existing");

        execute("CREATE KEYSPACE testXYZ WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        // clean-up
        execute("DROP KEYSPACE testXYZ");
    }

    /**
     *  Test {@link ConfigurationException} is thrown on create keyspace with invalid DC option in replication configuration .
     */
    @Test
    public void testCreateKeyspaceWithNTSOnlyAcceptsConfiguredDataCenterNames() throws Throwable
    {
        assertInvalidThrow(ConfigurationException.class, "CREATE KEYSPACE testABC WITH replication = { 'class' : 'NetworkTopologyStrategy', 'INVALID_DC' : 2 }");
        execute("CREATE KEYSPACE testABC WITH replication = {'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 2 }");

        // Mix valid and invalid, should throw an exception
        assertInvalidThrow(ConfigurationException.class, "CREATE KEYSPACE testXYZ WITH replication={ 'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 2 , 'INVALID_DC': 1}");

        // clean-up
        execute("DROP KEYSPACE IF EXISTS testABC");
        execute("DROP KEYSPACE IF EXISTS testXYZ");
    }

    /**
     * Test {@link ConfigurationException} is not thrown on create NetworkTopologyStrategy keyspace without any options.
     */
    @Test
    public void testCreateKeyspaceWithNetworkTopologyStrategyNoOptions() throws Throwable
    {
        schemaChange("CREATE KEYSPACE testXYZ with replication = { 'class': 'NetworkTopologyStrategy' }");

        // clean-up
        execute("DROP KEYSPACE IF EXISTS testXYZ");
    }

    /**
     * Test {@link ConfigurationException} is not thrown on create SimpleStrategy keyspace without any options.
     */
    @Test
    public void testCreateKeyspaceWithSimpleStrategyNoOptions() throws Throwable
    {
        schemaChange("CREATE KEYSPACE testXYZ WITH replication = { 'class' : 'SimpleStrategy' }");

        // clean-up
        execute("DROP KEYSPACE IF EXISTS testXYZ");
    }

    @Test
    public void testCreateKeyspaceWithMultipleInstancesOfSameDCThrowsException() throws Throwable
    {
        try
        {
            assertInvalidThrow(SyntaxException.class, "CREATE KEYSPACE testABC WITH replication = {'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 2, '" + DATA_CENTER + "' : 3 }");
        }
        finally
        {
            // clean-up
            execute("DROP KEYSPACE IF EXISTS testABC");
        }
    }

    /**
     * Test create and drop table
     * migrated from cql_tests.py:TestCQL.table_test()
     */
    @Test
    public void testTable() throws Throwable
    {
        String table1 = createTable(" CREATE TABLE %s (k int PRIMARY KEY, c int)");
        createTable("CREATE TABLE %s (k int, c int, PRIMARY KEY (k),)");

        String table4 = createTableName();

        // repeated column
        assertInvalidMessage("Duplicate column 'k' declaration for table", String.format("CREATE TABLE %s (k int PRIMARY KEY, c int, k text)", table4));

        execute(String.format("DROP TABLE %s.%s", keyspace(), table1));

        createTable(String.format("CREATE TABLE %s.%s ( k int PRIMARY KEY, c1 int, c2 int, ) ", keyspace(), table1));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multiordering_validation_test()
     */
    @Test
    public void testMultiOrderingValidation() throws Throwable
    {
        String tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 DESC)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC, c3 DESC)", tableName));

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)");
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");
    }

    @Test
    public void testCreateTrigger() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");
        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1");
        execute("CREATE TRIGGER trigger_2 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_2");
        assertInvalid("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        execute("CREATE TRIGGER \"Trigger 3\" ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("Trigger 3");
    }

    @Test
    public void testCreateTriggerIfNotExists() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        execute("CREATE TRIGGER IF NOT EXISTS trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1");

        execute("CREATE TRIGGER IF NOT EXISTS trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1");
    }

    @Test
    public void testDropTrigger() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");

        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1");

        execute("DROP TRIGGER trigger_1 ON %s");
        assertTriggerDoesNotExists("trigger_1");

        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1");

        assertInvalid("DROP TRIGGER trigger_2 ON %s");

        execute("CREATE TRIGGER \"Trigger 3\" ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("Trigger 3");

        execute("DROP TRIGGER \"Trigger 3\" ON %s");
        assertTriggerDoesNotExists("Trigger 3");
    }

    @Test
    public void testDropTriggerIfExists() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");

        execute("DROP TRIGGER IF EXISTS trigger_1 ON %s");
        assertTriggerDoesNotExists("trigger_1");

        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1");

        execute("DROP TRIGGER IF EXISTS trigger_1 ON %s");
        assertTriggerDoesNotExists("trigger_1");
    }

    @Test
    // tests CASSANDRA-4278
    public void testHyphenDatacenters() throws Throwable
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

        // Register an EndpointSnitch which returns fixed values for test.
        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint) { return RACK1; }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint) { return "us-east-1"; }

            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2) { return 0; }
        });

        // this forces the dc above to be added to the list of known datacenters (fixes static init problem
        // with this group of tests), ok to remove at some point if doing so doesn't break the test
        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.0.0.255"));
        execute("CREATE KEYSPACE Foo WITH replication = { 'class' : 'NetworkTopologyStrategy', 'us-east-1' : 1 };");

        // Restore the previous EndpointSnitch
        DatabaseDescriptor.setEndpointSnitch(snitch);

        // clean up
        execute("DROP KEYSPACE IF EXISTS Foo");
    }

    @Test
    // tests CASSANDRA-9565
    public void testDoubleWith() throws Throwable
    {
        String[] stmts = { "CREATE KEYSPACE WITH WITH DURABLE_WRITES = true",
                           "CREATE KEYSPACE ks WITH WITH DURABLE_WRITES = true" };

        for (String stmt : stmts)
            assertInvalidSyntaxMessage("no viable alternative at input 'WITH'", stmt);
    }

    public static class InvalidMemtableFactoryMethod
    {
        @SuppressWarnings("unused")
        public static String factory(Map<String, String> options)
        {
            return "invalid";
        }
    }

    public static class InvalidMemtableFactoryField
    {
        @SuppressWarnings("unused")
        public static String FACTORY = "invalid";
    }

    @Test
    public void testCreateTableWithMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))");
        assertSame(MemtableParams.DEFAULT.factory(), getCurrentColumnFamilyStore().metadata().params.memtable.factory());
        Class<? extends Memtable> defaultClass = getCurrentColumnFamilyStore().getTracker().getView().getCurrentMemtable().getClass();

        assertSchemaOption("memtable", null);

        testMemtableConfig("skiplist", SkipListMemtable.FACTORY, SkipListMemtable.class);
        testMemtableConfig("trie", MemtableParams.get("trie").factory(), TrieMemtable.class);
        testMemtableConfig("skiplist_remapped", SkipListMemtable.FACTORY, SkipListMemtable.class);
        testMemtableConfig("test_fullname", TestMemtable.FACTORY, SkipListMemtable.class);
        testMemtableConfig("test_shortname", SkipListMemtable.FACTORY, SkipListMemtable.class);
        testMemtableConfig("default", MemtableParams.DEFAULT.factory(), defaultClass);

        assertThrowsConfigurationException("The 'class_name' option must be specified.",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH memtable = 'test_empty_class';");

        assertThrowsConfigurationException("The 'class_name' option must be specified.",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH memtable = 'test_missing_class';");

        assertThrowsConfigurationException("Memtable class org.apache.cassandra.db.memtable.SkipListMemtable does not accept any futher parameters, but {invalid=throw} were given.",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH memtable = 'test_invalid_param';");

        assertThrowsConfigurationException("Could not create memtable factory for class NotExisting",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH memtable = 'test_unknown_class';");

        assertThrowsConfigurationException("Memtable class org.apache.cassandra.db.memtable.TestMemtable does not accept any futher parameters, but {invalid=throw} were given.",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH memtable = 'test_invalid_extra_param';");

        assertThrowsConfigurationException("Could not create memtable factory for class " + InvalidMemtableFactoryMethod.class.getName(),
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH memtable = 'test_invalid_factory_method';");

        assertThrowsConfigurationException("Could not create memtable factory for class " + InvalidMemtableFactoryField.class.getName(),
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH memtable = 'test_invalid_factory_field';");

        assertThrowsConfigurationException("Memtable configuration \"unknown\" not found.",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH memtable = 'unknown';");
    }

    private void testMemtableConfig(String memtableConfig, Memtable.Factory factoryInstance, Class<? extends Memtable> memtableClass) throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                    + " WITH memtable = '" + memtableConfig + "';");
        assertSame(factoryInstance, getCurrentColumnFamilyStore().metadata().params.memtable.factory());
        Assert.assertTrue(memtableClass.isInstance(getCurrentColumnFamilyStore().getTracker().getView().getCurrentMemtable()));

        assertSchemaOption("memtable", MemtableParams.DEFAULT.configurationKey().equals(memtableConfig) ? null : memtableConfig);
    }

    void assertSchemaOption(String option, Object expected) throws Throwable
    {
        assertRows(execute(format("SELECT " + option + " FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(expected));
    }

    @Test
    public void testCreateTableWithCompression() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))");
        assertSchemaOption("compression", map("chunk_length_in_kb", "16", "class", "org.apache.cassandra.io.compress.LZ4Compressor"));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");
        assertSchemaOption("compression", map("chunk_length_in_kb", "32", "class", "org.apache.cassandra.io.compress.SnappyCompressor"));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32, 'enabled' : true };");
        assertSchemaOption("compression", map("chunk_length_in_kb", "32", "class", "org.apache.cassandra.io.compress.SnappyCompressor"));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 2 };");
        assertSchemaOption("compression", map("chunk_length_in_kb", "16", "class", "org.apache.cassandra.io.compress.SnappyCompressor", "min_compress_ratio", "2.0"));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                    + " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 1 };");
        assertSchemaOption("compression", map("chunk_length_in_kb", "16", "class", "org.apache.cassandra.io.compress.SnappyCompressor", "min_compress_ratio", "1.0"));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                    + " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 0 };");
        assertSchemaOption("compression", map("chunk_length_in_kb", "16", "class", "org.apache.cassandra.io.compress.SnappyCompressor"));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'enabled' : 'false'};");
        assertSchemaOption("compression", map("enabled", "false"));

        assertThrowsConfigurationException("Missing sub-option 'class' for the 'compression' option.",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = {'chunk_length_in_kb' : 32};");

        assertThrowsConfigurationException("The 'class' option must not be empty. To disable compression use 'enabled' : false",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'class' : ''};");

        assertThrowsConfigurationException("If the 'enabled' option is set to false no other options must be specified",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'enabled' : 'false', 'class' : 'SnappyCompressor'};");

        assertThrowsConfigurationException("If the 'enabled' option is set to false no other options must be specified",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'enabled' : 'false', 'chunk_length_in_kb' : 32};");

        assertThrowsConfigurationException("chunk_length_in_kb must be a power of 2",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 31 };");

        assertThrowsConfigurationException("Invalid negative or null chunk_length_in_kb",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : -1 };");

        assertThrowsConfigurationException("Invalid negative min_compress_ratio",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                            + " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : -1 };");

        assertThrowsConfigurationException("Unknown compression options unknownOption",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                            + " WITH compression = { 'class' : 'SnappyCompressor', 'unknownOption' : 32 };");
    }

    private void assertThrowsConfigurationException(String errorMsg, String createStmt)
    {
        try
        {
            createTable(createStmt);
            fail("Query should be invalid but no error was thrown. Query is: " + createStmt);
        }
        catch (RuntimeException e)
        {
            Throwable cause = e.getCause();
            assertTrue("The exception should be a ConfigurationException", cause instanceof ConfigurationException);
            assertEquals(errorMsg, cause.getMessage());
        }
    }

    private void assertTriggerExists(String name)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace(), currentTable());
        assertTrue("the trigger does not exist", metadata.triggers.get(name).isPresent());
    }

    private void assertTriggerDoesNotExists(String name)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace(), currentTable());
        assertFalse("the trigger exists", metadata.triggers.get(name).isPresent());
    }

    public static class TestTrigger implements ITrigger
    {
        public TestTrigger() { }
        public Collection<Mutation> augment(Partition update)
        {
            return Collections.emptyList();
        }
    }

}
