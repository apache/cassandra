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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.String.format;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.apache.cassandra.cql3.Duration.*;
import static org.junit.Assert.assertEquals;

public class CreateTest extends CQLTester
{
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
        assertInvalidMessage("duration type is not supported for PRIMARY KEY part a",
                             "CREATE TABLE test (a duration PRIMARY KEY, b int);");

        assertInvalidMessage("duration type is not supported for PRIMARY KEY part b",
                             "CREATE TABLE test (a text, b duration, c duration, primary key (a, b));");

        assertInvalidMessage("duration type is not supported for PRIMARY KEY part b",
                             "CREATE TABLE test (a text, b duration, c duration, primary key (a, b)) with clustering order by (b DESC);");

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
                             "CREATE TABLE test(pk int PRIMARY KEY, m map<duration, text>)");

        createTable("CREATE TABLE %s(pk int PRIMARY KEY, m map<text, duration>)");
        execute("INSERT INTO %s (pk, m) VALUES (1, {'one month' : 1mo, '60 days' : 60d})");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, map("one month", Duration.from("1mo"), "60 days", Duration.from("60d"))));

        assertInvalidMessage("duration type is not supported for PRIMARY KEY part m",
                "CREATE TABLE %s(m frozen<map<text, duration>> PRIMARY KEY, v int)");

        assertInvalidMessage("duration type is not supported for PRIMARY KEY part m",
                             "CREATE TABLE %s(pk int, m frozen<map<text, duration>>, v int, PRIMARY KEY (pk, m))");

        // Test duration within Set
        assertInvalidMessage("Durations are not allowed inside sets: set<duration>",
                             "CREATE TABLE %s(pk int PRIMARY KEY, s set<duration>)");

        assertInvalidMessage("Durations are not allowed inside sets: frozen<set<duration>>",
                             "CREATE TABLE %s(s frozen<set<duration>> PRIMARY KEY, v int)");

        // Test duration within List
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, l list<duration>)");
        execute("INSERT INTO %s (pk, l) VALUES (1, [1mo, 60d])");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, list(Duration.from("1mo"), Duration.from("60d"))));

        assertInvalidMessage("duration type is not supported for PRIMARY KEY part l",
                             "CREATE TABLE %s(l frozen<list<duration>> PRIMARY KEY, v int)");

        // Test duration within Tuple
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, t tuple<int, duration>)");
        execute("INSERT INTO %s (pk, t) VALUES (1, (1, 1mo))");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, tuple(1, Duration.from("1mo"))));

        assertInvalidMessage("duration type is not supported for PRIMARY KEY part t",
                             "CREATE TABLE %s(t frozen<tuple<int, duration>> PRIMARY KEY, v int)");

        // Test duration within UDT
        String typename = createType("CREATE TYPE %s (a duration)");
        String myType = KEYSPACE + '.' + typename;
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, u " + myType + ")");
        execute("INSERT INTO %s (pk, u) VALUES (1, {a : 1mo})");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, userType("a", Duration.from("1mo"))));

        assertInvalidMessage("duration type is not supported for PRIMARY KEY part u",
                             "CREATE TABLE %s(pk int, u frozen<" + myType + ">, v int, PRIMARY KEY(pk, u))");

        // Test duration with several level of depth
        assertInvalidMessage("duration type is not supported for PRIMARY KEY part m",
                "CREATE TABLE %s(pk int, m frozen<map<text, list<tuple<int, duration>>>>, v int, PRIMARY KEY (pk, m))");
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
        assertInvalidThrow(SyntaxException.class, "CREATE TABLE test (foo text PRIMARY KEY, c int) WITH default_validation=timestamp");

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
        assertInvalidThrow(ConfigurationException.class, "DROP KEYSPACE non_existing");

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
     * Test {@link ConfigurationException} is thrown on create keyspace without any options.
     */
    @Test
    public void testConfigurationExceptionThrownWhenCreateKeyspaceWithNoOptions() throws Throwable
    {
        assertInvalidThrow(ConfigurationException.class, "CREATE KEYSPACE testXYZ with replication = { 'class': 'NetworkTopologyStrategy' }");
        assertInvalidThrow(ConfigurationException.class, "CREATE KEYSPACE testXYZ WITH replication = { 'class' : 'SimpleStrategy' }");
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
        assertInvalidMessage("Multiple definition of identifier k", String.format("CREATE TABLE %s (k int PRIMARY KEY, c int, k text)", table4));
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
            public int compareEndpoints(InetAddressAndPort target, InetAddressAndPort a1, InetAddressAndPort a2) { return 0; }
        });

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

    @Test
    public void testCreateTableWithCompression() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "64", "class", "org.apache.cassandra.io.compress.LZ4Compressor")));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "32", "class", "org.apache.cassandra.io.compress.SnappyCompressor")));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32, 'enabled' : true };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "32", "class", "org.apache.cassandra.io.compress.SnappyCompressor")));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'chunk_length_kb' : 32 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "32", "class", "org.apache.cassandra.io.compress.SnappyCompressor")));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'min_compress_ratio' : 2 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "64", "class", "org.apache.cassandra.io.compress.SnappyCompressor", "min_compress_ratio", "2.0")));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                    + " WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'min_compress_ratio' : 1 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "64", "class", "org.apache.cassandra.io.compress.SnappyCompressor", "min_compress_ratio", "1.0")));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                    + " WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'min_compress_ratio' : 0 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "64", "class", "org.apache.cassandra.io.compress.SnappyCompressor")));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'sstable_compression' : '', 'chunk_length_kb' : 32 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("enabled", "false")));

        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                + " WITH compression = { 'enabled' : 'false'};");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("enabled", "false")));

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

        assertThrowsConfigurationException("The 'sstable_compression' option must not be used if the compression algorithm is already specified by the 'class' option",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'class' : 'SnappyCompressor'};");

        assertThrowsConfigurationException("The 'chunk_length_kb' option must not be used if the chunk length is already specified by the 'chunk_length_in_kb' option",
                                           "CREATE TABLE %s (a text, b int, c int, primary key (a, b))"
                                           + " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_kb' : 32 , 'chunk_length_in_kb' : 32 };");

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

    @Test
    public void compactTableTest() throws Throwable
    {
        assertInvalidMessage("Compact tables are not allowed in Cassandra starting with 4.0 version.",
                             "CREATE TABLE compact_table_create (id text PRIMARY KEY, content text) WITH COMPACT STORAGE;");
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
