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
package org.apache.cassandra.cql3.validation.entities;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.CQLTester;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TypeTest extends CQLTester
{
    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidMessage("No user type named", "DROP TYPE " + KEYSPACE + ".type_does_not_exist");
        assertInvalidMessage("Cannot drop type in unknown keyspace", "DROP TYPE keyspace_does_not_exist.type_does_not_exist");

        execute("DROP TYPE IF EXISTS " + KEYSPACE + ".type_does_not_exist");
        execute("DROP TYPE IF EXISTS keyspace_does_not_exist.type_does_not_exist");
    }

    @Test
    public void testNowToUUIDCompatibility() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b uuid, PRIMARY KEY (a, b))");
        execute("INSERT INTO %s (a, b) VALUES (0, now())");
        UntypedResultSet results = execute("SELECT * FROM %s WHERE a=0 AND b < now()");
        assertEquals(1, results.size());
    }

    @Test
    public void testDateCompatibility() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b timestamp, c bigint, d varint, PRIMARY KEY (a, b, c, d))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (0, toUnixTimestamp(now()), toTimestamp(now()), toTimestamp(now()))");
        UntypedResultSet results = execute("SELECT * FROM %s WHERE a=0 AND b <= toUnixTimestamp(now())");
        assertEquals(1, results.size());

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, unixTimestampOf(now()), dateOf(now()), dateOf(now()))");
        results = execute("SELECT * FROM %s WHERE a=1 AND b <= toUnixTimestamp(now())");
        assertEquals(1, results.size());
    }

    @Test
    public void testReversedTypeCompatibility() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b timeuuid, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        execute("INSERT INTO %s (a, b) VALUES (0, now())");
        UntypedResultSet results = execute("SELECT * FROM %s WHERE a=0 AND b < now()");
        assertEquals(1, results.size());
    }

    @Test
    // tests CASSANDRA-7797
    public void testAlterReversedColumn() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (a int, b 'org.apache.cassandra.db.marshal.DateType', PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        alterTable("ALTER TABLE %s ALTER b TYPE 'org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)'");
    }

    @Test
    public void testIncompatibleReversedTypes() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (a int, b 'org.apache.cassandra.db.marshal.DateType', PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        try
        {
            alterTable("ALTER TABLE %s ALTER b TYPE 'org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimeUUIDType)'");
            fail("Expected error for ALTER statement");
        }
        catch (RuntimeException e) { }
    }

    @Test
    public void testReversedAndNonReversed() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (a int, b 'org.apache.cassandra.db.marshal.DateType', PRIMARY KEY (a, b))");
        try
        {
            alterTable("ALTER TABLE %s ALTER b TYPE 'org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.DateType)'");
            fail("Expected error for ALTER statement");
        }
        catch (RuntimeException e) { }
    }
}
