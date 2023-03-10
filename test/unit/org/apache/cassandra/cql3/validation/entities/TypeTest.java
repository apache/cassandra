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

public class TypeTest extends CQLTester
{
    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidMessage(String.format("Type '%s.type_does_not_exist' doesn't exist", KEYSPACE), "DROP TYPE " + KEYSPACE + ".type_does_not_exist");
        assertInvalidMessage("Type 'keyspace_does_not_exist.type_does_not_exist' doesn't exist", "DROP TYPE keyspace_does_not_exist.type_does_not_exist");

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

        execute("INSERT INTO %s (a, b, c, d) VALUES (0, to_unix_timestamp(now()), to_timestamp(now()), to_timestamp(now()))");
        UntypedResultSet results = execute("SELECT * FROM %s WHERE a=0 AND b <= to_unix_timestamp(now())");
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
}
