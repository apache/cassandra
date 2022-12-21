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
package org.apache.cassandra.tools.nodetool;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;

public class CompactTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void keyPresent() throws Throwable
    {
        long token = 42;
        long key = Murmur3Partitioner.LongToken.keyForToken(token).getLong();
        createTable("CREATE TABLE %s (id bigint, value text, PRIMARY KEY ((id)))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        cfs.disableAutoCompaction();
        // write SSTables for the specific key
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (id, value) VALUES (?, ?)", key, "This is just some text... part " + i);
            flush(keyspace());
        }
        Assertions.assertThat(cfs.getTracker().getView().liveSSTables()).hasSize(10);
        invokeNodetool("compact", "--partition", Long.toString(key), keyspace(), currentTable()).assertOnCleanExit();

        // only 1 SSTable should exist
        Assertions.assertThat(cfs.getTracker().getView().liveSSTables()).hasSize(1);
    }

    @Test
    public void keyNotPresent() throws Throwable
    {
        long token = 42;
        long key = Murmur3Partitioner.LongToken.keyForToken(token).getLong();
        createTable("CREATE TABLE %s (id bigint, value text, PRIMARY KEY ((id)))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        cfs.disableAutoCompaction();
        // write SSTables for the specific key
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (id, value) VALUES (?, ?)", key, "This is just some text... part " + i);
            flush(keyspace());
        }
        Assertions.assertThat(cfs.getTracker().getView().liveSSTables()).hasSize(10);

        for (long keyNotFound : Arrays.asList(key - 1, key + 1))
        {
            invokeNodetool("compact", "--partition", Long.toString(keyNotFound), keyspace(), currentTable()).assertOnCleanExit();

            // only 1 SSTable should exist
            Assertions.assertThat(cfs.getTracker().getView().liveSSTables()).hasSize(10);
        }
    }

    @Test
    public void tableNotFound()
    {
        invokeNodetool("compact", "--partition", Long.toString(42), keyspace(), "doesnotexist")
        .asserts()
        .failure()
        .errorContains(String.format("java.lang.IllegalArgumentException: Unknown keyspace/cf pair (%s.doesnotexist)", keyspace()));
    }

    @Test
    public void keyWrongType()
    {
        createTable("CREATE TABLE %s (id bigint, value text, PRIMARY KEY ((id)))");

        invokeNodetool("compact", "--partition", "this_will_not_work", keyspace(), currentTable())
        .asserts()
        .failure()
        .errorContains(String.format("Unable to parse partition key 'this_will_not_work' for table %s.%s; Unable to make long from 'this_will_not_work'", keyspace(), currentTable()));
    }
}
