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
package org.apache.cassandra.cql3;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;

import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;

/**
 * Base class for CQL tests.
 */
public class DropKeyspaceCommitLogRecycleTest
{
    protected static final Logger logger = LoggerFactory.getLogger(DropKeyspaceCommitLogRecycleTest.class);

    private static final String KEYSPACE = "cql_test_keyspace";
    private static final String KEYSPACE2 = "cql_test_keyspace2";

    static
    {
        // Once per-JVM is enough
        SchemaLoader.prepareServer();
    }

    private void create(boolean both)
    {
        executeOnceInternal(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        executeOnceInternal(String.format("CREATE TABLE %s.test (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))", KEYSPACE));
        
        if (both)
        {
            executeOnceInternal(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE2));
            executeOnceInternal(String.format("CREATE TABLE %s.test (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))", KEYSPACE2));
        }
    }

    private void insert()
    {
        executeOnceInternal(String.format("INSERT INTO %s.test (k1, k2, v) VALUES (0, 0, 0)", KEYSPACE));
        executeOnceInternal(String.format("INSERT INTO %s.test (k1, k2, v) VALUES (1, 1, 1)", KEYSPACE));
        executeOnceInternal(String.format("INSERT INTO %s.test (k1, k2, v) VALUES (2, 2, 2)", KEYSPACE));

        executeOnceInternal(String.format("INSERT INTO %s.test (k1, k2, v) VALUES (0, 0, 0)", KEYSPACE2));
        executeOnceInternal(String.format("INSERT INTO %s.test (k1, k2, v) VALUES (1, 1, 1)", KEYSPACE2));
        executeOnceInternal(String.format("INSERT INTO %s.test (k1, k2, v) VALUES (2, 2, 2)", KEYSPACE2));       
    }

    private void drop(boolean both)
    {
        executeOnceInternal(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE));
        if (both)
            executeOnceInternal(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE2));
    }

    @Test
    public void testRecycle()
    {
        for (int i = 0 ; i < 1000 ; i++)
        {
            create(i == 0);
            insert();
            drop(false);
        }
    }

    @After
    public void afterTest() throws Throwable
    {
        drop(true);
    }
}
