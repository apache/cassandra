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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.gms.Gossiper;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

/**
 * Test that TombstoneOverwhelmingException gets thrown when it should be and doesn't when it shouldn't be.
 */
public class SliceQueryFilterWithTombstonesTest
{
    static final String KEYSPACE = "tombstone_overwhelming_exception_test";
    static final String TABLE = "overwhelmed";

    static final int ORIGINAL_THRESHOLD = DatabaseDescriptor.getTombstoneFailureThreshold();
    static final int THRESHOLD = 100;

    @BeforeClass
    public static void setUp() throws Throwable
    {
        SchemaLoader.loadSchema();

        process(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
                              KEYSPACE),
                ConsistencyLevel.ONE);

        process(String.format("CREATE TABLE IF NOT EXISTS %s.%s (a text, b text, c text, PRIMARY KEY (a, b));",
                              KEYSPACE,
                              TABLE),
                ConsistencyLevel.ONE);

        DatabaseDescriptor.setTombstoneFailureThreshold(THRESHOLD);
    }

    @AfterClass
    public static void tearDown()
    {
        Gossiper.instance.stop();

        DatabaseDescriptor.setTombstoneFailureThreshold(ORIGINAL_THRESHOLD);
    }

    private static UntypedResultSet execute(String query)
    {
        return processInternal(String.format(query, KEYSPACE, TABLE));
    }

    @Test
    public void testBelowThresholdSelect()
    {
        // insert exactly the amount of tombstones that shouldn't trigger an exception
        for (int i = 0; i < THRESHOLD; i++)
            execute("INSERT INTO %s.%s (a, b, c) VALUES ('key1', 'column" + i + "', null);");

        try
        {
            execute("SELECT * FROM %s.%s WHERE a = 'key1';");
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones below the threshold should not have failed, but has: " + e);
        }
    }

    @Test
    public void testBeyondThresholdSelect()
    {
        // insert exactly the amount of tombstones that *SHOULD* trigger an exception
        for (int i = 0; i < THRESHOLD + 1; i++)
            execute("INSERT INTO %s.%s (a, b, c) VALUES ('key2', 'column" + i + "', null);");

        try
        {
            execute("SELECT * FROM %s.%s WHERE a = 'key2';");
            fail("SELECT with tombstones beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            assertTrue(e instanceof TombstoneOverwhelmingException);
        }
    }

    @Test
    public void testAllShadowedSelect()
    {
        // insert exactly the amount of tombstones that *SHOULD* normally trigger an exception
        for (int i = 0; i < THRESHOLD + 1; i++)
            execute("INSERT INTO %s.%s (a, b, c) VALUES ('key3', 'column" + i + "', null);");

        // delete all with a partition level tombstone
        execute("DELETE FROM %s.%s WHERE a = 'key3'");

        try
        {
            execute("SELECT * FROM %s.%s WHERE a = 'key3';");
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones shadowed by a partition tombstone should not have failed, but has: " + e);
        }
    }

    @Test
    public void testLiveShadowedCellsSelect()
    {
        for (int i = 0; i < THRESHOLD + 1; i++)
            execute("INSERT INTO %s.%s (a, b, c) VALUES ('key4', 'column" + i + "', 'column');");

        // delete all with a partition level tombstone
        execute("DELETE FROM %s.%s WHERE a = 'key4'");

        try
        {
            execute("SELECT * FROM %s.%s WHERE a = 'key4';");
        }
        catch (Throwable e)
        {
            fail("SELECT with regular cells shadowed by a partition tombstone should not have failed, but has: " + e);
        }
    }
}
