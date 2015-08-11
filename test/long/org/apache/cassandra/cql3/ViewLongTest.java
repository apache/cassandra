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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.utils.WrappedRunnable;

public class ViewLongTest extends CQLTester
{
    int protocolVersion = 4;
    private final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }
    @Before
    public void begin()
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        for (String viewName : views)
            executeNet(protocolVersion, "DROP MATERIALIZED VIEW " + viewName);
    }

    private void createView(String name, String query) throws Throwable
    {
        executeNet(protocolVersion, String.format(query, name));
        // If exception is thrown, the view will not be added to the list; since it shouldn't have been created, this is
        // the desired behavior
        views.add(name);
    }

    @Test
    public void testConflictResolution() throws Throwable
    {
        final int writers = 96;
        final int insertsPerWriter = 50;
        final Map<Integer, Exception> failedWrites = new ConcurrentHashMap<>();

        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        CyclicBarrier semaphore = new CyclicBarrier(writers);

        Thread[] threads = new Thread[writers];
        for (int i = 0; i < writers; i++)
        {
            final int writer = i;
            Thread t = new Thread(new WrappedRunnable()
            {
                public void runMayThrow()
                {
                    try
                    {
                        int writerOffset = writer * insertsPerWriter;
                        semaphore.await();
                        for (int i = 0; i < insertsPerWriter; i++)
                        {
                            try
                            {
                                executeNet(protocolVersion, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP 1",
                                           1,
                                           1,
                                           i + writerOffset);
                            }
                            catch (NoHostAvailableException|WriteTimeoutException e)
                            {
                                failedWrites.put(i + writerOffset, e);
                            }
                        }
                    }
                    catch (Throwable e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });
            t.start();
            threads[i] = t;
        }

        for (int i = 0; i < writers; i++)
            threads[i].join();

        for (int i = 0; i < writers * insertsPerWriter; i++)
        {
            if (executeNet(protocolVersion, "SELECT COUNT(*) FROM system.batchlog").one().getLong(0) == 0)
                break;
            try
            {
                // This will throw exceptions whenever there are exceptions trying to push the view values out, caused
                // by the view becoming overwhelmed.
                BatchlogManager.instance.startBatchlogReplay().get();
            }
            catch (Throwable ignore)
            {

            }
        }

        int value = executeNet(protocolVersion, "SELECT c FROM %s WHERE a = 1 AND b = 1").one().getInt("c");

        List<Row> rows = executeNet(protocolVersion, "SELECT c FROM " + keyspace() + ".mv").all();

        boolean containsC = false;
        StringBuilder others = new StringBuilder();
        StringBuilder overlappingFailedWrites = new StringBuilder();
        for (Row row : rows)
        {
            int c = row.getInt("c");
            if (c == value)
                containsC = true;
            else
            {
                if (others.length() != 0)
                    others.append(' ');
                others.append(c);
                if (failedWrites.containsKey(c))
                {
                    if (overlappingFailedWrites.length() != 0)
                        overlappingFailedWrites.append(' ');
                    overlappingFailedWrites.append(c)
                                           .append(':')
                                           .append(failedWrites.get(c).getMessage());
                }
            }
        }

        if (rows.size() > 1)
        {
            throw new AssertionError(String.format("Expected 1 row, but found %d; %s c = %d, and (%s) of which (%s) failed to insert", rows.size(), containsC ? "found row with" : "no rows contained", value, others, overlappingFailedWrites));
        }
        else if (rows.isEmpty())
        {
            throw new AssertionError(String.format("Could not find row with c = %d", value));
        }
        else if (rows.size() == 1 && !containsC)
        {
            throw new AssertionError(String.format("Single row had c = %d, expected %d", rows.get(0).getInt("c"), value));
        }
    }
}
