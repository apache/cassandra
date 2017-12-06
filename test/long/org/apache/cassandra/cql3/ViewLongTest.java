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
import java.util.Arrays;
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
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

public class ViewLongTest extends CQLTester
{
    ProtocolVersion protocolVersion = ProtocolVersion.V4;
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
            Thread t = NamedThreadFactory.createThread(new WrappedRunnable()
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

    @Test
    public void testExpiredLivenessInfoWithDefaultTTLWithFlush() throws Throwable
    {
        testExpiredLivenessInfoWithDefaultTTL(true);
    }

    @Test
    public void testExpiredLivenessInfoWithDefaultTTLWithoutFlush() throws Throwable
    {
        testExpiredLivenessInfoWithDefaultTTL(false);
    }

    private void testExpiredLivenessInfoWithDefaultTTL(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (field1 int,field2 int,date int,PRIMARY KEY ((field1), field2)) WITH default_time_to_live = 5;");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW mv AS SELECT * FROM %%s WHERE field1 IS NOT NULL AND field2 IS NOT NULL AND date IS NOT NULL PRIMARY KEY ((field1), date, field2) WITH CLUSTERING ORDER BY (date desc, field2 asc);");

        updateViewWithFlush("insert into %s (field1, field2, date) values (1, 2, 111);", flush);
        assertRows(execute("select * from %s"), row(1, 2, 111));
        assertRows(execute("select * from mv"), row(1, 111, 2));

        updateViewWithFlush("insert into %s (field1, field2, date) values (1, 2, 222);", flush);
        assertRows(execute("select * from %s"), row(1, 2, 222));
        assertRows(execute("select * from mv"), row(1, 222, 2));

        updateViewWithFlush("insert into %s (field1, field2, date) values (1, 2, 333);", flush);

        assertRows(execute("select * from %s"), row(1, 2, 333));
        assertRows(execute("select * from mv"), row(1, 333, 2));

        if (flush)
        {
            Keyspace.open(keyspace()).getColumnFamilyStore("mv").forceMajorCompaction();
            assertRows(execute("select * from %s"), row(1, 2, 333));
            assertRows(execute("select * from mv"), row(1, 333, 2));
        }

        // wait for ttl, data should be removed
        updateViewWithFlush("insert into %s (field1, field2, date) values (1, 2, 444);", flush);
        assertRows(execute("select * from %s"), row(1, 2, 444));
        assertRows(execute("select * from mv"), row(1, 444, 2));

        Thread.sleep(5000);
        assertRows(execute("select * from %s"));
        assertRows(execute("select * from mv"));

        // shadow mv with date=555 and then update it back to live, wait for ttl
        updateView("update %s set date=555 where field1=1 and field2=2;");
        updateView("update %s set date=666 where field1=1 and field2=2;");
        updateViewWithFlush("update %s set date=555 where field1=1 and field2=2;", flush);
        assertRows(execute("select * from %s"), row(1, 2, 555));
        assertRows(execute("select * from mv"), row(1, 555, 2));

        Thread.sleep(5000);
        assertRows(execute("select * from %s"));
        assertRows(execute("select * from mv"));

        // test user-provided ttl for table with/without default-ttl
        for (boolean withDefaultTTL : Arrays.asList(true, false))
        {
            execute("TRUNCATE %s");
            if (withDefaultTTL)
                execute("ALTER TABLE %s with default_time_to_live=" + (withDefaultTTL ? 10 : 0));
            updateViewWithFlush("insert into %s (field1, field2, date) values (1, 2, 666) USING TTL 1000;", flush);

            assertRows(execute("select * from %s"), row(1, 2, 666));
            assertRows(execute("select * from mv"), row(1, 666, 2));

            updateViewWithFlush("insert into %s (field1, field2, date) values (1, 2, 777) USING TTL 1100;", flush);
            assertRows(execute("select * from %s"), row(1, 2, 777));
            assertRows(execute("select * from mv"), row(1, 777, 2));

            updateViewWithFlush("insert into %s (field1, field2, date) values (1, 2, 888) USING TTL 800;", flush);

            assertRows(execute("select * from %s"), row(1, 2, 888));
            assertRows(execute("select * from mv"), row(1, 888, 2));

            if (flush)
            {
                Keyspace.open(keyspace()).getColumnFamilyStore("mv").forceMajorCompaction();
                assertRows(execute("select * from %s"), row(1, 2, 888));
                assertRows(execute("select * from mv"), row(1, 888, 2));
            }

            // wait for ttl, data should be removed
            updateViewWithFlush("insert into %s (field1, field2, date) values (1, 2, 999) USING TTL 5;", flush);
            assertRows(execute("select * from %s"), row(1, 2, 999));
            assertRows(execute("select * from mv"), row(1, 999, 2));

            Thread.sleep(5000);
            assertRows(execute("select * from %s"));
            assertRows(execute("select * from mv"));

            // shadow mv with date=555 and then update it back to live with ttl=5, wait for ttl to expire
            updateViewWithFlush("update %s  USING TTL 800 set date=555 where field1=1 and field2=2;", flush);
            assertRows(execute("select * from %s"), row(1, 2, 555));
            assertRows(execute("select * from mv"), row(1, 555, 2));

            updateViewWithFlush("update %s set date=666 where field1=1 and field2=2;", flush);
            assertRows(execute("select * from %s"), row(1, 2, 666));
            assertRows(execute("select * from mv"), row(1, 666, 2));

            updateViewWithFlush("update %s USING TTL 5 set date=555 where field1=1 and field2=2;", flush);
            assertRows(execute("select * from %s"), row(1, 2, 555));
            assertRows(execute("select * from mv"), row(1, 555, 2));

            Thread.sleep(5000);
            assertRows(execute("select * from %s"));
            assertRows(execute("select * from mv"));
        }
    }

    @Test
    public void testExpiredLivenessInfoWithUnselectedColumnAndDefaultTTLWithFlush() throws Throwable
    {
        testExpiredLivenessInfoWithUnselectedColumnAndDefaultTTL(true);
    }

    @Test
    public void testExpiredLivenessInfoWithUnselectedColumnAndDefaultTTLWithoutFlush() throws Throwable
    {
        testExpiredLivenessInfoWithUnselectedColumnAndDefaultTTL(false);
    }

    private void testExpiredLivenessInfoWithUnselectedColumnAndDefaultTTL(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (k int,c int,a int, b int, PRIMARY KEY ((k), c)) WITH default_time_to_live = 1000;");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW mv AS SELECT k,c,a FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL "
                           + "PRIMARY KEY (c, k)");

        // table default ttl
        updateViewWithFlush("UPDATE %s SET b = 111 WHERE k = 1 AND c = 2", flush);
        assertRows(execute("select k,c,a,b from %s"), row(1, 2, null, 111));
        assertRows(execute("select k,c,a from mv"), row(1, 2, null));

        updateViewWithFlush("UPDATE %s SET b = null WHERE k = 1 AND c = 2", flush);
        assertRows(execute("select k,c,a,b from %s"));
        assertRows(execute("select k,c,a from mv"));

        updateViewWithFlush("UPDATE %s SET b = 222 WHERE k = 1 AND c = 2", flush);
        assertRows(execute("select k,c,a,b from %s"), row(1, 2, null, 222));
        assertRows(execute("select k,c,a from mv"), row(1, 2, null));

        updateViewWithFlush("DELETE b FROM %s WHERE k = 1 AND c = 2", flush);
        assertRows(execute("select k,c,a,b from %s"));
        assertRows(execute("select k,c,a from mv"));

        if (flush)
        {
            Keyspace.open(keyspace()).getColumnFamilyStore("mv").forceMajorCompaction();
            assertRows(execute("select k,c,a,b from %s"));
            assertRows(execute("select k,c,a from mv"));
        }

        // test user-provided ttl for table with/without default-ttl
        for (boolean withDefaultTTL : Arrays.asList(true, false))
        {
            execute("TRUNCATE %s");
            if (withDefaultTTL)
                execute("ALTER TABLE %s with default_time_to_live=" + (withDefaultTTL ? 10 : 0));

            updateViewWithFlush("UPDATE %s USING TTL 100 SET b = 666 WHERE k = 1 AND c = 2", flush);
            assertRows(execute("select k,c,a,b from %s"), row(1, 2, null, 666));
            assertRows(execute("select k,c,a from mv"), row(1, 2, null));

            updateViewWithFlush("UPDATE %s USING TTL 90  SET b = null WHERE k = 1 AND c = 2", flush);
            if (flush)
                FBUtilities.waitOnFutures(Keyspace.open(keyspace()).flush());
            assertRows(execute("select k,c,a,b from %s"));
            assertRows(execute("select k,c,a from mv"));

            updateViewWithFlush("UPDATE %s USING TTL 80  SET b = 777 WHERE k = 1 AND c = 2", flush);
            assertRows(execute("select k,c,a,b from %s"), row(1, 2, null, 777));
            assertRows(execute("select k,c,a from mv"), row(1, 2, null));

            updateViewWithFlush("DELETE b FROM %s WHERE k = 1 AND c = 2", flush);
            assertRows(execute("select k,c,a,b from %s"));
            assertRows(execute("select k,c,a from mv"));

            updateViewWithFlush("UPDATE %s USING TTL 110  SET b = 888 WHERE k = 1 AND c = 2", flush);
            assertRows(execute("select k,c,a,b from %s"), row(1, 2, null, 888));
            assertRows(execute("select k,c,a from mv"), row(1, 2, null));

            updateViewWithFlush("UPDATE %s USING TTL 5  SET b = 999 WHERE k = 1 AND c = 2", flush);
            assertRows(execute("select k,c,a,b from %s"), row(1, 2, null, 999));
            assertRows(execute("select k,c,a from mv"), row(1, 2, null));

            Thread.sleep(5000); // wait for ttl expired

            if (flush)
            {
                Keyspace.open(keyspace()).getColumnFamilyStore("mv").forceMajorCompaction();
                assertRows(execute("select k,c,a,b from %s"));
                assertRows(execute("select k,c,a from mv"));
            }
        }
    }

    private void updateView(String query, Object... params) throws Throwable
    {
        updateViewWithFlush(query, false, params);
    }

    private void updateViewWithFlush(String query, boolean flush, Object... params) throws Throwable
    {
        executeNet(protocolVersion, query, params);
        while (!(((SEPExecutor) StageManager.getStage(Stage.VIEW_MUTATION)).getPendingTasks() == 0
                && ((SEPExecutor) StageManager.getStage(Stage.VIEW_MUTATION)).getActiveCount() == 0))
        {
            Thread.sleep(1);
        }
        if (flush)
            Keyspace.open(keyspace()).flush();
    }
}
