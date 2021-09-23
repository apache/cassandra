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

package org.apache.cassandra.service;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.schema.TableMetadata;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;

/**
 * Tests that the methods of the {@link QueryInfoTracker} interface are correctly called.
 *
 * <p>The tests below use "drivers" sessions to ensure that queries go through {@link StorageProxy}, where
 * {@link QueryInfoTracker} is setup.
 */
@RunWith(BMUnitRunner.class)
public class QueryInfoTrackerTest extends CQLTester
{
    private volatile TestQueryInfoTracker tracker;
    private volatile Session session;

    @Before
    public void setupTest()
    {
        tracker = new TestQueryInfoTracker(KEYSPACE);
        StorageProxy.instance.registerQueryTracker(tracker);
        requireNetwork();
        session = sessionNet();
    }

    @Test
    public void testSimpleQueryTracing()
    {
        int keys = 4;
        int clustering = 4;
        String table = KEYSPACE + ".qit_simple";
        session.execute("CREATE TABLE " + table + "(k int, c int, v int, PRIMARY KEY (k, c))");
        for (int k = 0; k < keys; k++)
        {
            for (int v = 0; v < clustering; v++)
            {
                session.execute("INSERT INTO " + table + "(k, c) values (?, ?)", k, v);
            }
        }

        int expectedWrites = keys * clustering;
        int expectedRows = keys * clustering;
        assertEquals(expectedWrites, tracker.writes.get());
        assertEquals(expectedRows, tracker.writtenRows.get());
        assertEquals(0, tracker.loggedWrites.get());

        assertEquals(0, tracker.reads.get());
        session.execute("SELECT * FROM " + table + " WHERE k = ?", 0);
        assertEquals(1, tracker.reads.get());
        assertEquals(clustering, tracker.readRows.get());
        assertEquals(1, tracker.readPartitions.get());
        assertEquals(1, tracker.replicaPlans.get());

        assertEquals(0, tracker.rangeReads.get());
        session.execute("SELECT * FROM " + table);
        assertEquals(1, tracker.rangeReads.get());
        assertEquals(clustering + keys * clustering, tracker.readRows.get());
        assertEquals(1 + keys, tracker.readPartitions.get());
        assertEquals(1 + 1, tracker.replicaPlans.get());

        session.execute("UPDATE " + table + " SET v = ? WHERE k = ? AND c IN ?", 42, 0, Arrays.asList(0, 2, 3));
        expectedWrites += 1; // We only did one more write ...
        expectedRows += 3;   // ... but it updates 3 rows.
        assertEquals(expectedWrites, tracker.writes.get());
        assertEquals(expectedRows, tracker.writtenRows.get());
        assertEquals(0, tracker.loggedWrites.get());
    }

    @Test
    public void testLoggedBatchQueryTracing()
    {
        String table = KEYSPACE + ".qit_logged_batch";
        session.execute("CREATE TABLE " + table + "(k int, c int, v int, PRIMARY KEY (k, c))");

        session.execute("BEGIN BATCH "
                        + "INSERT INTO " + table + "(k, c, v) VALUES (0, 0, 0);"
                        + "INSERT INTO " + table + "(k, c, v) VALUES (1, 1, 1);"
                        + "INSERT INTO " + table + "(k, c, v) VALUES (2, 2, 2);"
                        + "INSERT INTO " + table + "(k, c, v) VALUES (3, 3, 3);"
                        + "APPLY BATCH");

        assertEquals(1, tracker.writes.get());
        assertEquals(1, tracker.loggedWrites.get());
        assertEquals(4, tracker.writtenRows.get());

        session.execute("BEGIN BATCH "
                        + "INSERT INTO " + table + "(k, c, v) VALUES (4, 4, 4);"
                        + "INSERT INTO " + table + "(k, c, v) VALUES (5, 5, 5);"
                        + "APPLY BATCH");

        assertEquals(2, tracker.writes.get());
        assertEquals(2, tracker.loggedWrites.get());
        assertEquals(6, tracker.writtenRows.get());
    }

    @Test
    public void testLWTQueryTracing()
    {
        String table = KEYSPACE + ".qit_lwt";
        session.execute("CREATE TABLE " + table + "(k int, c int, v int, PRIMARY KEY (k, c))");

        session.execute("INSERT INTO " + table + "(k, c, v) values (?, ?, ?)", 0, 0, 0);
        session.execute("INSERT INTO " + table + "(k, c, v) values (?, ?, ?)", 0, 1, 1);
        assertEquals(2, tracker.writes.get());
        assertEquals(2, tracker.writtenRows.get());

        session.execute("INSERT INTO " + table + "(k, c, v) values (?, ?, ?) IF NOT EXISTS", 0, 2, 2);
        // This should apply, so on top of the lwt specific increases, we'll have a new written row (but no read
        // row since while we will do a read, it will come up empty).
        assertEquals(1, tracker.lwts.get());
        assertEquals(0, tracker.nonAppliedLwts.get());
        assertEquals(1, tracker.appliedLwts.get());
        assertEquals(3, tracker.writtenRows.get());
        assertEquals(0, tracker.readRows.get());
        // The writes or reads shouldn't have changed though.
        assertEquals(2, tracker.writes.get());
        assertEquals(0, tracker.reads.get());
        assertEquals(1, tracker.replicaPlans.get());

        session.execute("INSERT INTO " + table + "(k, c, v) values (?, ?, ?) IF NOT EXISTS", 0, 2, 2);
        // This should not apply now and on top of the lwt specific increases, we should have read 1 additional row.
        assertEquals(2, tracker.lwts.get());
        assertEquals(1, tracker.nonAppliedLwts.get());
        assertEquals(1, tracker.appliedLwts.get());
        assertEquals(3, tracker.writtenRows.get());
        assertEquals(1, tracker.readRows.get());
        // The writes or reads shouldn't have changed though.
        assertEquals(2, tracker.writes.get());
        assertEquals(0, tracker.reads.get());
        assertEquals(2, tracker.replicaPlans.get());

        // More complex LWT batch.
        session.execute("BEGIN BATCH "
                        + "UPDATE " + table + " SET v = 42 WHERE k = 0 AND c = 0 IF v = 0; "
                        + "UPDATE " + table + " SET v = 42 WHERE k = 0 AND c = 1 IF v = 1; "
                        + "APPLY BATCH");
        // This should apply. Further this will have read 2 rows and written 2.
        assertEquals(3, tracker.lwts.get());
        assertEquals(1, tracker.nonAppliedLwts.get());
        assertEquals(2, tracker.appliedLwts.get());
        assertEquals(5, tracker.writtenRows.get());
        // Still no updates of writes or reads expected.
        assertEquals(2, tracker.writes.get());
        assertEquals(0, tracker.reads.get());
        assertEquals(3, tracker.readRows.get());
        assertEquals(3, tracker.replicaPlans.get());


        PreparedStatement statement = session.prepare("SELECT * FROM " + table + " WHERE k = ?")
                                             .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.SERIAL);
        session.execute(statement.bind(0));
        assertEquals(1, tracker.reads.get());
        assertEquals(6, tracker.readRows.get());
        assertEquals(4, tracker.replicaPlans.get());
    }

    @Test
    @BMRule(name = "Simulate cas read failure",
    targetClass = "StorageProxy",
    targetMethod = "doPaxos",
    targetLocation = "AT ENTRY",
    action = "throw new org.apache.cassandra.exceptions.UnavailableException(\"msg\", org.apache.cassandra.db.ConsistencyLevel.SERIAL, 3, 1)")
    public void testCasReadFailureCount()
    {
        String table = KEYSPACE + ".qit_cas_read_failure";
        session.execute("CREATE TABLE " + table + "(k int, c int, v int, PRIMARY KEY (k, c))");
        assertEquals(0, tracker.errorReads.get());

        PreparedStatement statement = session.prepare("SELECT * FROM " + table + " WHERE k = ?")
                                             .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.SERIAL);

        try
        {
            session.execute(statement.bind(0));
        }
        catch (NoHostAvailableException ex)
        { /* NOOP */ }

        assertEquals(1, tracker.errorReads.get());
        assertEquals(0, tracker.reads.get());
        assertEquals(0, tracker.readRows.get());
    }

    @Test
    @BMRule(name = "Simulate cas write failure",
    targetClass = "StorageProxy",
    targetMethod = "doPaxos",
    targetLocation = "AT ENTRY",
    action = "throw new org.apache.cassandra.exceptions.UnavailableException(\"msg\", org.apache.cassandra.db.ConsistencyLevel.SERIAL, 3, 1)")
    public void testCasWriteFailureCount()
    {
        String table = KEYSPACE + ".qit_cas_write_failure";
        session.execute("CREATE TABLE " + table + "(k int, c int, v int, PRIMARY KEY (k, c))");

        assertEquals(0, tracker.errorLwts.get());

        try
        {
            session.execute("INSERT INTO " + table + "(k, c, v) values (?, ?, ?) IF NOT EXISTS", 0, 2, 2);
        }
        catch (NoHostAvailableException ex)
        { /* NOOP */ }

        assertEquals(1, tracker.errorLwts.get());
        assertEquals(0, tracker.lwts.get());
        assertEquals(0, tracker.appliedLwts.get());
        assertEquals(0, tracker.nonAppliedLwts.get());
    }

    @Test
    @BMRule(name = "Simulate fetching rows failure",
    targetClass = "StorageProxy",
    targetMethod = "fetchRows",
    targetLocation = "AT ENTRY",
    action = "throw new org.apache.cassandra.exceptions.UnavailableException(\"msg\", org.apache.cassandra.db.ConsistencyLevel.SERIAL, 3, 1)")
    public void testReadFailureCount()
    {
        String table = KEYSPACE + ".qit_read_failure";
        session.execute("CREATE TABLE " + table + "(k int, c int, v int, PRIMARY KEY (k, c))");

        assertEquals(0, tracker.errorReads.get());

        try
        {
            session.execute("SELECT * FROM " + table + " WHERE k = ?", 0);
        }
        catch (NoHostAvailableException ex)
        { /* NOOP */ }

        assertEquals(1, tracker.errorReads.get());
        assertEquals(0, tracker.reads.get());
    }

    @Test
    @BMRule(name = "Simulate write failure",
    targetClass = "StorageProxy",
    targetMethod = "performWrite",
    targetLocation = "AT ENTRY",
    action = "throw new org.apache.cassandra.exceptions.UnavailableException(\"msg\", org.apache.cassandra.db.ConsistencyLevel.SERIAL, 3, 1)")
    public void testWriteFailureCount()
    {
        String table = KEYSPACE + ".qit_write_failure";
        session.execute("CREATE TABLE " + table + "(k int, c int, v int, PRIMARY KEY (k, c))");
        assertEquals(0, tracker.errorWrites.get());

        try
        {
            session.execute("INSERT INTO " + table + "(k, c, v) values (?, ?, ?)", 0, 0, 0);
        }
        catch (NoHostAvailableException ex)
        { /* NOOP */ }

        assertEquals(1, tracker.errorWrites.get());
        assertEquals(0, tracker.writes.get());
    }

    @Test
    @BMRule(name = "Simulate write batch failure",
    targetClass = "StorageProxy",
    targetMethod = "syncWriteBatchedMutations",
    targetLocation = "AT ENTRY",
    action = "throw new org.apache.cassandra.exceptions.UnavailableException(\"msg\", org.apache.cassandra.db.ConsistencyLevel.SERIAL, 3, 1)")
    public void testReadBatchFailureCount()
    {
        String table = KEYSPACE + ".qit_write_batch_failure";
        session.execute("CREATE TABLE " + table + "(k int, c int, v int, PRIMARY KEY (k, c))");

        assertEquals(0, tracker.errorWrites.get());

        try
        {
            session.execute("BEGIN BATCH "
                            + "INSERT INTO " + table + "(k, c, v) VALUES (0, 0, 0);"
                            + "INSERT INTO " + table + "(k, c, v) VALUES (1, 1, 1);"
                            + "APPLY BATCH");
        }
        catch (NoHostAvailableException ex)
        { /* NOOP */ }

        assertEquals(1, tracker.errorWrites.get());
        assertEquals(0, tracker.reads.get());
    }

    @Test
    public void testReplicaFilteringProtection() throws Throwable
    {
        createTable("CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT)");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0');");
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '0');");
        flush();

        StorageProxy.instance.registerQueryTracker(new QueryInfoTrackerTest.TestQueryInfoTracker(KEYSPACE));
        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0 ALLOW FILTERING");
        assertEquals(2, rows.all().size());

        QueryInfoTrackerTest.TestQueryInfoTracker queryInfoTracker = (QueryInfoTrackerTest.TestQueryInfoTracker)
                                                                     StorageProxy.queryTracker();

        assertEquals(1, queryInfoTracker.rangeReads.get());
        assertEquals(2, queryInfoTracker.readRows.get());
        assertEquals(1, queryInfoTracker.replicaPlans.get());
    }

    public static class TestQueryInfoTracker implements QueryInfoTracker, Serializable
    {
        public final AtomicInteger writes = new AtomicInteger();
        public final AtomicInteger loggedWrites = new AtomicInteger();
        public final AtomicInteger writtenRows = new AtomicInteger();
        public final AtomicInteger errorWrites = new AtomicInteger();

        public final AtomicInteger reads = new AtomicInteger();
        public final AtomicInteger rangeReads = new AtomicInteger();
        public final AtomicInteger readRows = new AtomicInteger();
        public final AtomicInteger readPartitions = new AtomicInteger();
        public final AtomicInteger errorReads = new AtomicInteger();
        public final AtomicInteger replicaPlans = new AtomicInteger();

        public final AtomicInteger lwts = new AtomicInteger();
        public final AtomicInteger nonAppliedLwts = new AtomicInteger();
        public final AtomicInteger appliedLwts = new AtomicInteger();
        public final AtomicInteger errorLwts = new AtomicInteger();
        private final String keyspace;

        public TestQueryInfoTracker(String keyspace)
        {
            this.keyspace = keyspace;
        }

        private boolean shouldIgnore(TableMetadata table)
        {
            // We exclude anything that isn't on our test keyspace to be sure no "system" query interferes.
            return !table.keyspace.equals(keyspace);
        }

        private TableMetadata extractTable(Collection<? extends IMutation> mutations)
        {
            return Iterables.getLast(mutations).getPartitionUpdates().iterator().next().metadata();
        }

        @Override
        public WriteTracker onWrite(ClientState state,
                                    boolean isLogged,
                                    Collection<? extends IMutation> mutations,
                                    ConsistencyLevel consistencyLevel)
        {
            if (shouldIgnore(extractTable(mutations)))
                return WriteTracker.NOOP;

            return new WriteTracker()
            {
                @Override
                public void onDone()
                {
                    writes.incrementAndGet();
                    if (isLogged)
                        loggedWrites.incrementAndGet();
                    for (IMutation mutation : mutations)
                    {
                        for (PartitionUpdate update : mutation.getPartitionUpdates())
                        {
                            writtenRows.addAndGet(update.rowCount());
                        }
                    }
                }

                @Override
                public void onError(Throwable exception)
                {
                    errorWrites.incrementAndGet();
                }
            };
        }

        @Override
        public ReadTracker onRead(ClientState state,
                                  TableMetadata table,
                                  List<SinglePartitionReadCommand> commands,
                                  ConsistencyLevel consistencyLevel)
        {
            if (shouldIgnore(table))
                return ReadTracker.NOOP;
            return new TestReadTracker();
        }

        @Override
        public ReadTracker onRangeRead(ClientState state,
                                       TableMetadata table,
                                       PartitionRangeReadCommand command,
                                       ConsistencyLevel consistencyLevel)
        {
            if (shouldIgnore(table))
                return ReadTracker.NOOP;
            return new TestRangeReadTracker();
        }

        @Override
        public LWTWriteTracker onLWTWrite(ClientState state,
                                          TableMetadata table,
                                          DecoratedKey key,
                                          ConsistencyLevel serialConsistency,
                                          ConsistencyLevel commitConsistency)
        {
            return new TestLWTWriteTracker();
        }


        private class TestReadTracker implements ReadTracker
        {
            @Override
            public void onDone()
            {
                reads.incrementAndGet();
            }

            @Override
            public void onError(Throwable exception)
            {
                errorReads.incrementAndGet();
            }

            @Override
            public void onReplicaPlan(ReplicaPlan.ForRead<?> replicaPlan)
            {
                replicaPlans.incrementAndGet();
            }

            @Override
            public void onPartition(DecoratedKey partitionKey)
            {
                readPartitions.incrementAndGet();
            }

            @Override
            public void onRow(Row row)
            {
                readRows.incrementAndGet();
            }
        }

        private class TestRangeReadTracker implements ReadTracker
        {
            @Override
            public void onDone()
            {
                rangeReads.incrementAndGet();
            }

            @Override
            public void onError(Throwable exception)
            {
                errorReads.incrementAndGet();
            }

            @Override
            public void onReplicaPlan(ReplicaPlan.ForRead<?> replicaPlan)
            {
                replicaPlans.incrementAndGet();
            }

            @Override
            public void onPartition(DecoratedKey partitionKey)
            {
                readPartitions.incrementAndGet();
            }

            @Override
            public void onRow(Row row)
            {
                readRows.incrementAndGet();
            }
        }

        private class TestLWTWriteTracker implements LWTWriteTracker
        {
            @Override
            public void onDone()
            {
                lwts.incrementAndGet();
            }

            @Override
            public void onError(Throwable exception)
            {
                errorLwts.incrementAndGet();
            }

            @Override
            public void onNotApplied()
            {
                nonAppliedLwts.incrementAndGet();
            }

            @Override
            public void onApplied(PartitionUpdate update)
            {
                appliedLwts.incrementAndGet();
                writtenRows.addAndGet(update.rowCount());
            }

            @Override
            public void onReplicaPlan(ReplicaPlan.ForRead<?> replicaPlan)
            {
                replicaPlans.incrementAndGet();
            }

            @Override
            public void onPartition(DecoratedKey partitionKey)
            {
                readPartitions.incrementAndGet();
            }

            @Override
            public void onRow(Row row)
            {
                readRows.incrementAndGet();
            }
        }
    }
}
