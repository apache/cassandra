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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.AccordReplicaMetrics;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

/**
 * Replica execution of (single key) plain mutations, which read nothing, so that every replica may execute the
 * transaction itself knowing only its executeAt. With accord.replica_execution: BLIND_WRITE read-modify-writes are not
 * executed by replicas (see AccordReplicaExecutionAllTest), but must remain correct when replica execution is enabled.
 *
 * @author LLM
 */
public class AccordReplicaExecutionTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordReplicaExecutionTest.class);
    private static final String RF1_KEYSPACE = "rf1_ks";
    private static final String RF3_KEYSPACE = "rf3_ks";
    private static final int TXNS = 60;
    private static final int CONCURRENCY = 12;

    private static boolean replicaExecuteReads;

    @BeforeClass
    public static void setUp() throws IOException
    {
        setUp(false, true);
    }

    /**
     * @param replicaExecuteReads true for accord.replica_execution: ALL, else BLIND_WRITE
     */
    protected static void setUp(boolean replicaExecuteReads, boolean serializeWriteKeys) throws IOException
    {
        AccordReplicaExecutionTest.replicaExecuteReads = replicaExecuteReads;
        AccordTestBase.setupCluster(builder -> builder.withConfig(config -> config.set("accord.replica_execution", replicaExecuteReads ? "ALL" : "BLIND_WRITE")
                                                                                     .set("accord.send_stable", "TO_ALL_REPLICA_EXECUTABLE_ELSE_FOR_READS")
                                                                                     .set("accord.alpha_compatibility_serialize_write_keys", Boolean.toString(serializeWriteKeys))), 3);
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + RF1_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + RF3_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    /**
     * The number of write transactions executed by replicas without the coordinator, across the cluster
     */
    private static long localExecutions()
    {
        long sum = 0;
        for (IInvokableInstance instance : SHARED_CLUSTER)
            sum += instance.callOnInstance(() -> AccordReplicaMetrics.writeMetrics.localExecutions.getCount());
        return sum;
    }

    /**
     * @return the number of failures, if tolerated
     */
    private static int executeConcurrently(String description, IntFunction<String> query, boolean tolerateFailures)
    {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
        List<Throwable> failures = new ArrayList<>();
        try
        {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0 ; i < TXNS ; ++i)
            {
                int txn = i;
                futures.add(executor.submit(() -> SHARED_CLUSTER.coordinator(1 + txn % SHARED_CLUSTER.size()).execute(query.apply(txn), ConsistencyLevel.QUORUM)));
            }
            for (Future<?> future : futures)
            {
                try { future.get(2, TimeUnit.MINUTES); }
                catch (ExecutionException | InterruptedException | TimeoutException e) { failures.add(e instanceof ExecutionException ? e.getCause() : e); }
            }
        }
        finally
        {
            executor.shutdownNow();
        }
        if (!failures.isEmpty())
        {
            AssertionError error = new AssertionError(failures.size() + " of " + TXNS + ' ' + description + " failed");
            failures.forEach(error::addSuppressed);
            if (!tolerateFailures)
                throw error;
            logger.warn("Tolerating failures", error);
        }
        return failures.size();
    }

    private static void awaitReplicas(String keyspace, String table, int key, IntPredicate isExpected, String expected) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true)
        {
            StringBuilder mismatches = new StringBuilder();
            for (IInvokableInstance instance : SHARED_CLUSTER)
            {
                if (!isReplica(instance, keyspace, table, key))
                    continue;
                Object[][] rows = instance.executeInternal(format("SELECT v FROM %s.%s WHERE k = ?", keyspace, table), key);
                if (rows.length != 1 || !isExpected.test((Integer) rows[0][0]))
                    mismatches.append("node").append(instance.config().num()).append('=').append(rows.length == 0 ? null : rows[0][0]).append(' ');
            }
            if (mismatches.length() == 0)
                return;
            if (System.nanoTime() > deadline)
                throw new AssertionError("Replicas of key " + key + " do not have " + expected + ": " + mismatches);
            Thread.sleep(100);
        }
    }

    private static String createTable(String keyspace)
    {
        String table = "tbl" + COUNTER.getAndIncrement();
        SHARED_CLUSTER.schemaChange(format("CREATE TABLE %s.%s (k int PRIMARY KEY, v int) WITH transactional_mode='full'", keyspace, table));
        ensureTableIsAccordManaged(SHARED_CLUSTER, keyspace, table);
        return table;
    }

    /**
     * Plain mutations (that are not transactions) may be executed by every replica, while the coordinator also
     * reads (nothing) from one replica, to which the replica may reply after it has executed.
     */
    private void blindWrites(String keyspace) throws Exception
    {
        String table = createTable(keyspace);
        long localExecutions = localExecutions();
        executeConcurrently("blind writes", txn -> format("INSERT INTO %s.%s (k, v) VALUES (%d, %d)", keyspace, table, txn, txn), false);
        localExecutions = localExecutions() - localExecutions;
        logger.info("{} local executions for {} blind writes", localExecutions, TXNS);

        // every replica must apply every write
        for (int key = 0 ; key < TXNS ; ++key)
        {
            int expect = key;
            awaitReplicas(keyspace, table, key, v -> v == expect, Integer.toString(expect));
        }
        assertTrue("Expected some blind writes to be executed by replicas", localExecutions > 0);
    }

    /**
     * A read-modify-write reads, so is executed by replicas only with accord.replica_execution: ALL, and every replica
     * must apply every increment exactly once (in particular, a replica's read must not witness the transaction's own
     * writes, applied concurrently by an Apply). With ALL, a coordinator's read that arrives after the replica has
     * executed is answered Redundant, so (with RF=1) the coordinator may report a failure to the client, though the
     * transaction is still applied; we tolerate these failures.
     */
    private void readModifyWrite(String keyspace) throws Exception
    {
        String table = createTable(keyspace);
        SHARED_CLUSTER.coordinator(1).execute(format("INSERT INTO %s.%s (k, v) VALUES (0, 0)", keyspace, table), ConsistencyLevel.QUORUM);

        long localExecutions = localExecutions();
        String increment = format("BEGIN TRANSACTION\n  UPDATE %s.%s SET v += 1 WHERE k = 0;\nCOMMIT TRANSACTION", keyspace, table);
        int failures = executeConcurrently("read-modify-writes", txn -> increment, replicaExecuteReads);
        localExecutions = localExecutions() - localExecutions;
        logger.info("{} local executions and {} failures for {} read-modify-writes", localExecutions, failures, TXNS);
        // with BLIND_WRITE we do not assert there are none, as recovery may execute a backlog of transactions locally (see accord.coordinator_backlog_execution)
        if (replicaExecuteReads)
            assertTrue("Expected some read-modify-writes to be executed by replicas", localExecutions > 0);
        // every transaction must be applied exactly once, even if the coordinator reported a failure to the client
        awaitReplicas(keyspace, table, 0, v -> v == TXNS, TXNS + " increments");
    }

    private static boolean isReplica(IInvokableInstance instance, String keyspace, String table, int k)
    {
        return instance.callOnInstance(() -> {
            org.apache.cassandra.schema.TableMetadata metadata = org.apache.cassandra.schema.Schema.instance.getTableMetadata(keyspace, table);
            org.apache.cassandra.db.DecoratedKey key = metadata.partitioner.decorateKey(org.apache.cassandra.db.marshal.Int32Type.instance.decompose(k));
            return org.apache.cassandra.tcm.ClusterMetadata.current().placement(org.apache.cassandra.schema.Schema.instance.getKeyspaceMetadata(keyspace).params.replication)
                                                           .writes.forToken(key.getToken()).get().endpoints()
                                                           .contains(org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort());
        });
    }

    @Test
    public void blindWritesRF1() throws Exception
    {
        blindWrites(RF1_KEYSPACE);
    }

    @Test
    public void blindWritesRF3() throws Exception
    {
        blindWrites(RF3_KEYSPACE);
    }

    @Test
    public void readModifyWriteRF1() throws Exception
    {
        readModifyWrite(RF1_KEYSPACE);
    }

    @Test
    public void readModifyWriteRF3() throws Exception
    {
        readModifyWrite(RF3_KEYSPACE);
    }
}
