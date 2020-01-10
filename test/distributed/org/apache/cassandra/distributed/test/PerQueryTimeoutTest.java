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

package org.apache.cassandra.distributed.test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryOptionsFactory;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.net.Verb.COUNTER_MUTATION_REQ;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.RANGE_REQ;
import static org.apache.cassandra.net.Verb.READ_REQ;

public class PerQueryTimeoutTest extends DistributedTestBase
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testReadTimeout() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getReadRpcTimeout(MILLISECONDS);
        int custom = 50;
        test(custom,
             configured,
             cluster -> {
                 cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)", ConsistencyLevel.ALL);
                 cluster.verbs(READ_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out");
    }

    @Test
    public void testReadTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getReadRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> {
                 cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)", ConsistencyLevel.ALL);
                 cluster.verbs(READ_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out");
    }

    @Test
    public void testRangeReadTimeout() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getRangeRpcTimeout(MILLISECONDS);
        int custom = 50;
        test(custom,
             configured,
             cluster -> {
                 cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)", ConsistencyLevel.ALL);
                 cluster.verbs(RANGE_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "SELECT * FROM " + KEYSPACE + ".tbl WHERE ck > 1 AND ck < 100 ALLOW FILTERING",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out");
    }

    @Test
    public void testRangeReadTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getRangeRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> {
                 cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)", ConsistencyLevel.ALL);
                 cluster.verbs(RANGE_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "SELECT * FROM " + KEYSPACE + ".tbl WHERE ck > 1 AND ck < 100 ALLOW FILTERING",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out");
    }

    @Test
    public void testMutationTimeout() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = 50;
        test(custom,
             configured,
             cluster -> cluster.verbs(MUTATION_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testMutationTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> cluster.verbs(MUTATION_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testCounterWriteTimeout() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getCounterWriteRpcTimeout(MILLISECONDS);
        int custom = 50;
        test(custom,
             configured,
             cluster -> {
                 cluster.forEach(ins -> ins.runOnInstance(() -> StorageService.instance.setRpcReady(true))); // can be removed once #CASSANDRA-15460 merged
                 cluster.verbs(COUNTER_MUTATION_REQ).from(1).drop();
                 cluster.verbs(MUTATION_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, c counter) WITH read_repair='none'",
             "UPDATE " + KEYSPACE + ".tbl SET c = c + 1 WHERE pk = 1",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testCounterWriteTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getCounterWriteRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> {
                 cluster.forEach(ins -> ins.runOnInstance(() -> StorageService.instance.setRpcReady(true)));
                 cluster.verbs(COUNTER_MUTATION_REQ).from(1).drop();
                 cluster.verbs(MUTATION_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, c counter) WITH read_repair='none'",
             "UPDATE " + KEYSPACE + ".tbl SET c = c + 1 WHERE pk = 1",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    private final static String singlePartitionBatchQuery =
    "BEGIN BATCH\n" +
    "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1);\n" +
    "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2);\n" +
    "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3);\n" +
    "APPLY BATCH ;";
    @Test
    public void testSingleParitionBatchTimeout() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = 100;
        test(custom,
             configured,
             cluster -> cluster.verbs(MUTATION_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             singlePartitionBatchQuery,
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testSingleParitionBatchTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> cluster.verbs(MUTATION_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             singlePartitionBatchQuery,
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    private final static String multiplePartitionBatchQuery =
    "BEGIN BATCH\n" +
    "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1);\n" +
    "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (2, 2, 2);\n" +
    "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (3, 3, 3);\n" +
    "APPLY BATCH ;";
    @Test
    public void testMultiplePatitionBatchTimeout() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = 100;
        test(custom,
             configured,
             cluster -> cluster.verbs(MUTATION_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             multiplePartitionBatchQuery,
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testMultipleParitionBatchTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> cluster.verbs(MUTATION_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             multiplePartitionBatchQuery,
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    private final static String counterBatchQuery =
    "BEGIN COUNTER BATCH\n" +
    "UPDATE " + KEYSPACE + ".tbl SET c = c + 1 WHERE pk = 1;\n" +
    "UPDATE " + KEYSPACE + ".tbl SET c = c + 5 WHERE pk = 1;\n" +
    "UPDATE " + KEYSPACE + ".tbl SET c = c - 6 WHERE pk = 1;\n" +
    "APPLY BATCH ;";
    @Test
    public void testCounterBatchTimeout() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getCounterWriteRpcTimeout(MILLISECONDS);
        int custom = 50;
        test(custom,
             configured,
             cluster -> {
                 cluster.forEach(ins -> ins.runOnInstance(() -> StorageService.instance.setRpcReady(true))); // can be removed once #CASSANDRA-15460 merged
                 cluster.verbs(COUNTER_MUTATION_REQ).from(1).drop();
                 cluster.verbs(MUTATION_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, c counter) WITH read_repair='none'",
             counterBatchQuery,
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testCounterBatchTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getCounterWriteRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> {
                 cluster.forEach(ins -> ins.runOnInstance(() -> StorageService.instance.setRpcReady(true))); // can be removed once #CASSANDRA-15460 merged
                 cluster.verbs(COUNTER_MUTATION_REQ).from(1).drop();
                 cluster.verbs(MUTATION_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, c counter) WITH read_repair='none'",
             counterBatchQuery,
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    private final static String casBatchQuery =
    "BEGIN BATCH\n" +
    "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS;\n" +
    "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2);\n" +
    "APPLY BATCH ;";
    @Test
    public void testCasBatchTimeout() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = 50;
        test(custom,
             configured,
             cluster -> {
                 cluster.forEach(ins -> ins.runOnInstance(() -> StorageService.instance.setRpcReady(true))); // can be removed once #CASSANDRA-15460 merged
                 cluster.verbs(PAXOS_PREPARE_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             casBatchQuery,
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testCasBatchTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> {
                 cluster.forEach(ins -> ins.runOnInstance(() -> StorageService.instance.setRpcReady(true))); // can be removed once #CASSANDRA-15460 merged
                 cluster.verbs(PAXOS_PREPARE_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             casBatchQuery,
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testCasWriteTimeoutDuringPrepare() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = 100;
        test(custom,
             configured,
             cluster -> cluster.verbs(Verb.PAXOS_PREPARE_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testCasWriteTimoutDuringFetch() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = 100;
        test(custom,
             configured,
             cluster -> cluster.verbs(READ_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out"); // note: the exception is read timeout!
    }

    @Test
    public void testCasWriteTimeoutDuringPropose() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = 100;
        test(custom,
             configured,
             cluster -> cluster.verbs(Verb.PAXOS_PROPOSE_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testCasWriteTimeoutDuringCommit() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = 100;
        test(custom,
             configured,
             cluster -> cluster.verbs(Verb.PAXOS_COMMIT_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testCasWriteTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> cluster.verbs(Verb.PAXOS_COMMIT_REQ).from(1).drop(),
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS",
             ConsistencyLevel.ALL,
             "org.apache.cassandra.exceptions.WriteTimeoutException: Operation timed out");
    }

    @Test
    public void testPaxosReadTimeout() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getReadRpcTimeout(MILLISECONDS);
        int custom = 100;
        test(custom,
             configured,
             cluster -> {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.ALL);
                cluster.verbs(Verb.PAXOS_PREPARE_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
             ConsistencyLevel.SERIAL,
             "org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out");
    }

    @Test
    public void testPaxosReadTimeoutCannotExceedConfigured() throws Throwable
    {
        int configured = (int) DatabaseDescriptor.getReadRpcTimeout(MILLISECONDS);
        int custom = configured + 10_000;
        test(custom,
             configured,
             cluster -> {
                 cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.ALL);
                 cluster.verbs(Verb.PAXOS_PREPARE_REQ).from(1).drop();
             },
             "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'",
             "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
             ConsistencyLevel.SERIAL,
             "org.apache.cassandra.exceptions.ReadTimeoutException: Operation timed out");
    }

    private void test(long customTimeout,
                      long configuredTimeout,
                      Consumer<Cluster> beforeTestQuery,
                      String table,
                      String query,
                      Enum<?> consistencyLevelOrigin,
                      String errorMessage)
    throws Throwable
    {
        long timeLowerBound = Math.min(customTimeout, configuredTimeout);
        long timeUpperBound = Math.max(customTimeout, configuredTimeout);
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange(table);
            beforeTestQuery.accept(cluster);
            assertTimedThrowableRun(errorMessage,
                                    timeLowerBound,
                                    timeUpperBound,
                                    () -> cluster.get(1)
                                                 .callOnInstance(executeInternalWithCustomTimeout(query,
                                                                                                  consistencyLevelOrigin,
                                                                                                  (int) customTimeout)));
        }
    }

    private interface ThrowableRun
    {
        void run() throws Exception;
    }

    private void assertTimedThrowableRun(String message, long timeLowerBound, long timeUpperBound, ThrowableRun run)
    {
        final long start = System.currentTimeMillis();
        boolean hasExpectedExpection = false;
        try
        {
            run.run();
        }
        catch (Exception e)
        {
            System.out.println("!!! " + e.getMessage());
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(message));
            hasExpectedExpection = true;
        }
        if (hasExpectedExpection)
        {
            final long gap = System.currentTimeMillis() - start;
            Assert.assertTrue(String.format("Time taken - Actual: %d; expected: (%d, %d)", gap, timeLowerBound, timeUpperBound), gap < timeUpperBound && gap > timeLowerBound);
        }
        else
        {
            Assert.fail("Expected to see timout exception but not.");
        }

    }

    private IIsolatedExecutor.SerializableCallable<Object[][]> executeInternalWithCustomTimeout(String query,
                                                                                                Enum<?> consistencyLevelOrigin,
                                                                                                int timeoutInMillis,
                                                                                                Object... boundValues)
    {
        return () -> {
            ClientState clientState = ClientState.forExternalCalls(new InetSocketAddress(FBUtilities.getJustLocalAddress(), 9042));
            QueryState state = new QueryState(clientState);
            List<ByteBuffer> boundBBValues = new ArrayList<>();
            ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelOrigin.name());
            for (Object boundValue : boundValues)
                boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));
            QueryOptions options = QueryOptionsFactory.create(consistencyLevel,
                                                              boundBBValues,
                                                              false,
                                                              Integer.MAX_VALUE,
                                                              null,
                                                              null,
                                                              ProtocolVersion.V5,
                                                              null,
                                                              Long.MIN_VALUE,
                                                              Integer.MIN_VALUE,
                                                              timeoutInMillis);
            ResultMessage res = QueryProcessor.instance.process(query, state, options);

            if (res != null && res.kind == ResultMessage.Kind.ROWS)
                return RowUtil.toObjects((ResultMessage.Rows) res);
            else
                return new Object[][]{};
        };
    }
}
