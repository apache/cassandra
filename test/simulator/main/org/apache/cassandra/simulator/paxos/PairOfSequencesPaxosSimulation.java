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

package org.apache.cassandra.simulator.paxos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionListener;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.cluster.KeyspaceActions;
import org.apache.cassandra.simulator.systems.SimulatedActionTask;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.utils.IntRange;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ANY;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.STREAM_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.TIME_AND_STREAM_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.TIME_LIMITED;
import static org.apache.cassandra.simulator.Debug.EventType.PARTITION;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.fail;

@SuppressWarnings("unused")
public class PairOfSequencesPaxosSimulation extends PaxosSimulation
{
    private static final Logger logger = LoggerFactory.getLogger(PairOfSequencesPaxosSimulation.class);

    private static final String KEYSPACE = "simple_paxos_simulation";
    private static final String TABLE = "tbl";
    private static final String CREATE_TABLE = "CREATE TABLE " + KEYSPACE + ".tbl (pk int, count int, seq1 text, seq2 list<int>, PRIMARY KEY (pk))";
    private static final String INSERT = "INSERT INTO " + KEYSPACE + ".tbl (pk, count, seq1, seq2) VALUES (?, 0, '', []) IF NOT EXISTS";
    private static final String INSERT1 = "INSERT INTO " + KEYSPACE + ".tbl (pk, count, seq1, seq2) VALUES (?, 0, '', []) USING TIMESTAMP 0";
    private static final String UPDATE = "UPDATE " + KEYSPACE + ".tbl SET count = count + 1, seq1 = seq1 + ?, seq2 = seq2 + ? WHERE pk = ? IF EXISTS";
    private static final String SELECT = "SELECT pk, count, seq1, seq2 FROM  " + KEYSPACE + ".tbl WHERE pk = ?";
    private static final ListType<Integer> LIST_TYPE = ListType.getInstance(Int32Type.instance, true);

    class VerifyingOperation extends Operation
    {
        final HistoryChecker historyChecker;
        public VerifyingOperation(int id, IInvokableInstance instance, ConsistencyLevel consistencyLevel, int primaryKey, HistoryChecker historyChecker)
        {
            super(primaryKey, id, instance, "SELECT", SELECT, consistencyLevel, null, primaryKey);
            this.historyChecker = historyChecker;
        }

        void verify(Observation outcome)
        {
            (outcome.result != null ? successfulReads : failedReads).incrementAndGet();

            if (outcome.result == null)
                return;

            if (outcome.result.length != 1)
                throw fail(primaryKey, "#result (%s) != 1", Arrays.toString(outcome.result));

            Object[] row = outcome.result[0];
            // first verify internally consistent
            int count = row[1] == null ? 0 : (Integer) row[1];
            int[] seq1 = Arrays.stream((row[2] == null ? "" : (String) row[2]).split(","))
                               .filter(s -> !s.isEmpty())
                               .mapToInt(Integer::parseInt)
                               .toArray();
            int[] seq2 = ((List<Integer>) (row[3] == null ? emptyList() : row[3]))
                         .stream().mapToInt(x -> x).toArray();

            if (!Arrays.equals(seq1, seq2))
                throw fail(primaryKey, "%s != %s", seq1, seq2);

            if (seq1.length != count)
                throw fail(primaryKey, "%d != #%s", count, seq1);

            historyChecker.witness(outcome, seq1, outcome.start, outcome.end);
        }
    }

    class NonVerifyingOperation extends Operation
    {
        public NonVerifyingOperation(int id, IInvokableInstance instance, ConsistencyLevel consistencyLevel, int primaryKey, HistoryChecker historyChecker)
        {
            super(primaryKey, id, instance, "SELECT", SELECT, consistencyLevel, null, primaryKey);
        }

        void verify(Observation outcome)
        {
        }
    }

    public class ModifyingOperation extends Operation
    {
        final HistoryChecker historyChecker;
        public ModifyingOperation(int id, IInvokableInstance instance, ConsistencyLevel commitConsistency, ConsistencyLevel serialConsistency, int primaryKey, HistoryChecker historyChecker)
        {
            super(primaryKey, id, instance, "UPDATE", UPDATE, commitConsistency, serialConsistency, id + ",", ByteBufferUtil.getArray(LIST_TYPE.decompose(singletonList(id))), primaryKey);
            this.historyChecker = historyChecker;
        }

        void verify(Observation outcome)
        {
            (outcome.result != null ? successfulWrites : failedWrites).incrementAndGet();
            if (outcome.result != null)
            {
                if (outcome.result.length != 1)
                    throw fail(primaryKey, "Result: 1 != #%s", Arrays.toString(outcome.result));
                if (outcome.result[0][0] != TRUE)
                    throw fail(primaryKey, "Result != TRUE");
            }
            historyChecker.applied(outcome.id, outcome.start, outcome.end, outcome.result != null);
        }
    }

    final ClusterActions.Options clusterOptions;
    final float readRatio;
    final IntRange withinKeyConcurrency;
    final int concurrency;
    final IntRange simulateKeyForSeconds;
    final ConsistencyLevel serialConsistency;
    final Debug debug;
    final List<HistoryChecker> historyCheckers = new ArrayList<>();
    final AtomicInteger successfulReads = new AtomicInteger();
    final AtomicInteger successfulWrites = new AtomicInteger();
    final AtomicInteger failedReads = new AtomicInteger();
    final AtomicInteger failedWrites = new AtomicInteger();
    final long seed;
    final int[] primaryKeys;

    public PairOfSequencesPaxosSimulation(SimulatedSystems simulated,
                                          Cluster cluster,
                                          ClusterActions.Options clusterOptions,
                                          float readRatio,
                                          int concurrency, IntRange simulateKeyForSeconds, IntRange withinKeyConcurrency,
                                          ConsistencyLevel serialConsistency, RunnableActionScheduler scheduler, Debug debug,
                                          long seed, int[] primaryKeys,
                                          long runForNanos, LongSupplier jitter)
    {
        super(runForNanos < 0 ? STREAM_LIMITED : clusterOptions.topologyChangeLimit < 0 ? TIME_LIMITED : TIME_AND_STREAM_LIMITED,
              simulated, cluster, scheduler, runForNanos, jitter);
        this.readRatio = readRatio;
        this.concurrency = concurrency;
        this.simulateKeyForSeconds = simulateKeyForSeconds;
        this.withinKeyConcurrency = withinKeyConcurrency;
        this.serialConsistency = serialConsistency;
        this.clusterOptions = clusterOptions;
        this.debug = debug;
        this.seed = seed;
        this.primaryKeys = primaryKeys.clone();
        Arrays.sort(this.primaryKeys);
    }

    public ActionPlan plan()
    {
        ActionPlan plan = new KeyspaceActions(simulated, KEYSPACE, TABLE, CREATE_TABLE, cluster,
                                              clusterOptions, serialConsistency, this, primaryKeys, debug).plan();

        plan = plan.encapsulate(ActionPlan.setUpTearDown(
            ActionList.of(
                cluster.stream().map(i -> simulated.run("Insert Partitions", i, executeForPrimaryKeys(INSERT1, primaryKeys)))
            ),
            ActionList.of(
                cluster.stream().map(i -> SimulatedActionTask.unsafeTask("Shutdown " + i.broadcastAddress(), RELIABLE, RELIABLE_NO_TIMEOUTS, simulated, i, i::shutdown))
            )
        ));

        final int nodes = cluster.size();
        for (int primaryKey : primaryKeys)
            historyCheckers.add(new HistoryChecker(primaryKey));

        List<Supplier<Action>> primaryKeyActions = new ArrayList<>();
        for (int pki = 0 ; pki < primaryKeys.length ; ++pki)
        {
            int primaryKey = primaryKeys[pki];
            HistoryChecker historyChecker = historyCheckers.get(pki);
            Supplier<Action> supplier = new Supplier<Action>()
            {
                int i = 0;

                @Override
                public Action get()
                {
                    int node = simulated.random.uniform(1, nodes + 1);
                    IInvokableInstance instance = cluster.get(node);
                    switch (serialConsistency)
                    {
                        default: throw new AssertionError();
                        case LOCAL_SERIAL:
                            if (simulated.snitch.dcOf(node) > 0)
                            {
                                // perform some queries against these nodes but don't expect them to be linearizable
                                return new NonVerifyingOperation(i++, instance, serialConsistency, primaryKey, historyChecker);
                            }
                        case SERIAL:
                            return simulated.random.decide(readRatio)
                                   ? new VerifyingOperation(i++, instance, serialConsistency, primaryKey, historyChecker)
                                   : new ModifyingOperation(i++, instance, ANY, serialConsistency, primaryKey, historyChecker);
                    }
                }

                @Override
                public String toString()
                {
                    return Integer.toString(primaryKey);
                }
            };

            final ActionListener listener = debug.debug(PARTITION, simulated.time, cluster, KEYSPACE, primaryKey);
            if (listener != null)
            {
                Supplier<Action> wrap = supplier;
                supplier = new Supplier<Action>()
                {
                    @Override
                    public Action get()
                    {
                        Action action = wrap.get();
                        action.register(listener);
                        return action;
                    }

                    @Override
                    public String toString()
                    {
                        return wrap.toString();
                    }
                };
            }

            primaryKeyActions.add(supplier);
        }

        List<Integer> available = IntStream.range(0, primaryKeys.length).boxed().collect(Collectors.toList());
        Action stream = Actions.infiniteStream(concurrency, new Supplier<Action>() {
            @Override
            public Action get()
            {
                int i = simulated.random.uniform(0, available.size());
                int next = available.get(i);
                available.set(i, available.get(available.size() - 1));
                available.remove(available.size() - 1);
                long untilNanos = simulated.time.nanoTime() + SECONDS.toNanos(simulateKeyForSeconds.select(simulated.random));
                int concurrency = withinKeyConcurrency.select(simulated.random);
                Supplier<Action> supplier = primaryKeyActions.get(next);
                // while this stream is finite, it participates in an infinite stream via its parent, so we want to permit termination while it's running
                return Actions.infiniteStream(concurrency, new Supplier<Action>()
                {
                    @Override
                    public Action get()
                    {
                        if (simulated.time.nanoTime() >= untilNanos)
                        {
                            available.add(next);
                            return null;
                        }
                        return supplier.get();
                    }

                    @Override
                    public String toString()
                    {
                        return supplier.toString();
                    }
                });
            }

            @Override
            public String toString()
            {
                return "Primary Key Actions";
            }
        });

        return simulated.execution.plan()
                                  .encapsulate(plan)
                                  .encapsulate(ActionPlan.interleave(singletonList(ActionList.of(stream))));
    }

    private IIsolatedExecutor.SerializableRunnable executeForPrimaryKeys(String cql, int[] primaryKeys)
    {
        return () -> {
            for (int primaryKey : primaryKeys)
                Instance.unsafeExecuteInternalWithResult(cql, primaryKey);
        };
    }

    @Override
    public TopologyChangeValidator newTopologyChangeValidator(Object id)
    {
        return new PaxosTopologyChangeVerifier(cluster, KEYSPACE, TABLE, id);
    }

    @Override
    public RepairValidator newRepairValidator(Object id)
    {
        return new PaxosRepairValidator(cluster, KEYSPACE, TABLE, id);
    }

    @Override
    void log(@Nullable Integer primaryKey)
    {
        if (primaryKey == null) historyCheckers.forEach(HistoryChecker::print);
        else historyCheckers.stream().filter(h -> h.primaryKey == primaryKey).forEach(HistoryChecker::print);
    }

    @Override
    public void run()
    {
        super.run();
        logger.warn("Writes: {} successful, {} failed", successfulWrites, failedWrites);
        logger.warn("Reads: {} successful {} failed", successfulReads, failedReads);
    }
}
