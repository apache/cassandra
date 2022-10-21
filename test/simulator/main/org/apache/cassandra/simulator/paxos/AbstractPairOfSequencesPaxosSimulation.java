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
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionListener;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.cluster.KeyspaceActions;
import org.apache.cassandra.simulator.systems.SimulatedActionTask;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.utils.IntRange;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.STREAM_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.TIME_AND_STREAM_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.TIME_LIMITED;
import static org.apache.cassandra.simulator.Debug.EventType.PARTITION;

@SuppressWarnings("unused")
abstract class AbstractPairOfSequencesPaxosSimulation extends PaxosSimulation
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPairOfSequencesPaxosSimulation.class);

    static final String KEYSPACE = "simple_paxos_simulation";
    static final String TABLE = "tbl";
    static final ListType<Integer> LIST_TYPE = ListType.getInstance(Int32Type.instance, true);

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

    public AbstractPairOfSequencesPaxosSimulation(SimulatedSystems simulated,
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

    protected abstract String createTableStmt();

    protected abstract String preInsertStmt();

    abstract Operation verifying(int operationId, IInvokableInstance instance, int primaryKey, HistoryChecker historyChecker);
    abstract Operation nonVerifying(int operationId, IInvokableInstance instance, int primaryKey, HistoryChecker historyChecker);
    abstract Operation modifying(int operationId, IInvokableInstance instance, int primaryKey, HistoryChecker historyChecker);
    abstract boolean joinAll();

    public ActionPlan plan()
    {
        ActionPlan plan = new KeyspaceActions(simulated, KEYSPACE, TABLE, createTableStmt(), cluster,
                                              clusterOptions, serialConsistency, this, primaryKeys, debug).plan(joinAll());

        plan = plan.encapsulate(ActionPlan.setUpTearDown(
            ActionList.of(
                cluster.stream().map(i -> simulated.run("Insert Partitions", i, executeForPrimaryKeys(preInsertStmt(), primaryKeys)))
            ).andThen(
                // TODO (now): this is temporary until we have correct epoch handling
                ActionList.of(
                    cluster.stream().map(i -> simulated.run("Create Accord Epoch", i, () -> AccordService.instance().createEpochFromConfigUnsafe()))
                )
//            ).andThen(
//                // TODO (now): this is temporary until we have parameterisation of simulation
//                ActionList.of(
//                    cluster.stream().map(i -> simulated.run("Disable Accord Cache", i, () -> AccordService.instance.setCacheSize(0)))
//                )
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
                                return nonVerifying(i++, instance, primaryKey, historyChecker);
                            }
                        case SERIAL:
                            return simulated.random.decide(readRatio)
                                   ? verifying(i++, instance, primaryKey, historyChecker)
                                   : modifying(i++, instance, primaryKey, historyChecker);
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
