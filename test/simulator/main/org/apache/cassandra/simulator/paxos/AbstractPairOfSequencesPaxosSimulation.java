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
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.distributed.impl.FileLogAction;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.cluster.KeyspaceActions;
import org.apache.cassandra.simulator.logging.RunStartDefiner;
import org.apache.cassandra.simulator.logging.SeedDefiner;
import org.apache.cassandra.simulator.systems.SimulatedActionTask;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.utils.IntRange;
import org.apache.cassandra.utils.Pair;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.STREAM_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.TIME_AND_STREAM_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.TIME_LIMITED;

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

    abstract boolean joinAll();
    boolean allowMultiplePartitions() { return false; }

    abstract BiFunction<SimulatedSystems, int[], Supplier<Action>> actionFactory();

    protected Action checkErrorLogs(IInvokableInstance inst)
    {
        DatabaseDescriptor.clientInitialization();
        return new Action("Error logs for node" + inst.config().num(), Action.Modifiers.NONE)
        {
            @Override
            protected ActionList performSimple()
            {
                // can't use inst.logs as that runs in the class loader, which uses in-memory file system
                String suite = new RunStartDefiner().getPropertyValue() + "-" +  new SeedDefiner().getPropertyValue();
                String instanceId = "node" + inst.config().num();
                File logFile = new File(String.format("build/test/logs/simulator/%s/%s/system.log", suite, instanceId));
                FileLogAction logs = new FileLogAction(logFile);

                LogResult<List<String>> errors = logs.grepForErrors();
                if (!errors.getResult().isEmpty())
                {
                    List<Pair<String, String>> errorsSeen = new ArrayList<>();
                    for (String error : errors.getResult())
                    {
                        for (String line : error.split("\\n"))
                        {
                            line = line.trim();
                            if (line.startsWith("ERROR")) continue;
                            if (line.startsWith("at ")) continue;
                            errorsSeen.add(Pair.create(line.split(":")[0], error));
                            break;
                        }
                    }
                    Class<? extends Throwable>[] expected = expectedExceptions();
                    StringBuilder sb = new StringBuilder();
                    for (Pair<String, String> pair : errorsSeen)
                    {
                        String name = pair.left;
                        String exception = pair.right;
                        Class<?> klass;
                        try
                        {
                            klass = Class.forName(name);
                        }
                        catch (ClassNotFoundException e)
                        {
                            throw new RuntimeException(e);
                        }

                        if (!Stream.of(expected).anyMatch(e -> e.isAssignableFrom(klass)))
                            sb.append("Unexpected exception:\n").append(exception).append('\n');
                    }
                    if (sb.length() > 0)
                    {
                        AssertionError error = new AssertionError("Saw errors in node" + inst.config().num() + ": " + sb);
                        // this stacktrace isn't helpful, can be more confusing
                        error.setStackTrace(new StackTraceElement[0]);
                        throw error;
                    }
                }
                return ActionList.empty();
            }
        };
    }

    public ActionPlan plan()
    {
        ActionPlan plan = new KeyspaceActions(simulated, KEYSPACE, TABLE, createTableStmt(), cluster,
                                              clusterOptions, serialConsistency, this, primaryKeys, debug).plan(joinAll());

        plan = plan.encapsulate(ActionPlan.setUpTearDown(
            ActionList.of(
                cluster.stream().map(i -> simulated.run("Insert Partitions", i, executeForPrimaryKeys(preInsertStmt(), primaryKeys))),
                // TODO (now): this is temporary until we have correct epoch handling
                cluster.stream().map(i -> simulated.run("Create Accord Epoch", i, () -> AccordService.instance().createEpochFromConfigUnsafe()))
            ),
            ActionList.of(
                cluster.stream().map(i -> checkErrorLogs(i)),
                cluster.stream().map(i -> SimulatedActionTask.unsafeTask("Shutdown " + i.broadcastAddress(), RELIABLE, RELIABLE_NO_TIMEOUTS, simulated, i, i::shutdown))
            )
        ));

        BiFunction<SimulatedSystems, int[], Supplier<Action>> factory = actionFactory();

        List<Integer> available = IntStream.range(0, primaryKeys.length).boxed().collect(Collectors.toList());
        Action stream = Actions.infiniteStream(concurrency, new Supplier<Action>() {
            @Override
            public Action get()
            {
                int[] primaryKeyIndex = consume(simulated.random, available);
                long untilNanos = simulated.time.nanoTime() + SECONDS.toNanos(simulateKeyForSeconds.select(simulated.random));
                int concurrency = withinKeyConcurrency.select(simulated.random);
                Supplier<Action> supplier = factory.apply(simulated, primaryKeyIndex);
                // while this stream is finite, it participates in an infinite stream via its parent, so we want to permit termination while it's running
                return Actions.infiniteStream(concurrency, new Supplier<Action>()
                {
                    @Override
                    public Action get()
                    {
                        if (simulated.time.nanoTime() >= untilNanos)
                        {
                            IntStream.of(primaryKeyIndex).boxed().forEach(available::add);
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

    private int[] consume(RandomSource random, List<Integer> available)
    {
        if (available.isEmpty())
            throw new AssertionError("available partitions are empty!");
        int numPartitions = available.size() == 1 || !allowMultiplePartitions() ? 1 : random.uniform(1, available.size());
        int[] partitions = new int[numPartitions];
        for (int counter = 0; counter < numPartitions; counter++)
        {
            int idx = random.uniform(0, available.size());
            int next = available.get(idx);
            int last = available.get(available.size() - 1);
            if (available.set(idx, last) != next)
                throw new IllegalStateException("Expected to set " + last + " index " + idx + " but did not return " + next);
            int removed = available.remove(available.size() - 1);
            if (last != removed)
                throw new IllegalStateException("Expected to remove " + last + " but removed " + removed);

            partitions[counter] = next;
        }
        Arrays.sort(partitions);
        return partitions;
    }

    IIsolatedExecutor.SerializableRunnable executeForPrimaryKeys(String cql, int[] primaryKeys)
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
    public void run()
    {
        super.run();
        logger.warn("Writes: {} successful, {} failed", successfulWrites, failedWrites);
        logger.warn("Reads: {} successful {} failed", successfulReads, failedReads);
    }
}
