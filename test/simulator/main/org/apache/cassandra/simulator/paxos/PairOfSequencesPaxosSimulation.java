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
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.ActionSchedulers;
import org.apache.cassandra.simulator.ActionSequence;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.cluster.KeyspaceActions;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ANY;
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
    private static final String DELETE = "DELETE FROM " + KEYSPACE + ".tbl WHERE pk = ? IF EXISTS";
    private static final String DELETE1 = "DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 4611686018427387904 WHERE pk = ?";
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

    class ModifyingOperation extends Operation
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
    final ConsistencyLevel serialConsistency;
    final Debug debug;
    final List<HistoryChecker> historyCheckers = new ArrayList<>();
    final AtomicInteger successfulReads = new AtomicInteger();
    final AtomicInteger successfulWrites = new AtomicInteger();
    final AtomicInteger failedReads = new AtomicInteger();
    final AtomicInteger failedWrites = new AtomicInteger();
    final long seed;
    final int[] primaryKeys;
    final int actionsPerKey;

    public PairOfSequencesPaxosSimulation(SimulatedSystems simulated,
                                          Cluster cluster,
                                          ClusterActions.Options clusterOptions,
                                          float readRatio,
                                          ConsistencyLevel serialConsistency,
                                          ActionSchedulers schedulers,
                                          Debug debug,
                                          long seed, int[] primaryKeys, int actionsPerKey)
    {
        super(simulated, cluster, schedulers);
        this.readRatio = readRatio;
        this.serialConsistency = serialConsistency;
        this.clusterOptions = clusterOptions;
        this.debug = debug;
        this.seed = seed;
        this.primaryKeys = primaryKeys;
        this.actionsPerKey = actionsPerKey;
    }

    public ActionPlan plan()
    {
        ActionPlan plan = new KeyspaceActions(simulated, KEYSPACE, TABLE, CREATE_TABLE, cluster,
                                              clusterOptions, serialConsistency, this, primaryKeys, debug).plan();

        plan = plan.encapsulate(new ActionPlan(
            ActionList.of(
                cluster.stream().map(i -> simulated.run("Insert Partitions", i, executeForPrimaryKeys(INSERT1, primaryKeys)))
            ),
            emptyList(),
            ActionList.of(
                cluster.stream().map(i -> simulated.run("Delete Partitions", i, executeForPrimaryKeys(DELETE1, primaryKeys)))
            )
        ));

        List<ActionSequence> interleave = new ArrayList<>();
        for (int primaryKey : primaryKeys)
        {
            List<ActionSequence> tmp = new ArrayList<>();
            HistoryChecker historyChecker = new HistoryChecker(primaryKey, actionsPerKey);
            historyCheckers.add(historyChecker);
            for (int nodei = 0, nodes = cluster.size() ; nodei < nodes ; ++nodei)
            {
                int node = 1 + Math.abs((nodei + primaryKey) % nodes);
                IInvokableInstance instance = cluster.get(node);
                List<Action> nodeActions = new ArrayList<>();
                for (int i = node - 1; i < actionsPerKey; i += nodes)
                {
                    switch (serialConsistency)
                    {
                        case LOCAL_SERIAL:
                            if (simulated.snitch.dcOf(node) > 0)
                            {
                                // perform some queries against these nodes but don't expect them to be linearizable
                                nodeActions.add(new NonVerifyingOperation(i, instance, serialConsistency, primaryKey, historyChecker));
                                break;
                            }
                        case SERIAL:
                            nodeActions.add(simulated.random.decide(readRatio)
                                            ? new VerifyingOperation(i, instance, serialConsistency, primaryKey, historyChecker)
                                            : new ModifyingOperation(i, instance, ANY, serialConsistency, primaryKey, historyChecker));
                    }
                }

                tmp.add(ActionSequence.strictSequential(nodeActions));
            }

            interleave.addAll(tmp);
            debug.debug(PARTITION, tmp, cluster, KEYSPACE, primaryKey);
        }

        return simulated.execution.plan()
                                  .encapsulate(plan)
                                  .encapsulate(new ActionPlan(ActionList.empty(), interleave, ActionList.empty()));
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
