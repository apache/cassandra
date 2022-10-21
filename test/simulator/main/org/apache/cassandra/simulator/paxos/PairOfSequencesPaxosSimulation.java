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
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.Query;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionListener;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.utils.IntRange;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ANY;
import static org.apache.cassandra.simulator.Debug.EventType.PARTITION;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.fail;

@SuppressWarnings("unused")
public class PairOfSequencesPaxosSimulation extends AbstractPairOfSequencesPaxosSimulation
{
    private static final Logger logger = LoggerFactory.getLogger(PairOfSequencesPaxosSimulation.class);
    private static final String UPDATE = "UPDATE " + KEYSPACE + ".tbl SET count = count + 1, seq1 = seq1 + ?, seq2 = seq2 + ? WHERE pk = ? IF EXISTS";
    private static final String SELECT = "SELECT pk, count, seq1, seq2 FROM  " + KEYSPACE + ".tbl WHERE pk = ?";

    class VerifyingOperation extends PaxosOperation
    {
        final HistoryChecker historyChecker;
        public VerifyingOperation(int id, IInvokableInstance instance, ConsistencyLevel consistencyLevel, int primaryKey, HistoryChecker historyChecker)
        {
            super(primaryKey, id, instance, "SELECT", SELECT, consistencyLevel, null, primaryKey);
            this.historyChecker = historyChecker;
        }

        void verify(Observation outcome)
        {
            SimpleQueryResult result = outcome.result;
            (result != null ? successfulReads : failedReads).incrementAndGet();

            if (result == null)
                return;

            if (!result.hasNext())
                throw fail(primaryKey, "#result: ([]) != 1");

            // pk, count, seq1, seq2
            Row row = result.next();

            // first verify internally consistent
            int count = row.getInteger("count", 0);
            int[] seq1 = Arrays.stream(row.getString("seq1", "").split(","))
                               .filter(s -> !s.isEmpty())
                               .mapToInt(Integer::parseInt)
                               .toArray();

            int[] seq2 = row.<Integer>getList("seq2", emptyList()).stream().mapToInt(x -> x).toArray();

            if (!Arrays.equals(seq1, seq2))
                throw fail(primaryKey, "%s != %s", seq1, seq2);

            if (seq1.length != count)
                throw fail(primaryKey, "%d != #%s", count, seq1);

            if (result.hasNext())
                throw fail(primaryKey, "#result (%s) != 1", ArrayUtils.toString(result.toObjectArrays()));

            historyChecker.witness(outcome, seq1);
        }
    }

    private abstract class PaxosOperation extends Operation
    {
        final int primaryKey;
        PaxosOperation(int primaryKey, int id, IInvokableInstance instance, String idString, String query, ConsistencyLevel commitConsistency, ConsistencyLevel serialConsistency, Object... params)
        {
            super(new int[] {primaryKey}, id, instance, idString, new Query(query, -1, commitConsistency, serialConsistency, params));
            this.primaryKey = primaryKey;
        }
    }

    class NonVerifyingOperation extends PaxosOperation
    {
        public NonVerifyingOperation(int id, IInvokableInstance instance, ConsistencyLevel consistencyLevel, int primaryKey, HistoryChecker historyChecker)
        {
            super(primaryKey, id, instance, "SELECT", SELECT, consistencyLevel, null, primaryKey);
        }

        void verify(Observation outcome)
        {
        }
    }

    public class ModifyingOperation extends PaxosOperation
    {
        final HistoryChecker historyChecker;
        public ModifyingOperation(int id, IInvokableInstance instance, ConsistencyLevel commitConsistency, ConsistencyLevel serialConsistency, int primaryKey, HistoryChecker historyChecker)
        {
            super(primaryKey, id, instance, "UPDATE", UPDATE, commitConsistency, serialConsistency, id + ",", ByteBufferUtil.getArray(LIST_TYPE.decompose(singletonList(id))), primaryKey);
            this.historyChecker = historyChecker;
        }

        void verify(Observation outcome)
        {
            SimpleQueryResult result = outcome.result;
            (result != null ? successfulWrites : failedWrites).incrementAndGet();
            if (result != null)
            {
                if (!result.hasNext())
                    throw fail(primaryKey, "Paxos Result: 1 != #[]");
                if (result.next().getBoolean(0) != TRUE)
                    throw fail(primaryKey, "Result != TRUE");
                if (result.hasNext())
                    throw fail(primaryKey, "Paxos Result: 1 != #%s", ArrayUtils.toString(result.toObjectArrays()));
            }
            historyChecker.applied(outcome.id, outcome.start, outcome.end, outcome.isSuccess());
        }
    }

    final List<HistoryChecker> historyCheckers = new ArrayList<>();

    public PairOfSequencesPaxosSimulation(SimulatedSystems simulated,
                                          Cluster cluster,
                                          ClusterActions.Options clusterOptions,
                                          float readRatio,
                                          int concurrency, IntRange simulateKeyForSeconds, IntRange withinKeyConcurrency,
                                          ConsistencyLevel serialConsistency, RunnableActionScheduler scheduler, Debug debug,
                                          long seed, int[] primaryKeys,
                                          long runForNanos, LongSupplier jitter)
    {
        super(simulated, cluster, clusterOptions,
              readRatio, concurrency, simulateKeyForSeconds, withinKeyConcurrency,
              serialConsistency,
              scheduler, debug,
              seed, primaryKeys,
              runForNanos, jitter);
    }

    @Override
    BiFunction<SimulatedSystems, int[], Supplier<Action>> actionFactory()
    {
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
        return (ignore, primaryKeyIndex) -> primaryKeyActions.get(only(primaryKeyIndex));
    }

    private static int only(int[] array)
    {
        if (array.length != 1)
            throw new AssertionError("Require only 1 element but found array " + Arrays.toString(array));
        return array[0];
    }

    @Override
    protected String createTableStmt()
    {
        return "CREATE TABLE " + KEYSPACE + ".tbl (pk int, count int, seq1 text, seq2 list<int>, PRIMARY KEY (pk))";
    }

    @Override
    protected String preInsertStmt()
    {
        return "INSERT INTO " + KEYSPACE + ".tbl (pk, count, seq1, seq2) VALUES (?, 0, '', []) USING TIMESTAMP 0";
    }

    private Operation verifying(int operationId, IInvokableInstance instance, int primaryKey, HistoryChecker historyChecker)
    {
        return new VerifyingOperation(operationId, instance, serialConsistency, primaryKey, historyChecker);
    }

    private Operation nonVerifying(int operationId, IInvokableInstance instance, int primaryKey, HistoryChecker historyChecker)
    {
        return new NonVerifyingOperation(operationId, instance, serialConsistency, primaryKey, historyChecker);
    }

    private Operation modifying(int operationId, IInvokableInstance instance, int primaryKey, HistoryChecker historyChecker)
    {
        return new ModifyingOperation(operationId, instance, ANY, serialConsistency, primaryKey, historyChecker);
    }

    @Override
    void log(@Nullable Integer primaryKey)
    {
        if (primaryKey == null) historyCheckers.forEach(HistoryChecker::print);
        else historyCheckers.stream().filter(h -> h.primaryKey == primaryKey).forEach(HistoryChecker::print);
    }

    @Override
    boolean joinAll()
    {
        return false;
    }
}
