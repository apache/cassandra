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

import java.util.Arrays;
import java.util.List;
import java.util.function.LongSupplier;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.utils.IntRange;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ANY;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.fail;

@SuppressWarnings("unused")
public class PairOfSequencesPaxosSimulation extends AbstractPairOfSequencesPaxosSimulation
{
    private static final Logger logger = LoggerFactory.getLogger(PairOfSequencesPaxosSimulation.class);
    private static final String UPDATE = "UPDATE " + KEYSPACE + ".tbl SET count = count + 1, seq1 = seq1 + ?, seq2 = seq2 + ? WHERE pk = ? IF EXISTS";
    private static final String SELECT = "SELECT pk, count, seq1, seq2 FROM  " + KEYSPACE + ".tbl WHERE pk = ?";

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
                    throw fail(primaryKey, "Paxos Result: 1 != #%s", ArrayUtils.toString(outcome.result));
                if (outcome.result[0][0] != TRUE)
                    throw fail(primaryKey, "Result != TRUE");
            }
            historyChecker.applied(outcome.id, outcome.start, outcome.end, outcome.result != null);
        }
    }

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
    protected String createTableStmt()
    {
        return "CREATE TABLE " + KEYSPACE + ".tbl (pk int, count int, seq1 text, seq2 list<int>, PRIMARY KEY (pk))";
    }

    @Override
    protected String preInsertStmt()
    {
        return "INSERT INTO " + KEYSPACE + ".tbl (pk, count, seq1, seq2) VALUES (?, 0, '', []) USING TIMESTAMP 0";
    }

    @Override
    Operation verifying(int operationId, IInvokableInstance instance, int primaryKey, HistoryChecker historyChecker)
    {
        return new VerifyingOperation(operationId, instance, serialConsistency, primaryKey, historyChecker);
    }

    @Override
    Operation nonVerifying(int operationId, IInvokableInstance instance, int primaryKey, HistoryChecker historyChecker)
    {
        return new NonVerifyingOperation(operationId, instance, serialConsistency, primaryKey, historyChecker);
    }

    @Override
    Operation modifying(int operationId, IInvokableInstance instance, int primaryKey, HistoryChecker historyChecker)
    {
        return new ModifyingOperation(operationId, instance, ANY, serialConsistency, primaryKey, historyChecker);
    }

    @Override
    boolean joinAll()
    {
        return false;
    }
}
