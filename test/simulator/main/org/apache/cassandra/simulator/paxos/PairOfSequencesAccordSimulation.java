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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.txn.Txn;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTxnBuilder;
import org.apache.cassandra.service.accord.db.AccordData;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.utils.IntRange;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ANY;
import static org.apache.cassandra.simulator.paxos.HistoryChecker.fail;

// TODO: the class hierarchy is a bit broken, but hard to untangle. Need to go Paxos->Consensus, probably.
@SuppressWarnings("unused")
public class PairOfSequencesAccordSimulation extends AbstractPairOfSequencesPaxosSimulation
{
    private static final Logger logger = LoggerFactory.getLogger(PairOfSequencesAccordSimulation.class);

    class VerifyingOperation extends Operation
    {
        final HistoryChecker historyChecker;
        public VerifyingOperation(int id, IInvokableInstance instance, ConsistencyLevel consistencyLevel, int primaryKey, HistoryChecker historyChecker)
        {
            super(primaryKey, id, instance, "SELECT", read(primaryKey));
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

    private static IIsolatedExecutor.SerializableCallable<Object[][]> read(int primaryKey)
    {
        return () -> {
            AccordTxnBuilder builder = new AccordTxnBuilder();
            builder.withRead(SELECT, primaryKey);
            // TODO (now): support complex columns
            return execute(builder.build(), "pk", "count", "seq1", "seq2");
        };
    }

    private static IIsolatedExecutor.SerializableCallable<Object[][]> write(int id, int primaryKey)
    {
        return () -> {
            AccordTxnBuilder builder = new AccordTxnBuilder();
            builder.withWrite(UPDATE, id + ",", singletonList(id), primaryKey);
            return execute(builder.build());
        };
    }

    private static Object[][] execute(Txn txn, String ... columns)
    {
        try
        {
            AccordData result = (AccordData) AccordService.instance.node.coordinate(txn).get();
            Assert.assertNotNull(result);
            QueryResults.Builder builder = QueryResults.builder();
            boolean addedHeader = false;
            for (FilteredPartition partition : result)
            {
                //TODO lot of this is copy/paste from SelectStatement...
                TableMetadata metadata = partition.metadata();
                if (!addedHeader)
                {
                    builder.columns(columns);
                    addedHeader = true;
                }
                ByteBuffer[] keyComponents = SelectStatement.getComponents(metadata, partition.partitionKey());
                for (Row row : partition)
                    append(metadata, keyComponents, row, builder, columns);
            }
            return builder.build().toObjectArrays();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            if (e.getCause() instanceof Preempted)
                return null;
            if (e.getCause() instanceof Timeout)
                return null;
            if (e.getCause() instanceof RequestTimeoutException)
                return null;
            throw new AssertionError(e);
        }
    }

    private static void append(TableMetadata metadata, ByteBuffer[] keyComponents, Row row, QueryResults.Builder builder, String[] columnNames)
    {
        Object[] buffer = new Object[columnNames.length];
        Clustering<?> clustering = row.clustering();
        int idx = 0;
        for (String columnName : columnNames)
        {
            ColumnMetadata column = metadata.getColumn(new ColumnIdentifier(columnName, true));
            switch (column.kind)
            {
                case PARTITION_KEY:
                    buffer[idx++] = column.type.compose(keyComponents[column.position()]);
                    break;
                case CLUSTERING:
                    buffer[idx++] = column.type.compose(clustering.bufferAt(column.position()));
                    break;
                case REGULAR:
                {
                    if (column.isComplex())
                    {
                        ComplexColumnData data = row.getComplexColumnData(column);
                        if (data == null)
                        {
                            buffer[idx++] = new ArrayList<>();
                        }
                        else
                        {
                            List<Object> result = new ArrayList<>(data.cellsCount());
                            for (Cell cell : data)
                                result.add(column.cellValueType().compose(cell.buffer()));
                            buffer[idx++] = result;
                        }
                    }
                    else
                    {
                        //TODO deletes
                        buffer[idx++] = column.type.compose(row.getCell(column).buffer());
                    }
                }
                break;
//                case STATIC:
                default:
                    throw new IllegalArgumentException("Unsupported kind: " + column.kind);
            }
        }
        builder.row(buffer);
    }

    class NonVerifyingOperation extends Operation
    {
        public NonVerifyingOperation(int id, IInvokableInstance instance, ConsistencyLevel consistencyLevel, int primaryKey, HistoryChecker historyChecker)
        {
            super(primaryKey, id, instance, "SELECT", read(primaryKey));
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
            super(primaryKey, id, instance, "UPDATE", write(id, primaryKey));
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

    public PairOfSequencesAccordSimulation(SimulatedSystems simulated,
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
        return true;
    }
}
