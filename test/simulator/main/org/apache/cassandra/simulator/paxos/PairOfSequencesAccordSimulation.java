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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.Query;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.utils.IntRange;

import static org.apache.cassandra.simulator.paxos.HistoryChecker.fail;
import static org.apache.cassandra.utils.AssertionUtils.hasCause;
import static org.apache.cassandra.utils.AssertionUtils.isThrowableInstanceof;

// TODO: the class hierarchy is a bit broken, but hard to untangle. Need to go Paxos->Consensus, probably.
@SuppressWarnings("unused")
public class PairOfSequencesAccordSimulation extends AbstractPairOfSequencesPaxosSimulation
{
    private static final Logger logger = LoggerFactory.getLogger(PairOfSequencesAccordSimulation.class);
    private static final String SELECT = "SELECT pk, count, seq FROM  " + KEYSPACE + ".tbl WHERE pk IN (%s);";
    private static final String UPDATE = "UPDATE " + KEYSPACE + ".tbl SET count += 1, seq = seq + ? WHERE pk = ?;";

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
                            for (Cell<?> cell : data)
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

    @Override
    void log(@Nullable Integer pk)
    {
        validator.print(pk);
    }

    private final float writeRatio;
    private final HistoryValidator validator;

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
        this.writeRatio = 1F - readRatio;
        validator = new LoggingHistoryValidator(new StrictSerializabilityValidator(primaryKeys));
    }

    @Override
    protected String createTableStmt()
    {
        return "CREATE TABLE " + KEYSPACE + ".tbl (pk int, count int, seq text, PRIMARY KEY (pk))";
    }

    @Override
    protected String preInsertStmt()
    {
        return "INSERT INTO " + KEYSPACE + ".tbl (pk, count, seq) VALUES (?, 0, '') USING TIMESTAMP 0";
    }

    @Override
    boolean allowMultiplePartitions() { return true; }

    @Override
    BiFunction<SimulatedSystems, int[], Supplier<Action>> actionFactory()
    {
        AtomicInteger id = new AtomicInteger(0);

        return (simulated, primaryKeyIndex) -> {
            int[] primaryKeys = IntStream.of(primaryKeyIndex).map(i -> this.primaryKeys[i]).toArray();
            return () -> accordAction(id.getAndIncrement(), simulated, primaryKeys);
        };
    }

    public class ReadWriteOperation extends Operation
    {
        private final IntHashSet reads, writes;

        public ReadWriteOperation(int id, int[] primaryKeys, IntHashSet reads, IntHashSet writes, IInvokableInstance instance)
        {
            super(primaryKeys, id, instance, "Accord", createQuery(id, reads, writes));
            this.reads = reads;
            this.writes = writes;
        }

        @Override
        void verify(Observation outcome)
        {
            SimpleQueryResult result = outcome.result;
            (result != null ? successfulWrites : failedWrites).incrementAndGet();
            if (result != null)
            {
                IntHashSet seen = new IntHashSet();
                //TODO if there isn't a value then we get empty read, which then doesn't make it into the QueryResult
                // given the fact that we always run with the partitions defined this should be fine
                try (HistoryValidator.Checker checker = validator.witness(outcome.start, outcome.end))
                {
                    while (result.hasNext())
                    {
                        org.apache.cassandra.distributed.api.Row row = result.next();

                        int pk = row.getInteger("pk");
                        int count = row.getInteger("count", 0);
                        int[] seq = Arrays.stream(row.getString("seq", "").split(","))
                                          .filter(s -> !s.isEmpty())
                                          .mapToInt(Integer::parseInt)
                                          .toArray();

                        if (!seen.add(pk))
                            throw new IllegalStateException("Duplicate partition key " + pk);
                        // every partition was read, but not all were written to... need to verify each partition
                        if (seq.length != count)
                            throw fail(pk, "%d != #%s", count, seq);

                        checker.read(pk, outcome.id, count, seq);
                    }
                    if (!seen.equals(reads))
                        throw fail(0, "#result had %s partitions, but should have had %s", seen, reads);
                    // handle writes
                    for (IntCursor c : writes)
                        checker.write(c.value, outcome.id, outcome.isSuccess());
                }
            }
        }
    }

    private Action accordAction(int id, SimulatedSystems simulated, int[] partitions)
    {
        IntArrayList reads = new IntArrayList();
        IntArrayList writes = new IntArrayList();
        for (int partition : partitions)
        {
            boolean added = false;
            if (simulated.random.decide(readRatio))
            {
                reads.add(partition);
                added = true;
            }
            if (simulated.random.decide(writeRatio))
            {
                writes.add(partition);
                added = true;
            }
            if (!added)
            {
                // when read ratio fails that implies write
                // when write ratio fails that implies read
                // so make that case a read/write
                // Its possible that both cases were true leading to a read/write; which is fine
                // this just makes sure every partition is consumed.
                reads.add(partition);
                writes.add(partition);
            }
        }

        int node = simulated.random.uniform(1, cluster.size() + 1);
        IInvokableInstance instance = cluster.get(node);
        return new ReadWriteOperation(id, partitions, new IntHashSet(reads), new IntHashSet(writes), instance);
    }

    private int[] genReadOnly(SimulatedSystems simulated, int[] partitions)
    {
        IntArrayList readOnly = new IntArrayList();
        for (int partition : partitions)
        {
            if (simulated.random.decide(readRatio))
                readOnly.add(partition);
        }
        return readOnly.toArray();
    }

    private static Query createQuery(int id, IntHashSet reads, IntHashSet writes)
    {
        if (reads.isEmpty() && writes.isEmpty())
            throw new IllegalArgumentException("Partitions are empty");
        List<Object> binds = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("BEGIN TRANSACTION\n");
        if (!reads.isEmpty())
        {

            sb.append("\t")
              .append(String.format(SELECT, String.join(", ", IntStream.of(reads.toArray())
                                                                       .mapToObj(i -> {
                                                                           binds.add(i);
                                                                           return "?";
                                                                       })
                                                                       .collect(Collectors.joining(", ")))))
              .append('\n');
        }

        for (IntCursor c : writes)
        {
            sb.append('\t').append(UPDATE).append("\n");
            binds.add(id + ",");
            binds.add(c.value);
        }

        sb.append("COMMIT TRANSACTION");
        return new Query(sb.toString(), 0, ConsistencyLevel.ANY, ConsistencyLevel.ANY, binds.toArray(new Object[0]));
    }

    @Override
    boolean joinAll()
    {
        return true;
    }
}
