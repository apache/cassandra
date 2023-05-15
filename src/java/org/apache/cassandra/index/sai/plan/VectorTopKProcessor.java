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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Triple;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Processor that scans all rows from reconciled query result and selects rows with top-k scores based on vector indexes.
 *
 * This processor performs the following steps:
 * - collect rows with score into PriorityQueue that sorts rows based on score. If there are multiple vector indexes,
 *   the final score is the sum of all vector index scores.
 * - remove rows with the lowest scores from PQ if PQ size exceeds limit
 * - return rows from PQ in primary key order to client
 *
 * Note that recall will be lower with paging, because:
 * - page size is used as limit
 * - for the first query, coordinator returns global top page-size rows within entire ring
 * - for the subsequent queries, coordinators returns global top page-size rows withom range from last-returned-row to max token
 */
public class VectorTopKProcessor
{
    private final ReadCommand command;
    private final StorageAttachedIndexQueryPlan queryPlan;

    private final int limit;

    public VectorTopKProcessor(ReadCommand command, StorageAttachedIndexQueryPlan queryPlan)
    {
        this.command = command;
        this.queryPlan = queryPlan;
        this.limit = command.limits().count();
    }

    public PartitionIterator filter(PartitionIterator partitions)
    {
        // priority queue ordered by score in ascending order
        PriorityQueue<Triple<Pair<DecoratedKey, Row>, Row, Float>> topK = new PriorityQueue<>(limit, Comparator.comparing(Triple::getRight));

        Map<StorageAttachedIndex, float[]> queryVectorPerIndex = getQueryVectorPerIndex(command);

        while (partitions.hasNext())
        {
            try (RowIterator partition = partitions.next())
            {
                DecoratedKey key = partition.partitionKey();
                Row staticRow = partition.staticRow();
                Pair<DecoratedKey, Row> keyAndStatic = Pair.create(key, staticRow);
                // compute key and static row score once per partition
                float keyAndStaticScore = getScoreForRow(queryVectorPerIndex, key, staticRow);

                while (partition.hasNext())
                {
                    Row row = partition.next();
                    float rowScore = getScoreForRow(queryVectorPerIndex, null, row);
                    topK.add(Triple.of(keyAndStatic, row, keyAndStaticScore + rowScore));

                    // when exceeding limit, remove row with low score
                    while (topK.size() > limit)
                        topK.poll();
                }
            }
        }
        partitions.close();

        // reorder rows in partition/clustering order
        TreeMap<Pair<DecoratedKey, Row>, TreeSet<Row>> rowsByPartition = new TreeMap<>(Comparator.comparing(p -> p.left));
        for (Triple<Pair<DecoratedKey, Row>, Row, Float> triple : topK)
            rowsByPartition.computeIfAbsent(triple.getLeft(), k -> new TreeSet<>(command.metadata().comparator))
                           .add(triple.getMiddle());

        return new InMemoryPartitionIterator(rowsByPartition);
    }

    private Map<StorageAttachedIndex, float[]> getQueryVectorPerIndex(ReadCommand command)
    {
        return command.rowFilter().getExpressions().stream()
                      .map(e -> {
                          StorageAttachedIndex sai = findVectorIndexFor(e);
                          if (sai == null)
                              return null;

                          float[] qv = (float[]) sai.getIndexContext().getValidator().getSerializer().deserialize(e.getIndexValue().duplicate());
                          return Pair.create(sai, qv);
                      }).filter(Objects::nonNull)
                      .collect(Collectors.toMap(p -> p.left, p -> p.right));
    }

    /**
     * Sum the scores from different vector indexes for the row
     */
    private float getScoreForRow(Map<StorageAttachedIndex, float[]> queryVectorPerIndex, DecoratedKey key, Row row)
    {
        float score = 0;
        for (Map.Entry<StorageAttachedIndex, float[]> e : queryVectorPerIndex.entrySet())
        {
            IndexContext indexContext = e.getKey().getIndexContext();
            ColumnMetadata column = indexContext.getDefinition();

            if (column.isPrimaryKeyColumn() && key == null)
                continue;

            if (column.isStatic() && !row.isStatic())
                continue;

            if ((column.isClusteringColumn() || column.isRegular()) && row.isStatic())
                continue;

            ByteBuffer value = indexContext.getValueOf(key, row, FBUtilities.nowInSeconds());
            if (value != null)
            {
                float[] vector = (float[]) indexContext.getValidator().getSerializer().deserialize(value.duplicate());
                score += indexContext.getIndexWriterConfig().getSimilarityFunction().compare(vector, e.getValue());
            }
        }
        return score;
    }

    @Nullable
    private StorageAttachedIndex findVectorIndexFor(RowFilter.Expression e)
    {
        if (e.operator() != Operator.ANN)
            return null;

        for (Index index : queryPlan.getIndexes())
        {
            if (!(index instanceof StorageAttachedIndex))
                continue;

            StorageAttachedIndex sai = (StorageAttachedIndex) index;
            if (e.column().equals(sai.getIndexContext().getDefinition()))
                return sai;
        }

        return null;
    }

    private class InMemoryPartitionIterator implements PartitionIterator
    {
        private final Iterator<Map.Entry<Pair<DecoratedKey, Row>, TreeSet<Row>>> partitions;

        public InMemoryPartitionIterator(TreeMap<Pair<DecoratedKey, Row>, TreeSet<Row>> rowsByPartitions)
        {
            this.partitions = rowsByPartitions.entrySet().iterator();
        }

        @Override
        public void close()
        {
        }

        @Override
        public boolean hasNext()
        {
            return partitions.hasNext();
        }

        @Override
        public RowIterator next()
        {
            return new InMemoryRowIterator(partitions.next());
        }
    }

    private class InMemoryRowIterator implements RowIterator
    {
        private final DecoratedKey decoratedKey;
        private final Row staticRow;
        private final Iterator<Row> rows;

        public InMemoryRowIterator(Map.Entry<Pair<DecoratedKey, Row>, TreeSet<Row>> rows)
        {
            this.rows = rows.getValue().iterator();
            this.decoratedKey = rows.getKey().left;
            this.staticRow = rows.getKey().right;
        }

        @Override
        public void close()
        {
        }

        @Override
        public boolean hasNext()
        {
            return rows.hasNext();
        }

        @Override
        public Row next()
        {
            return rows.next();
        }

        @Override
        public TableMetadata metadata()
        {
            return command.metadata();
        }

        @Override
        public boolean isReverseOrder()
        {
            return command.isReversed();
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return command.metadata().regularAndStaticColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return decoratedKey;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }
    }
}
