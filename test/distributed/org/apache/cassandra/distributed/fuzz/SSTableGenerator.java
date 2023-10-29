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

package org.apache.cassandra.distributed.fuzz;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;
import harry.operations.Relation;
import harry.operations.Query;
import harry.operations.QueryGenerator;
import harry.util.BitSet;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static harry.generators.DataGenerators.UNSET_VALUE;

public class SSTableGenerator
{
    protected final SchemaSpec schema;
    protected final OpSelectors.DescriptorSelector descriptorSelector;
    protected final OpSelectors.MonotonicClock clock;
    protected final ColumnFamilyStore store;
    protected final TableMetadata metadata;
    protected final QueryGenerator rangeSelector;

    private final Set<SSTableReader> sstables = new HashSet<>();

    private long lts = 0;

    public SSTableGenerator(Run run,
                            ColumnFamilyStore store)
    {
        this.schema = run.schemaSpec;
        this.descriptorSelector = run.descriptorSelector;
        this.clock = run.clock;
        this.store = store;
        // We assume metadata can not change over the lifetime of sstable generator
        this.metadata = store.metadata.get();
        this.rangeSelector = new QueryGenerator(schema, run.pdSelector, descriptorSelector, run.rng);
        store.disableAutoCompaction();
    }

    public Collection<SSTableReader> gen(int rows)
    {
        mark();
        for (int i = 0; i < rows; i++)
        {
            long current = lts++;
            write(current, current, current, current, true).applyUnsafe();
            if (schema.staticColumns != null)
                writeStatic(current, current, current, current, true).applyUnsafe();
        }

        return flush();
    }

    public void mark()
    {
        sstables.clear();
        sstables.addAll(store.getLiveSSTables());
    }

    public Collection<SSTableReader> flush()
    {
        Util.flush(store);
        sstables.removeAll(store.getLiveSSTables());

        Set<SSTableReader> ret = new HashSet<>(sstables);
        mark();
        return ret;
    }

    public Mutation write(long lts, long pd, long cd, long opId, boolean withRowMarker)
    {
        long[] vds = descriptorSelector.vds(pd, cd, lts, opId, OpSelectors.OperationKind.INSERT, schema);

        Object[] partitionKey = schema.inflatePartitionKey(pd);
        Object[] clusteringKey = schema.inflateClusteringKey(cd);
        Object[] regularColumns = schema.inflateRegularColumns(vds);

        RowUpdateBuilder builder = new RowUpdateBuilder(metadata,
                                                        FBUtilities.nowInSeconds(),
                                                        clock.rts(lts),
                                                        metadata.params.defaultTimeToLive,
                                                        serializePartitionKey(store, partitionKey))
                                   .clustering(clusteringKey);

        if (!withRowMarker)
            builder.noRowMarker();

        for (int i = 0; i < regularColumns.length; i++)
        {
            Object value = regularColumns[i];
            if (value == UNSET_VALUE)
                continue;

            ColumnMetadata def = metadata.getColumn(new ColumnIdentifier(schema.regularColumns.get(i).name, false));
            builder.add(def, value);
        }

        return builder.build();
    }

    public Mutation writeStatic(long lts, long pd, long cd, long opId, boolean withRowMarker)
    {
        long[] sds = descriptorSelector.sds(pd, cd, lts, opId, OpSelectors.OperationKind.INSERT_WITH_STATICS, schema);

        Object[] partitionKey = schema.inflatePartitionKey(pd);
        Object[] staticColumns = schema.inflateStaticColumns(sds);

        RowUpdateBuilder builder = new RowUpdateBuilder(metadata,
                                                        FBUtilities.nowInSeconds(),
                                                        clock.rts(lts),
                                                        metadata.params.defaultTimeToLive,
                                                        serializePartitionKey(store, partitionKey));

        if (!withRowMarker)
            builder.noRowMarker();

        for (int i = 0; i < staticColumns.length; i++)
        {
            Object value = staticColumns[i];
            if (value == UNSET_VALUE)
                continue;
            ColumnMetadata def = metadata.getColumn(new ColumnIdentifier(schema.staticColumns.get(i).name, false));
            builder.add(def, staticColumns[i]);
        }

        return builder.build();
    }

    public Mutation deleteColumn(long lts, long pd, long cd, long opId)
    {
        Object[] partitionKey = schema.inflatePartitionKey(pd);
        Object[] clusteringKey = schema.inflateClusteringKey(cd);

        RowUpdateBuilder builder = new RowUpdateBuilder(metadata,
                                                        FBUtilities.nowInSeconds(),
                                                        clock.rts(lts),
                                                        metadata.params.defaultTimeToLive,
                                                        serializePartitionKey(store, partitionKey))
                                   .noRowMarker()
                                   .clustering(clusteringKey);

        BitSet columns = descriptorSelector.columnMask(pd, lts, opId, OpSelectors.OperationKind.DELETE_COLUMN);
        BitSet mask = schema.regularColumnsMask();

        if (columns == null || columns.allUnset(mask))
            throw new IllegalArgumentException("Can't have a delete column query with no columns set. Column mask: " + columns);

        columns.eachSetBit((idx) -> {
            if (idx < schema.regularColumnsOffset)
                throw new RuntimeException("Can't delete parts of partition or clustering key");

            if (idx > schema.allColumns.size())
                throw new IndexOutOfBoundsException(String.format("Index %d is out of bounds. Max index: %d", idx, schema.allColumns.size()));

            builder.delete(schema.allColumns.get(idx).name);
        }, mask);

        return builder.build();
    }

    public Mutation deleteStatic(long lts, long pd, long opId)
    {
        Object[] partitionKey = schema.inflatePartitionKey(pd);

        RowUpdateBuilder builder = new RowUpdateBuilder(metadata,
                                                        FBUtilities.nowInSeconds(),
                                                        clock.rts(lts),
                                                        metadata.params.defaultTimeToLive,
                                                        serializePartitionKey(store, partitionKey))
                                   .noRowMarker();


        BitSet columns = descriptorSelector.columnMask(pd, lts, opId, OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS);
        BitSet mask = schema.staticColumnsMask();

        if (columns == null || columns.allUnset(mask))
            throw new IllegalArgumentException("Can't have a delete column query with no columns set. Column mask: " + columns);

        columns.eachSetBit((idx) -> {
            if (idx < schema.staticColumnsOffset)
                throw new RuntimeException(String.format("Can't delete parts of partition or clustering key %d (%s)",
                                                         idx, schema.allColumns.get(idx)));

            if (idx > schema.allColumns.size())
                throw new IndexOutOfBoundsException(String.format("Index %d is out of bounds. Max index: %d", idx, schema.allColumns.size()));

            builder.delete(schema.allColumns.get(idx).name);
        }, mask);

        return builder.build();
    }

    public Mutation deletePartition(long lts, long pd)
    {
        Object[] partitionKey = schema.inflatePartitionKey(pd);

        PartitionUpdate update = PartitionUpdate.fullPartitionDelete(metadata,
                                                                     serializePartitionKey(store, partitionKey),
                                                                     clock.rts(lts),
                                                                     FBUtilities.nowInSeconds());

        return new Mutation(update);
    }

    public Mutation deleteRow(long lts, long pd, long cd)
    {
        Object[] partitionKey = schema.inflatePartitionKey(pd);
        Object[] clusteringKey = schema.inflateClusteringKey(cd);

        return RowUpdateBuilder.deleteRow(metadata,
                                          clock.rts(lts),
                                          serializePartitionKey(store, partitionKey),
                                          clusteringKey);
    }

    public Mutation deleteSlice(long lts, long pd, long opId)
    {
        return delete(lts, pd, rangeSelector.inflate(lts, opId, Query.QueryKind.CLUSTERING_SLICE));
    }

    public Mutation deleteRange(long lts, long pd, long opId)
    {
        return delete(lts, pd, rangeSelector.inflate(lts, opId, Query.QueryKind.CLUSTERING_RANGE));
    }

    Mutation delete(long lts, long pd, Query query)
    {
        Object[] partitionKey = schema.inflatePartitionKey(pd);
        WhereClause.Builder builder = new WhereClause.Builder();
        List<ColumnIdentifier> variableNames = new ArrayList<>();

        List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < partitionKey.length; i++)
        {
            String name = schema.partitionKeys.get(i).name;
            ColumnMetadata columnDef = metadata.getColumn(ByteBufferUtil.bytes(name));
            variableNames.add(columnDef.name);
            values.add(ByteBufferUtil.objectToBytes(partitionKey[i]));
            builder.add(new SingleColumnRelation(ColumnIdentifier.getInterned(name, true),
                                                 toOperator(Relation.RelationKind.EQ),
                                                 new AbstractMarker.Raw(values.size() - 1)));
        }

        for (Relation relation : query.relations)
        {
            String name = relation.column();
            ColumnMetadata columnDef = metadata.getColumn(ByteBufferUtil.bytes(relation.column()));
            variableNames.add(columnDef.name);
            values.add(ByteBufferUtil.objectToBytes(relation.value()));
            builder.add(new SingleColumnRelation(ColumnIdentifier.getInterned(name, false),
                                                 toOperator(relation.kind),
                                                 new AbstractMarker.Raw(values.size() - 1)));
        }

        StatementRestrictions restrictions = new StatementRestrictions(null,
                                                                       StatementType.DELETE,
                                                                       metadata,
                                                                       builder.build(),
                                                                       new VariableSpecifications(variableNames),
                                                                       Collections.emptyList(),
                                                                       false,
                                                                       false,
                                                                       false,
                                                                       false);

        QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, values);
        SortedSet<ClusteringBound<?>> startBounds = restrictions.getClusteringColumnsBounds(Bound.START, options);
        SortedSet<ClusteringBound<?>> endBounds = restrictions.getClusteringColumnsBounds(Bound.END, options);

        Slices slices = DeleteStatement.toSlices(metadata, startBounds, endBounds);
        assert slices.size() == 1;
        long deletionTime = FBUtilities.nowInSeconds();
        long rts = clock.rts(lts);

        return new RowUpdateBuilder(metadata,
                                    deletionTime,
                                    rts,
                                    metadata.params.defaultTimeToLive,
                                    serializePartitionKey(store, partitionKey))
               .noRowMarker()
               .addRangeTombstone(new RangeTombstone(slices.get(0), DeletionTime.build(rts, deletionTime)))
               .build();
    }

    public static Operator toOperator(Relation.RelationKind kind)
    {
        switch (kind)
        {
            case LT: return Operator.LT;
            case GT: return Operator.GT;
            case LTE: return Operator.LTE;
            case GTE: return Operator.GTE;
            case EQ: return Operator.EQ;
            default: throw new IllegalArgumentException("Unsupported " + kind);
        }
    }

    public static DecoratedKey serializePartitionKey(ColumnFamilyStore store, Object... pk)
    {
        if (pk.length == 1)
            return store.getPartitioner().decorateKey(ByteBufferUtil.objectToBytes(pk[0]));

        ByteBuffer[] values = new ByteBuffer[pk.length];
        for (int i = 0; i < pk.length; i++)
            values[i] = ByteBufferUtil.objectToBytes(pk[i]);
        return store.getPartitioner().decorateKey(CompositeType.build(ByteBufferAccessor.instance, values));
    }
}
