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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public abstract class SimpleBuilders
{
    private SimpleBuilders()
    {
    }

    private static DecoratedKey makePartitonKey(CFMetaData metadata, Object... partitionKey)
    {
        if (partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey)
            return (DecoratedKey)partitionKey[0];

        ByteBuffer key = CFMetaData.serializePartitionKey(metadata.getKeyValidatorAsClusteringComparator().make(partitionKey));
        return metadata.decorateKey(key);
    }

    private static Clustering makeClustering(CFMetaData metadata, Object... clusteringColumns)
    {
        if (clusteringColumns.length == 1 && clusteringColumns[0] instanceof Clustering)
            return (Clustering)clusteringColumns[0];

        if (clusteringColumns.length == 0)
        {
            // If the table has clustering columns, passing no values is for updating the static values, so check we
            // do have some static columns defined.
            assert metadata.comparator.size() == 0 || !metadata.partitionColumns().statics.isEmpty();
            return metadata.comparator.size() == 0 ? Clustering.EMPTY : Clustering.STATIC_CLUSTERING;
        }
        else
        {
            return metadata.comparator.make(clusteringColumns);
        }
    }

    private static class AbstractBuilder<T>
    {
        protected long timestamp = FBUtilities.timestampMicros();
        protected int ttl = 0;
        protected int nowInSec = FBUtilities.nowInSeconds();

        protected void copyParams(AbstractBuilder<?> other)
        {
            other.timestamp = timestamp;
            other.ttl = ttl;
            other.nowInSec = nowInSec;
        }

        public T timestamp(long timestamp)
        {
            this.timestamp = timestamp;
            return (T)this;
        }

        public T ttl(int ttl)
        {
            this.ttl = ttl;
            return (T)this;
        }

        public T nowInSec(int nowInSec)
        {
            this.nowInSec = nowInSec;
            return (T)this;
        }
    }

    public static class MutationBuilder extends AbstractBuilder<Mutation.SimpleBuilder> implements Mutation.SimpleBuilder
    {
        private final String keyspaceName;
        private final DecoratedKey key;

        private final Map<UUID, PartitionUpdateBuilder> updateBuilders = new HashMap<>();

        public MutationBuilder(String keyspaceName, DecoratedKey key)
        {
            this.keyspaceName = keyspaceName;
            this.key = key;
        }

        public PartitionUpdate.SimpleBuilder update(CFMetaData metadata)
        {
            assert metadata.ksName.equals(keyspaceName);

            PartitionUpdateBuilder builder = updateBuilders.get(metadata.cfId);
            if (builder == null)
            {
                builder = new PartitionUpdateBuilder(metadata, key);
                updateBuilders.put(metadata.cfId, builder);
            }

            copyParams(builder);

            return builder;
        }

        public PartitionUpdate.SimpleBuilder update(String tableName)
        {
            CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, tableName);
            assert metadata != null : "Unknown table " + tableName + " in keyspace " + keyspaceName;
            return update(metadata);
        }

        public Mutation build()
        {
            assert !updateBuilders.isEmpty() : "Cannot create empty mutation";

            if (updateBuilders.size() == 1)
                return new Mutation(updateBuilders.values().iterator().next().build());

            Mutation mutation = new Mutation(keyspaceName, key);
            for (PartitionUpdateBuilder builder : updateBuilders.values())
                mutation.add(builder.build());
            return mutation;
        }
    }

    public static class PartitionUpdateBuilder extends AbstractBuilder<PartitionUpdate.SimpleBuilder> implements PartitionUpdate.SimpleBuilder
    {
        private final CFMetaData metadata;
        private final DecoratedKey key;
        private final Map<Clustering, RowBuilder> rowBuilders = new HashMap<>();
        private List<RTBuilder> rangeBuilders = null; // We use that rarely, so create lazily

        private DeletionTime partitionDeletion = DeletionTime.LIVE;

        public PartitionUpdateBuilder(CFMetaData metadata, Object... partitionKeyValues)
        {
            this.metadata = metadata;
            this.key = makePartitonKey(metadata, partitionKeyValues);
        }

        public CFMetaData metadata()
        {
            return metadata;
        }

        public Row.SimpleBuilder row(Object... clusteringValues)
        {
            Clustering clustering = makeClustering(metadata, clusteringValues);
            RowBuilder builder = rowBuilders.get(clustering);
            if (builder == null)
            {
                builder = new RowBuilder(metadata, clustering);
                rowBuilders.put(clustering, builder);
            }

            copyParams(builder);

            return builder;
        }

        public PartitionUpdate.SimpleBuilder delete()
        {
            this.partitionDeletion = new DeletionTime(timestamp, nowInSec);
            return this;
        }

        public RangeTombstoneBuilder addRangeTombstone()
        {
            if (rangeBuilders == null)
                rangeBuilders = new ArrayList<>();

            RTBuilder builder = new RTBuilder(metadata.comparator, new DeletionTime(timestamp, nowInSec));
            rangeBuilders.add(builder);
            return builder;
        }

        public PartitionUpdate build()
        {
            // Collect all updated columns
            PartitionColumns.Builder columns = PartitionColumns.builder();
            for (RowBuilder builder : rowBuilders.values())
                columns.addAll(builder.columns());

            // Note that rowBuilders.size() could include the static column so could be 1 off the really need capacity
            // of the final PartitionUpdate, but as that's just a sizing hint, we'll live.
            PartitionUpdate update = new PartitionUpdate(metadata, key, columns.build(), rowBuilders.size());

            update.addPartitionDeletion(partitionDeletion);
            if (rangeBuilders != null)
            {
                for (RTBuilder builder : rangeBuilders)
                    update.add(builder.build());
            }

            for (RowBuilder builder : rowBuilders.values())
                update.add(builder.build());

            return update;
        }

        public Mutation buildAsMutation()
        {
            return new Mutation(build());
        }

        private static class RTBuilder implements RangeTombstoneBuilder
        {
            private final ClusteringComparator comparator;
            private final DeletionTime deletionTime;

            private Object[] start;
            private Object[] end;

            private boolean startInclusive = true;
            private boolean endInclusive = true;

            private RTBuilder(ClusteringComparator comparator, DeletionTime deletionTime)
            {
                this.comparator = comparator;
                this.deletionTime = deletionTime;
            }

            public RangeTombstoneBuilder start(Object... values)
            {
                this.start = values;
                return this;
            }

            public RangeTombstoneBuilder end(Object... values)
            {
                this.end = values;
                return this;
            }

            public RangeTombstoneBuilder inclStart()
            {
                this.startInclusive = true;
                return this;
            }

            public RangeTombstoneBuilder exclStart()
            {
                this.startInclusive = false;
                return this;
            }

            public RangeTombstoneBuilder inclEnd()
            {
                this.endInclusive = true;
                return this;
            }

            public RangeTombstoneBuilder exclEnd()
            {
                this.endInclusive = false;
                return this;
            }

            private RangeTombstone build()
            {
                ClusteringBound startBound = ClusteringBound.create(comparator, true, startInclusive, start);
                ClusteringBound endBound = ClusteringBound.create(comparator, false, endInclusive, end);
                return new RangeTombstone(Slice.make(startBound, endBound), deletionTime);
            }
        }
    }

    public static class RowBuilder extends AbstractBuilder<Row.SimpleBuilder> implements Row.SimpleBuilder
    {
        private final CFMetaData metadata;

        private final Set<ColumnDefinition> columns = new HashSet<>();
        private final Row.Builder builder;

        private boolean initiated;
        private boolean noPrimaryKeyLivenessInfo;

        public RowBuilder(CFMetaData metadata, Object... clusteringColumns)
        {
            this.metadata = metadata;
            this.builder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());

            this.builder.newRow(makeClustering(metadata, clusteringColumns));
        }

        Set<ColumnDefinition> columns()
        {
            return columns;
        }

        private void maybeInit()
        {
            // We're working around the fact that Row.Builder requires that addPrimaryKeyLivenessInfo() and
            // addRowDeletion() are called before any cell addition (which is done so the builder can more easily skip
            // shadowed cells).
            if (initiated)
                return;

            // If a CQL table, add the "row marker"
            if (metadata.isCQLTable() && !noPrimaryKeyLivenessInfo)
                builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, ttl, nowInSec));

            initiated = true;
        }

        public Row.SimpleBuilder add(String columnName, Object value)
        {
            return add(columnName, value, true);
        }

        public Row.SimpleBuilder appendAll(String columnName, Object value)
        {
            return add(columnName, value, false);
        }

        private Row.SimpleBuilder add(String columnName, Object value, boolean overwriteForCollection)
        {
            maybeInit();
            ColumnDefinition column = getColumn(columnName);

            if (!overwriteForCollection && !(column.type.isMultiCell() && column.type.isCollection()))
                throw new IllegalArgumentException("appendAll() can only be called on non-frozen colletions");

            columns.add(column);

            if (!column.type.isMultiCell())
            {
                builder.addCell(cell(column, toByteBuffer(value, column.type), null));
                return this;
            }

            assert column.type instanceof CollectionType : "Collection are the only multi-cell types supported so far";

            if (value == null)
            {
                builder.addComplexDeletion(column, new DeletionTime(timestamp, nowInSec));
                return this;
            }

            // Erase previous entry if any.
            if (overwriteForCollection)
                builder.addComplexDeletion(column, new DeletionTime(timestamp - 1, nowInSec));
            switch (((CollectionType)column.type).kind)
            {
                case LIST:
                    ListType lt = (ListType)column.type;
                    assert value instanceof List;
                    for (Object elt : (List)value)
                        builder.addCell(cell(column, toByteBuffer(elt, lt.getElementsType()), CellPath.create(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()))));
                    break;
                case SET:
                    SetType st = (SetType)column.type;
                    assert value instanceof Set;
                    for (Object elt : (Set)value)
                        builder.addCell(cell(column, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(toByteBuffer(elt, st.getElementsType()))));
                    break;
                case MAP:
                    MapType mt = (MapType)column.type;
                    assert value instanceof Map;
                    for (Map.Entry entry : ((Map<?, ?>)value).entrySet())
                        builder.addCell(cell(column,
                                             toByteBuffer(entry.getValue(), mt.getValuesType()),
                                             CellPath.create(toByteBuffer(entry.getKey(), mt.getKeysType()))));
                    break;
                default:
                    throw new AssertionError();
            }
            return this;
        }

        public Row.SimpleBuilder delete()
        {
            assert !initiated : "If called, delete() should be called before any other column value addition";
            builder.addRowDeletion(Row.Deletion.regular(new DeletionTime(timestamp, nowInSec)));
            return this;
        }

        public Row.SimpleBuilder delete(String columnName)
        {
            return add(columnName, null);
        }

        public Row.SimpleBuilder noPrimaryKeyLivenessInfo()
        {
            this.noPrimaryKeyLivenessInfo = true;
            return this;
        }

        public Row build()
        {
            maybeInit();
            return builder.build();
        }

        private ColumnDefinition getColumn(String columnName)
        {
            ColumnDefinition column = metadata.getColumnDefinition(new ColumnIdentifier(columnName, true));
            assert column != null : "Cannot find column " + columnName;
            assert !column.isPrimaryKeyColumn();
            assert !column.isStatic() || builder.clustering() == Clustering.STATIC_CLUSTERING : "Cannot add non-static column to static-row";
            return column;
        }

        private Cell cell(ColumnDefinition column, ByteBuffer value, CellPath path)
        {
            if (value == null)
                return BufferCell.tombstone(column, timestamp, nowInSec, path);

            return ttl == LivenessInfo.NO_TTL
                 ? BufferCell.live(column, timestamp, value, path)
                 : BufferCell.expiring(column, timestamp, ttl, nowInSec, value, path);
        }

        private ByteBuffer toByteBuffer(Object value, AbstractType<?> type)
        {
            if (value == null)
                return null;

            if (value instanceof ByteBuffer)
                return (ByteBuffer)value;

            if (type.isCounter())
            {
                // See UpdateParameters.addCounter()
                assert value instanceof Long : "Attempted to adjust Counter cell with non-long value.";
                return CounterContext.instance().createGlobal(CounterId.getLocalId(), 1, (Long)value);
            }

            return ((AbstractType)type).decompose(value);
        }
    }
}
