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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.utils.*;

/**
 * Convenience object to create single row updates.
 *
 * This is meant for system table update, when performance is not of the utmost importance.
 */
public class RowUpdateBuilder
{
    private final PartitionUpdate update;

    private final long timestamp;
    private final int ttl;
    private final int localDeletionTime;

    private final DeletionTime deletionTime;

    private final Mutation mutation;

    private Row.Builder regularBuilder;
    private Row.Builder staticBuilder;

    private boolean useRowMarker = true;

    private RowUpdateBuilder(PartitionUpdate update, long timestamp, int ttl, int localDeletionTime, Mutation mutation)
    {
        this.update = update;

        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = localDeletionTime;
        this.deletionTime = new DeletionTime(timestamp, localDeletionTime);

        // note that the created mutation may get further update later on, so we don't use the ctor that create a singletonMap
        // underneath (this class if for convenience, not performance)
        this.mutation = mutation == null ? new Mutation(update.metadata().ksName, update.partitionKey()).add(update) : mutation;
    }

    private RowUpdateBuilder(PartitionUpdate update, long timestamp, int ttl, Mutation mutation)
    {
        this(update, timestamp, ttl, FBUtilities.nowInSeconds(), mutation);
    }

    private void startRow(Clustering clustering)
    {
        assert staticBuilder == null : "Cannot update both static and non-static columns with the same RowUpdateBuilder object";
        assert regularBuilder == null : "Cannot add the clustering twice to the same row";

        regularBuilder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
        regularBuilder.newRow(clustering);

        // If a CQL table, add the "row marker"
        if (update.metadata().isCQLTable() && useRowMarker)
            regularBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, ttl, localDeletionTime));
    }

    private Row.Builder builder()
    {
        assert staticBuilder == null : "Cannot update both static and non-static columns with the same RowUpdateBuilder object";
        if (regularBuilder == null)
        {
            // we don't force people to call clustering() if the table has no clustering, so call it ourselves
            assert update.metadata().comparator.size() == 0 : "Missing call to clustering()";
            startRow(Clustering.EMPTY);
        }
        return regularBuilder;
    }

    private Row.Builder staticBuilder()
    {
        assert regularBuilder == null : "Cannot update both static and non-static columns with the same RowUpdateBuilder object";
        if (staticBuilder == null)
        {
            staticBuilder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
            staticBuilder.newRow(Clustering.STATIC_CLUSTERING);
        }
        return staticBuilder;
    }

    private Row.Builder builder(ColumnDefinition c)
    {
        return c.isStatic() ? staticBuilder() : builder();
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, Object partitionKey)
    {
        this(metadata, FBUtilities.nowInSeconds(), timestamp, partitionKey);
    }

    public RowUpdateBuilder(CFMetaData metadata, int localDeletionTime, long timestamp, Object partitionKey)
    {
        this(metadata, localDeletionTime, timestamp, metadata.params.defaultTimeToLive, partitionKey);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, int ttl, Object partitionKey)
    {
        this(metadata, FBUtilities.nowInSeconds(), timestamp, ttl, partitionKey);
    }

    public RowUpdateBuilder(CFMetaData metadata, int localDeletionTime, long timestamp, int ttl, Object partitionKey)
    {
        this(new PartitionUpdate(metadata, makeKey(metadata, partitionKey), metadata.partitionColumns(), 1), timestamp, ttl, localDeletionTime, null);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, Mutation mutation)
    {
        this(metadata, timestamp, LivenessInfo.NO_TTL, mutation);
    }

    public RowUpdateBuilder(CFMetaData metadata, long timestamp, int ttl, Mutation mutation)
    {
        this(getOrAdd(metadata, mutation), timestamp, ttl, mutation);
    }

    public RowUpdateBuilder(PartitionUpdate update, long timestamp, int ttl)
    {
        this(update, timestamp, ttl, null);
    }

    // This must be called before any addition or deletion if used.
    public RowUpdateBuilder noRowMarker()
    {
        this.useRowMarker = false;
        return this;
    }

    public RowUpdateBuilder clustering(Object... clusteringValues)
    {
        assert clusteringValues.length == update.metadata().comparator.size()
             : "Invalid clustering values length. Expected: " + update.metadata().comparator.size() + " got: " + clusteringValues.length;

        startRow(clusteringValues.length == 0 ? Clustering.EMPTY : update.metadata().comparator.make(clusteringValues));
        return this;
    }

    public Mutation build()
    {
        Row.Builder builder = regularBuilder == null ? staticBuilder : regularBuilder;
        if (builder != null)
            update.add(builder.build());
        return mutation;
    }

    public PartitionUpdate buildUpdate()
    {
        build();
        return update;
    }

    private static void deleteRow(PartitionUpdate update, long timestamp, int localDeletionTime, Object... clusteringValues)
    {
        assert clusteringValues.length == update.metadata().comparator.size() || (clusteringValues.length == 0 && !update.columns().statics.isEmpty());

        boolean isStatic = clusteringValues.length != update.metadata().comparator.size();
        Row.Builder builder = BTreeRow.sortedBuilder();

        if (isStatic)
            builder.newRow(Clustering.STATIC_CLUSTERING);
        else
            builder.newRow(clusteringValues.length == 0 ? Clustering.EMPTY : update.metadata().comparator.make(clusteringValues));
        builder.addRowDeletion(Row.Deletion.regular(new DeletionTime(timestamp, localDeletionTime)));

        update.add(builder.build());
    }

    public static Mutation deleteRow(CFMetaData metadata, long timestamp, Mutation mutation, Object... clusteringValues)
    {
        deleteRow(getOrAdd(metadata, mutation), timestamp, FBUtilities.nowInSeconds(), clusteringValues);
        return mutation;
    }

    public static Mutation deleteRow(CFMetaData metadata, long timestamp, Object key, Object... clusteringValues)
    {
        return deleteRowAt(metadata, timestamp, FBUtilities.nowInSeconds(), key, clusteringValues);
    }

    public static Mutation deleteRowAt(CFMetaData metadata, long timestamp, int localDeletionTime, Object key, Object... clusteringValues)
    {
        PartitionUpdate update = new PartitionUpdate(metadata, makeKey(metadata, key), metadata.partitionColumns(), 0);
        deleteRow(update, timestamp, localDeletionTime, clusteringValues);
        // note that the created mutation may get further update later on, so we don't use the ctor that create a singletonMap
        // underneath (this class if for convenience, not performance)
        return new Mutation(update.metadata().ksName, update.partitionKey()).add(update);
    }

    private static DecoratedKey makeKey(CFMetaData metadata, Object... partitionKey)
    {
        if (partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey)
            return (DecoratedKey)partitionKey[0];

        ByteBuffer key = CFMetaData.serializePartitionKey(metadata.getKeyValidatorAsClusteringComparator().make(partitionKey));
        return metadata.decorateKey(key);
    }

    private static PartitionUpdate getOrAdd(CFMetaData metadata, Mutation mutation)
    {
        PartitionUpdate upd = mutation.get(metadata);
        if (upd == null)
        {
            upd = new PartitionUpdate(metadata, mutation.key(), metadata.partitionColumns(), 1);
            mutation.add(upd);
        }
        return upd;
    }

    public RowUpdateBuilder resetCollection(String columnName)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c != null : "Cannot find column " + columnName;
        assert c.isStatic() || update.metadata().comparator.size() == 0 || regularBuilder != null : "Cannot set non static column " + c + " since no clustering has been provided";
        assert c.type.isCollection() && c.type.isMultiCell();
        builder(c).addComplexDeletion(c, new DeletionTime(timestamp - 1, localDeletionTime));
        return this;
    }

    public RowUpdateBuilder addRangeTombstone(RangeTombstone rt)
    {
        update.add(rt);
        return this;
    }

    public RowUpdateBuilder addRangeTombstone(Slice slice)
    {
        return addRangeTombstone(new RangeTombstone(slice, deletionTime));
    }

    public RowUpdateBuilder addRangeTombstone(Object start, Object end)
    {
        ClusteringComparator cmp = update.metadata().comparator;
        Slice slice = Slice.make(cmp.make(start), cmp.make(end));
        return addRangeTombstone(slice);
    }

    public RowUpdateBuilder add(String columnName, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c != null : "Cannot find column " + columnName;
        return add(c, value);
    }

    private Cell makeCell(ColumnDefinition c, ByteBuffer value, CellPath path)
    {
        return value == null
             ? BufferCell.tombstone(c, timestamp, localDeletionTime)
             : (ttl == LivenessInfo.NO_TTL ? BufferCell.live(c, timestamp, value, path) : BufferCell.expiring(c, timestamp, ttl, localDeletionTime, value, path));
    }

    public RowUpdateBuilder add(ColumnDefinition columnDefinition, Object value)
    {
        assert columnDefinition.isStatic() || update.metadata().comparator.size() == 0 || regularBuilder != null : "Cannot set non static column " + columnDefinition + " since no clustering hasn't been provided";
        builder(columnDefinition).addCell(makeCell(columnDefinition, bb(value, columnDefinition.type), null));
        return this;
    }

    public RowUpdateBuilder delete(String columnName)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c != null : "Cannot find column " + columnName;
        return delete(c);
    }

    public RowUpdateBuilder delete(ColumnDefinition columnDefinition)
    {
        return add(columnDefinition, null);
    }

    private static ByteBuffer bb(Object value, AbstractType<?> type)
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

    public RowUpdateBuilder map(String columnName, Map<?, ?> map)
    {
        resetCollection(columnName);
        for (Map.Entry<?, ?> entry : map.entrySet())
            addMapEntry(columnName, entry.getKey(), entry.getValue());
        return this;
    }

    public RowUpdateBuilder set(String columnName, Set<?> set)
    {
        resetCollection(columnName);
        for (Object element : set)
            addSetEntry(columnName, element);
        return this;
    }

    public RowUpdateBuilder frozenList(String columnName, List<?> list)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.isStatic() || regularBuilder != null : "Cannot set non static column " + c + " since no clustering has been provided";
        assert c.type instanceof ListType && !c.type.isMultiCell() : "Column " + c + " is not a frozen list";
        builder(c).addCell(makeCell(c, bb(((AbstractType)c.type).decompose(list), c.type), null));
        return this;
    }

    public RowUpdateBuilder frozenSet(String columnName, Set<?> set)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.isStatic() || regularBuilder != null : "Cannot set non static column " + c + " since no clustering has been provided";
        assert c.type instanceof SetType && !c.type.isMultiCell() : "Column " + c + " is not a frozen set";
        builder(c).addCell(makeCell(c, bb(((AbstractType)c.type).decompose(set), c.type), null));
        return this;
    }

    public RowUpdateBuilder frozenMap(String columnName, Map<?, ?> map)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.isStatic() || regularBuilder != null : "Cannot set non static column " + c + " since no clustering has been provided";
        assert c.type instanceof MapType && !c.type.isMultiCell() : "Column " + c + " is not a frozen map";
        builder(c).addCell(makeCell(c, bb(((AbstractType)c.type).decompose(map), c.type), null));
        return this;
    }

    public RowUpdateBuilder addMapEntry(String columnName, Object key, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.isStatic() || update.metadata().comparator.size() == 0 || regularBuilder != null : "Cannot set non static column " + c + " since no clustering has been provided";
        assert c.type instanceof MapType && c.type.isMultiCell() : "Column " + c + " is not a non-frozen map";
        MapType mt = (MapType)c.type;
        builder(c).addCell(makeCell(c, bb(value, mt.getValuesType()), CellPath.create(bb(key, mt.getKeysType()))));
        return this;
    }

    public RowUpdateBuilder addListEntry(String columnName, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.isStatic() || regularBuilder != null : "Cannot set non static column " + c + " since no clustering has been provided";
        assert c.type instanceof ListType && c.type.isMultiCell() : "Column " + c + " is not a non-frozen list";
        ListType lt = (ListType)c.type;
        builder(c).addCell(makeCell(c, bb(value, lt.getElementsType()), CellPath.create(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()))));
        return this;
    }

    public RowUpdateBuilder addSetEntry(String columnName, Object value)
    {
        ColumnDefinition c = getDefinition(columnName);
        assert c.isStatic() || regularBuilder != null : "Cannot set non static column " + c + " since no clustering has been provided";
        assert c.type instanceof SetType && c.type.isMultiCell() : "Column " + c + " is not a non-frozen set";
        SetType st = (SetType)c.type;
        builder(c).addCell(makeCell(c, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(bb(value, st.getElementsType()))));
        return this;
    }

    private ColumnDefinition getDefinition(String name)
    {
        return update.metadata().getColumnDefinition(new ColumnIdentifier(name, true));
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        return update.unfilteredIterator();
    }
}
