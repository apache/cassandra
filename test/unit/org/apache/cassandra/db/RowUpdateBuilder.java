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
 * Convenience object to create single row updates for tests.
 *
 * This is a thin wrapper over the builders in SimpleBuilders for historical reasons.
 * We could modify all the tests using this class to use the simple builders directly
 * instead, but there is a fair amount of use so the value of such effort is unclear.
 */
public class RowUpdateBuilder
{
    private final PartitionUpdate.SimpleBuilder updateBuilder;
    private Row.SimpleBuilder rowBuilder;
    private boolean noRowMarker;

    private List<RangeTombstone> rts = new ArrayList<>();

    private RowUpdateBuilder(PartitionUpdate.SimpleBuilder updateBuilder)
    {
        this.updateBuilder = updateBuilder;
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
        this(PartitionUpdate.simpleBuilder(metadata, partitionKey));

        this.updateBuilder.timestamp(timestamp);
        this.updateBuilder.ttl(ttl);
        this.updateBuilder.nowInSec(localDeletionTime);
    }

    private Row.SimpleBuilder rowBuilder()
    {
        // Normally, rowBuilder is created by the call to clustering(), but we allow skipping that call for an empty
        // clustering.
        if (rowBuilder == null)
        {
            rowBuilder = updateBuilder.row();
            if (noRowMarker)
                rowBuilder.noPrimaryKeyLivenessInfo();
        }

        return rowBuilder;
    }

    // This must be called before any addition or deletion if used.
    public RowUpdateBuilder noRowMarker()
    {
        this.noRowMarker = true;
        if (rowBuilder != null)
            rowBuilder.noPrimaryKeyLivenessInfo();
        return this;
    }

    public RowUpdateBuilder clustering(Object... clusteringValues)
    {
        assert rowBuilder == null;
        rowBuilder = updateBuilder.row(clusteringValues);
        if (noRowMarker)
            rowBuilder.noPrimaryKeyLivenessInfo();
        return this;
    }

    public Mutation build()
    {
        return new Mutation(buildUpdate());
    }

    public PartitionUpdate buildUpdate()
    {
        PartitionUpdate update = updateBuilder.build();
        for (RangeTombstone rt : rts)
            update.add(rt);
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

    public RowUpdateBuilder addRangeTombstone(RangeTombstone rt)
    {
        rts.add(rt);
        return this;
    }

    public RowUpdateBuilder addRangeTombstone(Object start, Object end)
    {
        updateBuilder.addRangeTombstone().start(start).end(end);
        return this;
    }

    public RowUpdateBuilder add(String columnName, Object value)
    {
        rowBuilder().add(columnName, value);
        return this;
    }

    public RowUpdateBuilder add(ColumnDefinition columnDefinition, Object value)
    {
        return add(columnDefinition.name.toString(), value);
    }

    public RowUpdateBuilder delete(String columnName)
    {
        rowBuilder().delete(columnName);
        return this;
    }

    public RowUpdateBuilder delete(ColumnDefinition columnDefinition)
    {
        return delete(columnDefinition.name.toString());
    }
}
