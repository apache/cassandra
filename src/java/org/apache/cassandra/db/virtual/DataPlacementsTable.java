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

package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.ReversedLongLocalPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;

final class DataPlacementsTable extends AbstractVirtualTable
{
    static final String TABLE_COMMENT = "Data placement information showing read and write endpoints per range";

    static final String TABLE_NAME = "data_placements";

    // Partition key columns
    static final String KEYSPACE_NAME = "keyspace_name";
    static final String TABLE_NAME_COLUMN = "table_name";
    static final String RANGE_START = "range_start";
    static final String RANGE_END = "range_end";

    // Regular columns
    static final String RANGE_START_BYTES = "range_start_bytes";
    static final String RANGE_END_BYTES = "range_end_bytes";
    static final String TOKEN_TYPE = "token_type";
    static final String READ_ENDPOINTS = "read_endpoints";
    static final String WRITE_ENDPOINTS = "write_endpoints";
    static final String READ_REPLICAS = "read_replicas";
    static final String WRITE_REPLICAS = "write_replicas";

    DataPlacementsTable(String keyspace)
    {
        super(buildTableMetadata(keyspace));
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        ClusterMetadata metadata = ClusterMetadata.current();
        DataPlacements placements = metadata.placements;

        for (KeyspaceMetadata ksm : metadata.schema.getKeyspaces())
        {
            ReplicationParams params = ksm.params.replication;
            DataPlacement placement = placements.get(params);

            for (TableMetadata table : ksm.tables)
                addPlacementRows(result, ksm.name, table.name, params, placement, metadata, null, null);
        }

        return result;
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        ByteBuffer keyBytes = partitionKey.getKey();

        Preconditions.checkArgument(metadata().partitionKeyType instanceof CompositeType,
                                    "Expected CompositeType partition key, got %s", metadata().partitionKeyType.getClass());

        ByteBuffer[] components = ((CompositeType) metadata().partitionKeyType).split(keyBytes);
        Preconditions.checkArgument(components.length == 1 || components.length == 2 || components.length == 4,
                                    "Expected 1, 2, or 4 partition key components (keyspace[, table[, range_start, range_end]]), got %d",
                                    components.length);

        String keyspaceName = UTF8Type.instance.compose(components[0]);
        String tableName = components.length >= 2 ? UTF8Type.instance.compose(components[1]) : null;
        String rangeStartFilter = components.length == 4 ? UTF8Type.instance.compose(components[2]) : null;
        String rangeEndFilter = components.length == 4 ? UTF8Type.instance.compose(components[3]) : null;

        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata ksm = metadata.schema.getKeyspaces().getNullable(keyspaceName);
        if (ksm == null)
            return result;

        ReplicationParams params = ksm.params.replication;
        DataPlacement placement = metadata.placements.get(params);
        Preconditions.checkState(placement != null, "Placements should never be null for %s", keyspaceName);

        if (tableName != null)
        {
            TableMetadata table = ksm.getTableOrViewNullable(tableName);
            Preconditions.checkArgument(table != null, "Couldn't find table %s", tableName);
            addPlacementRows(result, ksm.name, table.name, params, placement, metadata, rangeStartFilter, rangeEndFilter);
        }
        else
        {
            for (TableMetadata table : ksm.tables)
                addPlacementRows(result, ksm.name, table.name, params, placement, metadata, null, null);
        }

        return result;
    }

    private void addPlacementRows(SimpleDataSet result, String keyspaceName, String tableName, ReplicationParams params, DataPlacement placement, ClusterMetadata metadata, String rangeStartFilter, String rangeEndFilter)
    {
        Preconditions.checkState(placement.reads.ranges.equals(placement.writes.ranges),
                                 "Read ranges (%s) are not the same as write ranges (%s)",
                                 placement.reads.ranges, placement.writes.ranges);

        List<Range<Token>> ranges = placement.reads.ranges;
        for (int i = 0; i < ranges.size(); i++)
        {
            Range<Token> range = ranges.get(i);
            if (rangeStartFilter != null && !range.left.toString().equals(rangeStartFilter))
                continue;
            if (rangeEndFilter != null && !range.right.toString().equals(rangeEndFilter))
                continue;

            VersionedEndpoints.ForRange readEndpoints = placement.reads.forRange(range);
            VersionedEndpoints.ForRange writeEndpoints = placement.writes.forRange(range);

            Set<String> readEndpointSet = toEndpointStrings(readEndpoints.get());
            Set<String> writeEndpointSet = toEndpointStrings(writeEndpoints.get());

            Set<Integer> readReplicaSet = toNodeIds(readEndpoints.get(), metadata);
            Set<Integer> writeReplicaSet = toNodeIds(writeEndpoints.get(), metadata);

            IPartitioner partitioner = params.isMeta() ? ReversedLongLocalPartitioner.instance : metadata.partitioner;
            result.row(keyspaceName, tableName, range.left.toString(), range.right.toString())
                  .column(TOKEN_TYPE, range.left.getClass().getCanonicalName())
                  .column(RANGE_START_BYTES, partitioner.getTokenFactory().toByteArray(range.left))
                  .column(RANGE_END_BYTES, partitioner.getTokenFactory().toByteArray(range.right))
                  .column(READ_ENDPOINTS, readEndpointSet)
                  .column(WRITE_ENDPOINTS, writeEndpointSet)
                  .column(READ_REPLICAS, readReplicaSet)
                  .column(WRITE_REPLICAS, writeReplicaSet);
        }
    }

    static Set<String> toEndpointStrings(EndpointsForRange endpoints)
    {
        return endpoints.stream()
                       .map(Replica::endpoint)
                       .map(Object::toString)
                       .collect(Collectors.toSet());
    }

    static Set<Integer> toNodeIds(EndpointsForRange endpoints, ClusterMetadata metadata)
    {
        return endpoints.stream()
                       .map(Replica::endpoint)
                       .map(metadata.directory::peerId)
                       .map(NodeId::id)
                       .collect(Collectors.toSet());
    }

    private static TableMetadata buildTableMetadata(String keyspace)
    {
        return TableMetadata.builder(keyspace, TABLE_NAME)
                            .comment(TABLE_COMMENT)
                            .kind(TableMetadata.Kind.VIRTUAL)
                            .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance, UTF8Type.instance, UTF8Type.instance)))
                            .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                            .addPartitionKeyColumn(TABLE_NAME_COLUMN, UTF8Type.instance)
                            .addPartitionKeyColumn(RANGE_START, UTF8Type.instance)
                            .addPartitionKeyColumn(RANGE_END, UTF8Type.instance)
                            .addRegularColumn(TOKEN_TYPE, UTF8Type.instance)
                            .addRegularColumn(RANGE_START_BYTES, BytesType.instance)
                            .addRegularColumn(RANGE_END_BYTES, BytesType.instance)
                            .addRegularColumn(READ_ENDPOINTS, SetType.getInstance(UTF8Type.instance, false))
                            .addRegularColumn(WRITE_ENDPOINTS, SetType.getInstance(UTF8Type.instance, false))
                            .addRegularColumn(READ_REPLICAS, SetType.getInstance(Int32Type.instance, false))
                            .addRegularColumn(WRITE_REPLICAS, SetType.getInstance(Int32Type.instance, false))
                            .build();
    }
}
