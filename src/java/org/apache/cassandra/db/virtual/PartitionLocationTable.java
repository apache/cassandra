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
import java.util.Set;

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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;

import static com.google.common.base.Preconditions.*;

final class PartitionLocationTable extends AbstractVirtualTable
{
    static final String TABLE_NAME = "partition_location";
    static final String TABLE_COMMENT = "shows the token range and replicas (read and write) for a given partition";

    // Partition keys
    static final String COLUMN_KEYSPACE_NAME = "keyspace_name";
    static final String COLUMN_TABLE_NAME = "table_name";
    static final String COLUMN_KEY = "key";

    // Regular columns
    static final String COLUMN_TOKEN = "tkn"; // can't use "token" because this is reserved in CQL
    static final String COLUMN_TOKEN_BYTES = "tkn_bytes";
    static final String COLUMN_RANGE_START = "range_start";
    static final String COLUMN_RANGE_END = "range_end";
    static final String COLUMN_RANGE_START_BYTES = "range_start_bytes";
    static final String COLUMN_RANGE_END_BYTES = "range_end_bytes";
    static final String COLUMN_READ_ENDPOINTS = "read_endpoints";
    static final String COLUMN_WRITE_ENDPOINTS = "write_endpoints";
    static final String COLUMN_READ_REPLICAS = "read_replicas";
    static final String COLUMN_WRITE_REPLICAS = "write_replicas";

    PartitionLocationTable(String keyspace)
    {
        super(buildTableMetadata(keyspace));
    }

    @Override
    public DataSet data()
    {
        throw new InvalidRequestException("Partition location table requires a partition key, for example: " +
                                          "SELECT * FROM system_views.partition_location WHERE keyspace_name = 'ks' AND table_name = 'tbl' AND key = '1:a'");
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        // Partition key is (keyspace_name, table_name, key)
        ByteBuffer keyBytes = partitionKey.getKey();

        checkArgument(metadata().partitionKeyType instanceof CompositeType,
                                    "PartitionLocationTable partition key type must be CompositeType, got %s",
                                    metadata().partitionKeyType.getClass().getName());

        ByteBuffer[] components = ((CompositeType) metadata().partitionKeyType).split(keyBytes);
        checkArgument(components.length == 3,
                                    "PartitionLocationTable partition key must have exactly 3 components: keyspace_name, table_name, key; got %d",
                                    components.length);

        String keyspaceName = UTF8Type.instance.compose(components[0]);
        String tableName = UTF8Type.instance.compose(components[1]);
        String keyString = UTF8Type.instance.compose(components[2]);

        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata ksm = checkNotNull(metadata.schema.getKeyspaceMetadata(keyspaceName),
                                       "Keyspace %s is not found in metadata", keyspaceName);
        TableMetadata table = checkNotNull(ksm.getTableOrViewNullable(tableName),
                "Table %s is not found in metadata (within keyspace %s)", tableName, keyspaceName);

        ByteBuffer partitionKeyBytes = table.partitionKeyType.fromString(keyString);

        DecoratedKey dk = table.partitioner.decorateKey(partitionKeyBytes);
        Token token = dk.getToken();

        ReplicationParams replicationParams = ksm.params.replication;
        DataPlacement placement = metadata.placements.get(replicationParams);

        VersionedEndpoints.ForRange readEndpoints = placement.reads.forRange(token);
        VersionedEndpoints.ForRange writeEndpoints = placement.writes.forRange(token);

        Range<Token> range = readEndpoints.get().range();

        Set<String> readEndpointSet = DataPlacementsTable.toEndpointStrings(readEndpoints.get());
        Set<String> writeEndpointSet = DataPlacementsTable.toEndpointStrings(writeEndpoints.get());

        Set<Integer> readReplicaSet = DataPlacementsTable.toNodeIds(readEndpoints.get(), metadata);
        Set<Integer> writeReplicaSet = DataPlacementsTable.toNodeIds(writeEndpoints.get(), metadata);

        IPartitioner partitioner = replicationParams.isMeta() ? ReversedLongLocalPartitioner.instance : metadata.partitioner;
        result.row(keyspaceName, tableName, keyString)
              .column(COLUMN_TOKEN, token.toString())
              .column(COLUMN_TOKEN_BYTES, partitioner.getTokenFactory().toByteArray(token))
              .column(COLUMN_RANGE_START, range.left.toString())
              .column(COLUMN_RANGE_END, range.right.toString())
              .column(COLUMN_RANGE_START_BYTES, partitioner.getTokenFactory().toByteArray(range.left))
              .column(COLUMN_RANGE_END_BYTES, partitioner.getTokenFactory().toByteArray(range.right))
              .column(COLUMN_READ_ENDPOINTS, readEndpointSet)
              .column(COLUMN_WRITE_ENDPOINTS, writeEndpointSet)
              .column(COLUMN_READ_REPLICAS, readReplicaSet)
              .column(COLUMN_WRITE_REPLICAS, writeReplicaSet);

        return result;
    }

    private static TableMetadata buildTableMetadata(String keyspace) {
        return TableMetadata.builder(keyspace, TABLE_NAME)
                            .comment(TABLE_COMMENT)
                            .kind(TableMetadata.Kind.VIRTUAL)
                            .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance, UTF8Type.instance)))
                            .addPartitionKeyColumn(COLUMN_KEYSPACE_NAME, UTF8Type.instance)
                            .addPartitionKeyColumn(COLUMN_TABLE_NAME, UTF8Type.instance)
                            .addPartitionKeyColumn(COLUMN_KEY, UTF8Type.instance)
                            .addRegularColumn(COLUMN_TOKEN, UTF8Type.instance)
                            .addRegularColumn(COLUMN_TOKEN_BYTES, BytesType.instance)
                            .addRegularColumn(COLUMN_RANGE_START, UTF8Type.instance)
                            .addRegularColumn(COLUMN_RANGE_END, UTF8Type.instance)
                            .addRegularColumn(COLUMN_RANGE_START_BYTES, BytesType.instance)
                            .addRegularColumn(COLUMN_RANGE_END_BYTES, BytesType.instance)
                            .addRegularColumn(COLUMN_READ_ENDPOINTS, SetType.getInstance(UTF8Type.instance, false))
                            .addRegularColumn(COLUMN_WRITE_ENDPOINTS, SetType.getInstance(UTF8Type.instance, false))
                            .addRegularColumn(COLUMN_READ_REPLICAS, SetType.getInstance(Int32Type.instance, false))
                            .addRegularColumn(COLUMN_WRITE_REPLICAS, SetType.getInstance(Int32Type.instance, false))
                            .build();
    }
}