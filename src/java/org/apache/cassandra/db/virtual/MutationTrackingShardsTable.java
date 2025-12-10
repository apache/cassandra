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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.replication.CoordinatorLog;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Shard;
import org.apache.cassandra.schema.TableMetadata;

import java.util.Map;

public class MutationTrackingShardsTable extends AbstractVirtualTable
{
    private static final String KEYSPACE = "keyspace";
    private static final String LOG_ID = "log_id";
    private static final String RANGE_START = "range_start";
    private static final String RANGE_END = "range_end";
    private static final String LOCAL_NODE_ID = "local_node_id";
    private static final String PARTICIPANTS = "participants";
    private static final String WITNESSED_OFFSETS = "witnessed_offsets";
    private static final String RECONCILED_OFFSETS = "reconciled_offsets";
    private static final String PERSISTED_OFFSETS = "persisted_offsets";

    MutationTrackingShardsTable(String keyspace) {
        super(TableMetadata.builder(keyspace, "mutation_tracking_shards")
                           .comment("mutation tracking shards and their offset information")
                           .kind(TableMetadata.Kind.VIRTUAL).partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                           .addClusteringColumn(LOG_ID, UTF8Type.instance)
                           .addClusteringColumn(RANGE_START, UTF8Type.instance)
                           .addClusteringColumn(RANGE_END, UTF8Type.instance)
                           .addRegularColumn(LOCAL_NODE_ID, Int32Type.instance)
                           .addRegularColumn(PARTICIPANTS, UTF8Type.instance)
                           .addRegularColumn(WITNESSED_OFFSETS, UTF8Type.instance)
                           .addRegularColumn(RECONCILED_OFFSETS, UTF8Type.instance)
                           .addRegularColumn(PERSISTED_OFFSETS, UTF8Type.instance)
                           .build());
    }

    private void addShardRows(Shard shard, SimpleDataSet result)
    {
        Shard.DebugInfo shardDebugInfo = shard.getDebugInfo();
        for (Map.Entry<CoordinatorLogId, CoordinatorLog.DebugInfo> entry : shardDebugInfo.logs.entrySet())
        {
            CoordinatorLogId logId = entry.getKey();
            CoordinatorLog.DebugInfo logDebugInfo = entry.getValue();
            result.row(shardDebugInfo.keyspace,
                       logId.toString(),
                       shardDebugInfo.range.left.toString(),
                       shardDebugInfo.range.right.toString())
                  .column(LOCAL_NODE_ID, shardDebugInfo.localNodeId)
                  .column(PARTICIPANTS, shardDebugInfo.participants.toString())
                  .column(WITNESSED_OFFSETS, logDebugInfo.witnessedOffsets)
                  .column(RECONCILED_OFFSETS, logDebugInfo.reconciledOffsets)
                  .column(PERSISTED_OFFSETS, logDebugInfo.persistedOffsets);
        }
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Shard shard : MutationTrackingService.instance.getShards())
        {
            addShardRows(shard, result);
        }

        return result;
    }

    @Override
    public DataSet data(DecoratedKey key)
    {
        String keyspaceName = UTF8Type.instance.compose(key.getKey());
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Shard shard : MutationTrackingService.instance.getShards())
        {
            Shard.DebugInfo debugInfo = shard.getDebugInfo();
            if (!debugInfo.keyspace.equals(keyspaceName))
                continue;

            addShardRows(shard, result);
        }

        return result;
    }
}
