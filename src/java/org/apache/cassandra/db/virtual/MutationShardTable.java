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

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.replication.CoordinatorLog;
<<<<<<< HEAD:src/java/org/apache/cassandra/db/virtual/MutationShardTable.java
=======
import org.apache.cassandra.replication.CoordinatorLogId;
>>>>>>> c1b8c33eb9 (Minor PR feedback):src/java/org/apache/cassandra/db/virtual/MutationTrackingShardsTable.java
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Shard;
import org.apache.cassandra.schema.TableMetadata;

<<<<<<< HEAD:src/java/org/apache/cassandra/db/virtual/MutationShardTable.java
public class MutationShardTable extends AbstractVirtualTable
=======
import java.util.Map;

public class MutationTrackingShardsTable extends AbstractVirtualTable
>>>>>>> c1b8c33eb9 (Minor PR feedback):src/java/org/apache/cassandra/db/virtual/MutationTrackingShardsTable.java
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

    MutationShardTable(String keyspace) {
        super(TableMetadata.builder(keyspace, "mutation_shards")
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

<<<<<<< HEAD:src/java/org/apache/cassandra/db/virtual/MutationShardTable.java
=======
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

>>>>>>> c1b8c33eb9 (Minor PR feedback):src/java/org/apache/cassandra/db/virtual/MutationTrackingShardsTable.java
    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Shard shard : MutationTrackingService.instance.getShards())
        {
            for (CoordinatorLog log : shard.getLogs())
            {
                result.row(shard.getKeyspace(),
                           shard.getRange().left.toString(),
                           shard.getRange().right.toString(),
                           log.getLogId().toString())
                      .column(LOCAL_NODE_ID, shard.getLocalNodeId())
                      .column(PARTICIPANTS, shard.getParticipants().toString())
                      .column(WITNESSED_OFFSETS, log.getWitnessedOffsets())
                      .column(RECONCILED_OFFSETS, log.getReconciledOffsets())
                      .column(PERSISTED_OFFSETS, log.getPersistedOffsets());
            }
        }

        return result;
    }
}
