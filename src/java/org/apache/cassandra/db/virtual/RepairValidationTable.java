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

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.ValidationProgress;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;

public class RepairValidationTable extends AbstractVirtualTable
{
    protected RepairValidationTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "repair_validation")
                           .comment("repair validations")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UUIDType.instance))
                           .addPartitionKeyColumn("parent_session_id", UUIDType.instance)
                           .addClusteringColumn("session_id", UUIDType.instance)
                           .addClusteringColumn("ranges", ListType.getInstance(UTF8Type.instance, false))
                           .addRegularColumn("keyspace", UTF8Type.instance)
                           .addRegularColumn("column_family", UTF8Type.instance)
                           .addRegularColumn("state", UTF8Type.instance)
                           .addRegularColumn("progress_percentage", FloatType.instance)
                           .addRegularColumn("queue_duration_ms", LongType.instance)
                           .addRegularColumn("runtime_duration_ms", LongType.instance)
                           .addRegularColumn("total_duration_ms", LongType.instance)
                           .addRegularColumn("estimated_partitions", LongType.instance)
                           .addRegularColumn("partitions_processed", LongType.instance)
                           .addRegularColumn("estimated_total_bytes", LongType.instance)
                           .addRegularColumn("failure_cause", UTF8Type.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        ActiveRepairService.instance.validationProgress((a, b) -> updateDataset(result, a, b));
        return result;
    }

    public DataSet data(DecoratedKey partitionKey)
    {
        UUID parentSessionId = UUIDType.instance.compose(partitionKey.getKey());
        SimpleDataSet result = new SimpleDataSet(metadata());
        ActiveRepairService.instance.validationProgress(parentSessionId, (a, b) -> updateDataset(result, a, b));
        return result;
    }

    private void updateDataset(SimpleDataSet dataSet, RepairJobDesc desc, ValidationProgress progress)
    {
        // call this early to make sure progress state is visible
        long lastUpdatedNs = progress.getLastUpdatedAtNs();
        long creationTimeNs = progress.getCreationtTimeNs();
        long startTimeNs = progress.getStartTimeNs();

        UUID parentSessionId = desc.parentSessionId;
        UUID sessionId = desc.sessionId;
        String ks = desc.keyspace;
        String cf = desc.columnFamily;
        Collection<Range<Token>> ranges = desc.ranges;

        dataSet.row(parentSessionId, sessionId, ranges.stream().map(Range::toString).collect(Collectors.toList()));

        dataSet.column("keyspace", ks);
        dataSet.column("column_family", cf);

        dataSet.column("state", progress.getState().name().toLowerCase());
        dataSet.column("progress_percentage", 100 * progress.getProgress());

        dataSet.column("estimated_partitions", progress.getEstimatedPartitions());
        dataSet.column("partitions_processed", progress.getPartitionsProcessed());
        dataSet.column("estimated_total_bytes", progress.getEstimatedTotalBytes());

        dataSet.column("queue_duration_ms", TimeUnit.NANOSECONDS.toMillis(startTimeNs - creationTimeNs));
        dataSet.column("runtime_duration_ms", TimeUnit.NANOSECONDS.toMillis(lastUpdatedNs - startTimeNs));
        dataSet.column("total_duration_ms", TimeUnit.NANOSECONDS.toMillis(lastUpdatedNs - creationTimeNs));

        if (progress.getFailureCause() != null)
            dataSet.column("failure_cause", Throwables.getStackTraceAsString(progress.getFailureCause()));
    }
}
