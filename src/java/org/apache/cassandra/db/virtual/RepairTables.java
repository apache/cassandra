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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.RemoteState;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairState;
import org.apache.cassandra.repair.RepairState.JobState;
import org.apache.cassandra.repair.RepairState.SessionState;
import org.apache.cassandra.repair.ValidationState;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

public class RepairTables
{
    private RepairTables()
    {

    }

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        RepairValidationTable validations = new RepairValidationTable(keyspace);
        return Arrays.asList(
            new RepairTaskTable(keyspace, validations.metadata),
            validations
        );
    }

    public static class RepairTaskTable extends AbstractVirtualTable
    {

        private final TableMetadata validationMetadata;

        public RepairTaskTable(String keyspace, TableMetadata validationMetadata)
        {
            super(parse(keyspace, "Sub tasks that make up a repair",
                        "CREATE TABLE system_views.repair_tasks (\n" +
                        "  id uuid,\n" +
                        "  session_id uuid,\n" +
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  ranges frozen<list<text>>,\n" +
                        "  coordinator text,\n" +
                        "  participant text,\n" +
                        "\n" +
                        "  state text,\n" +
                        "  progress_percentage float, -- between 0.0 and 100.0\n" +
                        "  last_updated_at_millis bigint,\n" +
                        "  duration_micro bigint,\n" +
                        "  failure_cause text,\n" +
                        "\n" +
                        "  -- metrics\n" +
                        "  -- TODO\n" +
                        "\n" +
                        "  PRIMARY KEY ( (id), session_id, table_name, participant )\n" +
                        ")"));
            this.validationMetadata = validationMetadata;
        }

        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            ActiveRepairService.instance.getRepairStates().forEach(s -> updateDataset(result, s));
            return result;
        }

        public DataSet data(DecoratedKey partitionKey)
        {
            UUID id = UUIDType.instance.compose(partitionKey.getKey());
            SimpleDataSet result = new SimpleDataSet(metadata());
            RepairState state = ActiveRepairService.instance.getRepairState(id);
            if (state != null)
                updateDataset(result, state);
            return result;
        }

        private void updateDataset(SimpleDataSet dataSet, RepairState state)
        {
            UUID parentSessionId = state.id;
            String keyspace = state.keyspace;
            InetAddressAndPort coordinator = FBUtilities.getBroadcastAddressAndPort();
            Map<String, String> options = state.options.asMap();

            for (SessionState session : state)
            {
                for (JobState job : session)
                {
                    //TODO if all jobs are validating/syncing, is it best to do blocking at job level?
                    // if blocking was at repair level, then N (num_jobs) * P (num_participants) concurrent messages would go out
                    // if this is "too big" then could get a hoard affect with replies, so by blocking
                    // at the job level, this is bounded to P
                    RepairJobDesc jobDesc = job.desc;

                    // call early to make sure reads are visable
//                    long jobLastUpdatedAtNs = jobProgress.getLastUpdatedAtNs();

                    List<String> ranges = jobDesc.ranges.stream().map(Range::toString).collect(Collectors.toList());

                    CompletableFuture<Map<InetAddressAndPort, RemoteState>> participantStates = job.getParticipantState();
                    //TODO if exception, should query fail or should partition be skipped?
                    for (Map.Entry<InetAddressAndPort, RemoteState> e : participantStates.join().entrySet())
                    {
                        InetAddressAndPort participant = e.getKey();
                        RemoteState remoteState = e.getValue();

                        dataSet.row(parentSessionId, jobDesc.sessionId, jobDesc.columnFamily, participant.toString());

                        // shared columns
                        dataSet.column("keyspace_name", keyspace);
//                        dataSet.column("options", options); //TODO this breaks vtables =)
                        dataSet.column("coordinator", coordinator.toString());

                        // job specific columns
                        dataSet.column("ranges", ranges);

                        // participant state
                        dataSet.column("state", remoteState.state);
                        dataSet.column("progress_percentage", getTaskProgress(job.getState(), remoteState.progress) * 100);
                        dataSet.column("last_updated_at_millis", remoteState.lastUpdatedAtMillis);
                        dataSet.column("duration_micro", TimeUnit.NANOSECONDS.toMicros(remoteState.durationNanos));
                        if (remoteState.failureCause != null)
                            dataSet.column("failure_cause", remoteState.failureCause);
                    }
                }
            }
        }

        private static float getTaskProgress(JobState.State state, float progress)
        {
            // assume validation is 40% of work and syncing is 40% as well (20% for bookkeeping)
            int validationRange = 40;
            int syncRange = 40;
            switch (state)
            {
                case INIT:
                case START:
                    return 0f;
                case SNAPSHOT_REQUEST:
                case SNAPSHOT_COMPLETE:
                    return .2f;
                case VALIDATION_REQUEST:
                    // 21-60%
                    return ((validationRange * progress) + 20) / 100;
                case VALIDATON_COMPLETE:
                    return .6f;
                case SYNC_REQUEST:
                    // 61-100%
                    return ((validationRange * progress) + 60) / 100;
                case SYNC_COMPLETE:
                case FAILURE:
                    return 1f;
                default:
                    throw new IllegalStateException("Unknown state: " + state);
            }
        }
    }

    public static class RepairValidationTable extends AbstractVirtualTable
    {
        protected RepairValidationTable(String keyspace)
        {
            super(parse(keyspace, "repair validations",
                        "CREATE TABLE repair_validations (\n" +
                        "  id uuid,\n" +
                        "  session_id uuid,\n" +
                        "  ranges frozen<list<text>>,\n" +
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  initiator text,\n" +
                        "  state text,\n" +
                        "  progress_percentage float,\n" +
                        "  queue_duration_ms bigint,\n" +
                        "  runtime_duration_ms bigint,\n" +
                        "  total_duration_ms bigint,\n" +
                        "  estimated_partitions bigint,\n" +
                        "  partitions_processed bigint,\n" +
                        "  estimated_total_bytes bigint,\n" +
                        "  failure_cause text,\n" +
                        "\n" +
                        "  PRIMARY KEY ( (id), session_id, table_name )\n" +
                        ")"));
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

        private void updateDataset(SimpleDataSet dataSet, RepairJobDesc desc, ValidationState progress)
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

            dataSet.row(parentSessionId, sessionId, cf);

            dataSet.column("keyspace_name", ks);
            dataSet.column("ranges", ranges.stream().map(Range::toString).collect(Collectors.toList()));
            dataSet.column("initiator", progress.getInitiator().toString());

            dataSet.column("state", progress.getState().name().toLowerCase());
            dataSet.column("progress_percentage", 100 * progress.getProgress());

            dataSet.column("estimated_partitions", progress.getEstimatedPartitions());
            dataSet.column("partitions_processed", progress.getPartitionsProcessed());
            dataSet.column("estimated_total_bytes", progress.getEstimatedTotalBytes());

            dataSet.column("queue_duration_ms", TimeUnit.NANOSECONDS.toMillis(startTimeNs - creationTimeNs));
            dataSet.column("runtime_duration_ms", TimeUnit.NANOSECONDS.toMillis(lastUpdatedNs - startTimeNs));
            dataSet.column("total_duration_ms", TimeUnit.NANOSECONDS.toMillis(lastUpdatedNs - creationTimeNs));

            if (progress.getFailureCause() != null)
                dataSet.column("failure_cause", progress.getFailureCause());
        }
    }

    private static TableMetadata parse(String keyspace, String comment, String query)
    {
        return CreateTableStatement.parse(query, keyspace)
                                   .comment(comment)
                                   .kind(TableMetadata.Kind.VIRTUAL)
                                   .partitioner(new LocalPartitioner(UUIDType.instance))
                                   .build();
    }
}
