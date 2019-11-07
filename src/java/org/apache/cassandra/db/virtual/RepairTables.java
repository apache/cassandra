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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Throwables;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.JobProgress;
import org.apache.cassandra.repair.RepairDesc;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairProgress;
import org.apache.cassandra.repair.RepairSessionDesc;
import org.apache.cassandra.repair.SessionProgress;
import org.apache.cassandra.repair.ValidationProgress;
import org.apache.cassandra.repair.messages.ValidationStatusRequest;
import org.apache.cassandra.repair.messages.ValidationStatusResponse;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

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
            ActiveRepairService.instance.repairProgress((a, b) -> updateDataset(result, a, b));
            return result;
        }

        public DataSet data(DecoratedKey partitionKey)
        {
            UUID parentSessionId = UUIDType.instance.compose(partitionKey.getKey());
            SimpleDataSet result = new SimpleDataSet(metadata());
            ActiveRepairService.instance.repairProgress(parentSessionId, (a, b) -> updateDataset(result, a, b));
            return result;
        }

        private void updateDataset(SimpleDataSet dataSet, RepairDesc repairDesc, RepairProgress repairProgress)
        {
            UUID parentSessionId = repairDesc.parentSession;
            String keyspace = repairDesc.keyspace;

            Map<UUID, Pair<RepairSessionDesc, SessionProgress>> sessions = new HashMap<>();
            ActiveRepairService.instance.repairSessionProgress(parentSessionId, (a, b) -> {
                sessions.put(a.id, Pair.create(a, b));
            });

            ActiveRepairService.instance.repairJobProgress(parentSessionId, (jobDesc, jobProgress) -> {
                Pair<RepairSessionDesc, SessionProgress> session = sessions.get(jobDesc.sessionId);
                if (session == null)
                {
                    //TODO handle this; means that between this call the session should be there now
                    return;
                }

                // call early to make sure reads are visable
//                long jobLastUpdatedAtNs = jobProgress.getLastUpdatedAtNs();

                InetAddressAndPort coordinator = FBUtilities.getBroadcastAddressAndPort();
                List<String> ranges = jobDesc.ranges.stream().map(Range::toString).collect(Collectors.toList());

                RepairSessionDesc sessionDesc = session.left;
                Stream.concat(Stream.of(coordinator), sessionDesc.commonRange.endpoints.stream()).forEach(participant -> {
                    dataSet.row(parentSessionId, jobDesc.sessionId, jobDesc.columnFamily, participant.toString());

                    // shared columns
                    dataSet.column("keyspace_name", keyspace);
//                dataSet.column("options", options); //TODO this breaks vtables =)
                    dataSet.column("coordinator", coordinator.toString());

                    // job specific columns
                    dataSet.column("ranges", ranges);

                    switch (jobProgress.getState())
                    {
                        case VALIDATION_REQUEST:
                            try
                            {
                                RemoteState remoteState = getValidationState(jobDesc, participant).join();
                                dataSet.column("state", remoteState.state);
                                dataSet.column("progress_percentage", remoteState.progressPercentage);
                                if (remoteState.failureCause != null)
                                    dataSet.column("failure_cause", remoteState.failureCause);
                                break;
                            }
                            catch (Exception e)
                            {
                                // go to default
                                e.printStackTrace();
                            }
                        default:
                            dataSet.column("state", jobProgress.getState().name().toLowerCase());
                            dataSet.column("progress_percentage", jobProgress.getProgress() * 100);

                            if (jobProgress.getFailureCause() != null)
                                dataSet.column("failure_cause", Throwables.getStackTraceAsString(jobProgress.getFailureCause()));
                    }
                });
            });
        }

        private CompletableFuture<RemoteState> getValidationState(RepairJobDesc desc, InetAddressAndPort participant)
        {
            CompletableFuture<Message<ValidationStatusResponse>> future = MessagingService.instance().sendFuture(Message.builder(Verb.VALIDATION_STAT_REQ, new ValidationStatusRequest(desc)).build(), participant);
            return future.thenApply(rsp -> {
                ValidationStatusResponse status = rsp.payload;
                switch (status.state)
                {
                    case UNKNOWN:
                        return new RemoteState(JobProgress.State.VALIDATION_REQUEST.name().toLowerCase(), .4F, null);
                    case SUCCESS:
                        return new RemoteState("validaton_complete_await_rsp", 100, null);
                    case FAILURE:
                        return new RemoteState("failure", 100, status.failureCause);
                    default:
                        return new RemoteState("validating", 40F * status.progress, null);
                }
            });
        }
    }

    private static class RemoteState {
        public final String state;
        public final float progressPercentage;
        public final String failureCause;

        private RemoteState(String state, float progressPercentage, String failureCause)
        {
            this.state = state;
            this.progressPercentage = progressPercentage;
            this.failureCause = failureCause;
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

            dataSet.row(parentSessionId, sessionId, cf);

            dataSet.column("keyspace_name", ks);
            dataSet.column("ranges", ranges.stream().map(Range::toString).collect(Collectors.toList()));

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

    private static TableMetadata parse(String keyspace, String comment, String query)
    {
        return CreateTableStatement.parse(query, keyspace)
                                   .comment(comment)
                                   .kind(TableMetadata.Kind.VIRTUAL)
                                   .partitioner(new LocalPartitioner(UUIDType.instance))
                                   .build();
    }
}
