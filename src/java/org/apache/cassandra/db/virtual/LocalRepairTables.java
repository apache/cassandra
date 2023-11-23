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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.CommonRange;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.state.Completable;
import org.apache.cassandra.repair.state.CoordinatorState;
import org.apache.cassandra.repair.state.JobState;
import org.apache.cassandra.repair.state.ParticipateState;
import org.apache.cassandra.repair.state.SessionState;
import org.apache.cassandra.repair.state.State;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

public class LocalRepairTables
{
    private LocalRepairTables()
    {
    }

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        return Arrays.asList(
        new RepairTable(keyspace),
        new SessionTable(keyspace),
        new JobTable(keyspace),
        new ParticipateTable(keyspace),
        new ValidationTable(keyspace)
        );
    }

    private static final String JOB_DESC_COLUMNS = "  repair_id timeuuid,\n" +
                                                   "  session_id timeuuid,\n" +
                                                   "  keyspace_name text,\n" +
                                                   "  table_name text,\n" +
                                                   "  ranges frozen<list<text>>,\n";

    static final class RepairTable extends AbstractVirtualTable
    {
        protected RepairTable(String keyspace)
        {
            super(parse(keyspace, "Repair summary",
                        "CREATE TABLE repairs (\n" +
                        stdColumnsWithStatus(true) +
                        "  command_id int,\n" +
                        "  keyspace_name text,\n" +
                        // human readable definition of what the repair is doing
                        "  type text,\n" +
                        // list of all sessions; this is lazy so only once the session is created will it be present, so this dynamically changes within the life of a repair
                        "  sessions frozen<set<timeuuid>>,\n" +

                        // options_ maps to RepairOption
                        "  options_parallelism text,\n" +
                        "  options_primary_range boolean,\n" +
                        "  options_incremental boolean,\n" +
                        "  options_trace boolean,\n" +
                        "  options_job_threads int,\n" +
                        "  options_subrange_repair boolean,\n" +
                        "  options_pull_repair boolean,\n" +
                        "  options_force_repair boolean,\n" +
                        "  options_preview_kind text,\n" +
                        "  options_optimise_streams boolean,\n" +
                        "  options_ignore_unreplicated_keyspaces boolean,\n" +
                        "  options_column_families frozen<set<text>>,\n" +
                        "  options_data_centers frozen<set<text>>,\n" +
                        "  options_hosts frozen<set<text>>,\n" +
                        "  options_ranges frozen<set<text>>,\n" +

                        "  table_names frozen<list<text>>,\n" +
                        "  ranges frozen<list<list<text>>>,\n" +
                        "  unfiltered_ranges frozen<list<list<text>>>,\n" +
                        "  participants frozen<list<text>>,\n" +
                        stateColumns(CoordinatorState.State.class) +
                        "\n" +
                        "  PRIMARY KEY ( (id) )\n" +
                        ")"));
        }

        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            ActiveRepairService.instance().coordinators().forEach(s -> updateDataset(result, s));
            return result;
        }

        public DataSet data(DecoratedKey partitionKey)
        {
            TimeUUID id = TimeUUIDType.instance.compose(partitionKey.getKey());
            SimpleDataSet result = new SimpleDataSet(metadata());
            CoordinatorState state = ActiveRepairService.instance().coordinator(id);
            if (state != null)
                updateDataset(result, state);
            return result;
        }

        private void updateDataset(SimpleDataSet result, CoordinatorState state)
        {
            result.row(state.id);
            addState(result, state);
            result.column("type", getType(state));
            result.column("keyspace_name", state.keyspace);
            result.column("command_id", state.cmd);

            result.column("options_parallelism", state.options.getParallelism().name());
            result.column("options_primary_range", state.options.isPrimaryRange());
            result.column("options_trace", state.options.isTraced());
            result.column("options_job_threads", state.options.getJobThreads());
            result.column("options_subrange_repair", state.options.isSubrangeRepair());
            result.column("options_pull_repair", state.options.isPullRepair());
            result.column("options_force_repair", state.options.isForcedRepair());
            result.column("options_preview_kind", state.options.getPreviewKind().name());
            result.column("options_optimise_streams", state.options.optimiseStreams());
            result.column("options_ignore_unreplicated_keyspaces", state.options.ignoreUnreplicatedKeyspaces());
            result.column("options_column_families", state.options.getColumnFamilies());
            result.column("options_data_centers", state.options.getDataCenters());
            result.column("options_hosts", state.options.getHosts());
            result.column("options_ranges", toStringSet(state.options.getRanges()));

            result.column("sessions", state.getSessionIds());

            String[] columnFamilyNames = state.getColumnFamilyNames();
            result.column("table_names", columnFamilyNames == null ? null : Arrays.asList(columnFamilyNames));

            Set<InetAddressAndPort> participants = state.getParticipants();
            result.column("participants", participants == null ? null : toStringList(participants));

            List<CommonRange> ranges = state.getFilteredCommonRanges();
            result.column("ranges", ranges == null ? null : ranges.stream().map(c -> c.ranges).map(LocalRepairTables::toStringList).collect(Collectors.toList()));

            ranges = state.getCommonRanges();
            result.column("unfiltered_ranges", ranges == null ? null : ranges.stream().map(c -> c.ranges).map(LocalRepairTables::toStringList).collect(Collectors.toList()));
        }

        private String getType(CoordinatorState state)
        {
            if (state.options.isPreview())
            {
                switch (state.options.getPreviewKind())
                {
                    case ALL: return "preview full";
                    case REPAIRED: return "preview repaired";
                    case UNREPAIRED: return "preview unrepaired";
                    case NONE: throw new AssertionError("NONE preview kind not expected when preview repair is set");
                    default: throw new AssertionError("Unknown preview kind: " + state.options.getPreviewKind());
                }
            }
            else if (state.options.isIncremental())
            {
                return "incremental";
            }
            return "full";
        }
    }

    private static final class SessionTable extends AbstractVirtualTable
    {
        SessionTable(String keyspace)
        {
            super(parse(keyspace, "Repair session",
                        "CREATE TABLE repair_sessions (\n" +
                        stdColumnsWithStatus(true) +
                        "  repair_id timeuuid,\n" +
                        "  keyspace_name text,\n" +
                        "  table_names frozen<list<text>>,\n" +
                        "  ranges frozen<list<text>>,\n" +
                        "  participants frozen<list<text>>,\n" +
                        "  jobs frozen<set<uuid>>,\n" +
                        "\n" +
                        stateColumns(SessionState.State.class) +
                        "\n" +
                        "  PRIMARY KEY ( (id) )\n" +
                        ")"));
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            ActiveRepairService.instance().coordinators().stream()
                                                .flatMap(s -> s.getSessions().stream())
                                                .forEach(s -> updateDataset(result, s));
            return result;
        }

        private void updateDataset(SimpleDataSet result, SessionState state)
        {
            result.row(state.id);
            addState(result, state);
            result.column("repair_id", state.parentRepairSession);
            result.column("keyspace_name", state.keyspace);
            result.column("table_names", Arrays.asList(state.cfnames));
            result.column("ranges", toStringList(state.commonRange.ranges));
            result.column("jobs", state.getJobIds());
            result.column("participants", toStringList(state.getParticipants()));
        }
    }

    private static final class JobTable extends AbstractVirtualTable
    {
        JobTable(String keyspace)
        {
            super(parse(keyspace, "Repair job",
                        "CREATE TABLE repair_jobs (\n" +
                        stdColumnsWithStatus(false) +
                        "  participants frozen<list<text>>,\n" +
                        JOB_DESC_COLUMNS +
                        "\n" +
                        stateColumns(JobState.State.class) +
                        "\n" +
                        "  PRIMARY KEY ( (id) )\n" +
                        ")"));
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            ActiveRepairService.instance().coordinators().stream()
                                                .flatMap(s -> s.getSessions().stream())
                                                .flatMap(s -> s.getJobs().stream())
                                                .forEach(s -> updateDataset(result, s));
            return result;
        }

        private void updateDataset(SimpleDataSet result, JobState state)
        {
            result.row(state.id);
            addState(result, state);
            addState(result, state.desc);
            result.column("participants", toStringList(state.getParticipants()));
        }
    }

    static final class ParticipateTable extends AbstractVirtualTable
    {
        protected ParticipateTable(String keyspace)
        {
            super(parse(keyspace, "Repair participate summary",
                        "CREATE TABLE repair_participates (" +
                        stdColumns(true) +
                        "  initiator  text,\n" +
                        "  tables frozen<set<text>>, \n" +
                        "  ranges frozen<list<text>>,\n" +
                        "  incremental boolean,\n" +
                        "  global boolean,\n" +
                        "  preview_kind text,\n" +
                        "  repaired_at timestamp,\n" +
                        "  validations frozen<set<uuid>>,\n" +
                        "\n" +
                        "  PRIMARY KEY ( (id) )\n" +
                        ")"));
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            ActiveRepairService.instance().participates().stream()
                                        .forEach(s -> updateDataset(result, s));
            return result;
        }

        @Override
        public DataSet data(DecoratedKey partitionKey)
        {
            TimeUUID id = TimeUUIDType.instance.compose(partitionKey.getKey());
            SimpleDataSet result = new SimpleDataSet(metadata());
            ParticipateState state = ActiveRepairService.instance().participate(id);
            if (state != null)
                updateDataset(result, state);
            return result;
        }

        private void updateDataset(SimpleDataSet result, ParticipateState state)
        {
            result.row(state.id);
            addCompletableState(result, state);
            result.column("initiator", state.initiator.toString());
            result.column("tables", state.tableIds.stream()
                                                  .map(Schema.instance::getTableMetadata)
                                                  .filter(a -> a != null) // getTableMetadata returns null if id isn't know, most likely dropped
                                                  .map(Object::toString)
                                                  .collect(Collectors.toSet()));
            result.column("incremental", state.incremental);
            result.column("global", state.global);
            result.column("preview_kind", state.previewKind.name());
            if (state.repairedAt != 0)
                result.column("repaired_at", new Date(state.repairedAt));
            result.column("validations", ImmutableSet.copyOf(state.validationIds()));
            result.column("ranges", toStringList(state.ranges));
        }
    }

    private static final class ValidationTable extends AbstractVirtualTable
    {
        ValidationTable(String keyspace)
        {
            super(parse(keyspace, "Repair validation",
                        "CREATE TABLE repair_validations (\n" +
                        stdColumnsWithStatus(false) +
                        JOB_DESC_COLUMNS +
                        "  initiator  text,\n" +
                        "  estimated_partitions  bigint,\n" +
                        "  estimated_total_bytes  bigint,\n" +
                        "  partitions_processed  bigint,\n" +
                        "  bytes_read  bigint,\n" +
                        "  progress_percentage float,\n" +
                        "\n" +
                        stateColumns(ValidationState.State.class) +
                        "\n" +
                        "  PRIMARY KEY ( (id) )\n" +
                        ")"));
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            ActiveRepairService.instance().validations().stream()
                                                     .forEach(s -> updateDataset(result, s));
            return result;
        }

        @Override
        public DataSet data(DecoratedKey partitionKey)
        {
            UUID id = UUIDType.instance.compose(partitionKey.getKey());
            SimpleDataSet result = new SimpleDataSet(metadata());
            ValidationState state = ActiveRepairService.instance().validation(id);
            if (state != null)
                updateDataset(result, state);
            return result;
        }

        private void updateDataset(SimpleDataSet result, ValidationState state)
        {
            result.row(state.id);
            addState(result, state);
            addState(result, state.desc);
            result.column("initiator", state.initiator.toString());
            result.column("estimated_partitions", state.estimatedPartitions == 0 ? null : state.estimatedPartitions);
            result.column("estimated_total_bytes", state.estimatedTotalBytes == 0 ? null : state.estimatedTotalBytes);
            result.column("partitions_processed", state.partitionsProcessed == 0 ? null : state.partitionsProcessed);
            result.column("progress_percentage", round(state.getProgress() * 100));
            result.column("bytes_read", state.bytesRead);
        }
    }

    private static String timestampColumnName(Enum<?> e)
    {
        return timestampColumnName(e.name().toLowerCase());
    }

    private static String timestampColumnName(String e)
    {
        return "state_" + e + "_timestamp";
    }

    private static String stateColumns(Class<? extends Enum<?>> klass)
    {
        StringBuilder sb = new StringBuilder();
        for (Enum<?> e : klass.getEnumConstants())
            sb.append("  ").append(timestampColumnName(e)).append(" timestamp, \n");
        return sb.toString();
    }

    private static String stdStateColumns()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("  ").append(timestampColumnName("init")).append(" timestamp, \n");
        for (State.Result.Kind kind : State.Result.Kind.values())
            sb.append("  ").append(timestampColumnName(kind)).append(" timestamp, \n");
        return sb.toString();
    }

    private static void addCompletableState(SimpleDataSet ds, Completable<?> state)
    {
        // read timestamp early to see latest data
        ds.column("last_updated_at", new Date(state.getLastUpdatedAtMillis()));
        ds.column("duration_millis", state.getDurationMillis());
        State.Result result = state.getResult();
        ds.column("failure_cause", state.getFailureCause());
        ds.column("success_message", state.getSuccessMessage());
        ds.column(timestampColumnName("init"), new Date(state.getInitializedAtMillis()));
        ds.column("completed", result != null);

        if (result != null)
            ds.column(timestampColumnName(result.kind), new Date(state.getLastUpdatedAtMillis()));
    }

    private static <T extends Enum<T>> void addState(SimpleDataSet ds, State<T, ?> state)
    {
        addCompletableState(ds, state);

        T currentState = state.getStatus();
        State.Result result = state.getResult();
        ds.column("status", result != null ? result.kind.name().toLowerCase() : currentState == null ? "init" : currentState.name().toLowerCase());
        for (Map.Entry<T, Long> e : state.getStateTimesMillis().entrySet())
        {
            if (e.getValue().longValue() != 0)
                ds.column(timestampColumnName(e.getKey()), new Date(e.getValue()));
        }
    }

    @VisibleForTesting
    static float round(float value)
    {
        return Math.round(value * 100.0F) / 100.0F;
    }

    private static void addState(SimpleDataSet result, RepairJobDesc desc)
    {
        result.column("repair_id", desc.parentSessionId);
        result.column("session_id", desc.sessionId);
        result.column("keyspace_name", desc.keyspace);
        result.column("table_name", desc.columnFamily);
        result.column("ranges", toStringList(desc.ranges));
    }

    private static <T> List<String> toStringList(Collection<T> list)
    {
        if (list == null)
            return null;
        return list.stream().map(Object::toString).collect(Collectors.toList());
    }

    private static <T> Set<String> toStringSet(Collection<T> list)
    {
        if (list == null)
            return null;
        return list.stream().map(Object::toString).collect(Collectors.toSet());
    }

    private static TableMetadata parse(String keyspace, String comment, String query)
    {
        return CreateTableStatement.parse(query, keyspace)
                                   .comment(comment)
                                   .kind(TableMetadata.Kind.VIRTUAL)
                                   .partitioner(new LocalPartitioner(UUIDType.instance))
                                   .build();
    }

    private static String stdColumns(boolean time)
    {
        String str = "  id timeuuid,\n" +
                     "  last_updated_at timestamp,\n" +
                     "  completed boolean,\n" +
                     "  duration_millis bigint,\n" +
                     "  failure_cause text,\n" +
                     "  success_message text,\n";
        if (!time)
            str = str.replace("id timeuuid", "id uuid");
        return str + stdStateColumns();
    }

    private static String stdColumnsWithStatus(boolean time)
    {
        return stdColumns(time) + "  status text,\n";
    }
}
