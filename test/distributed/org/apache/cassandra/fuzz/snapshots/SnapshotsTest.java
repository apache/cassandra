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

package org.apache.cassandra.fuzz.snapshots;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.management.openmbean.TabularData;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import accord.utils.Property.StateOnlyCommand;
import accord.utils.RandomSource;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableFunction;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableTriConsumer;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.fuzz.snapshots.SnapshotsTest.State.TestSnapshot;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.LocalizeString;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.JavaRandom;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.apache.cassandra.fuzz.snapshots.SnapshotsTest.CreateKeyspace.createKeypace;
import static org.apache.cassandra.fuzz.snapshots.SnapshotsTest.CreateTable.createTable;
import static org.junit.Assert.assertEquals;

public class SnapshotsTest
{
    @Test
    public void fuzzySnapshotsTest()
    {
        stateful()
        .withExamples(1)
        .withSteps(500)
        .withStepTimeout(Duration.ofMinutes(1))
        .check(commands(() -> State::new)
               .add(CreateKeyspace::new)
               .add(DropKeyspace::new)
               .add(CreateTable::new)
               .add(DropTable::new)
               .add(TruncateTable::new)
               .add(TakeSnapshot::new)
               .add(ClearSnapshot::new)
               .add(ListSnapshots::new)
               .destroyState(State::destroy)
               .build());
    }

    public static class ListSnapshots extends AbstractCommand
    {
        private final Pair<SnapshotsHolder, Map<String, String>> listingParams;

        public ListSnapshots(RandomSource rs, State state)
        {
            super(rs, state);
            listingParams = generateParams();
        }

        @Override
        public void doWork(State state)
        {
            assertEquals(listingParams.left, categorize(list(listingParams.right)));
        }

        @Override
        public String toString()
        {
            return "List snapshots with parameters: " + listingParams;
        }

        private Pair<SnapshotsHolder, Map<String, String>> generateParams()
        {
            Map<String, String> listingParams = new HashMap<>();
            List<Integer> toShuffle = state.getShuffledListOfInts(4);

            String keyspace = null;
            for (int i = 0; i < 4; i++)
            {
                boolean picked = false;
                switch (toShuffle.get(i))
                {
                    case 1:
                        if (!state.truncatedSnapshots.isEmpty())
                        {
                            keyspace = state.rs.pickUnorderedSet(state.truncatedSnapshots.keySet()).split("\\.")[0];
                            picked = true;
                        }
                        break;
                    case 2:
                        if (!state.droppedSnapshots.isEmpty())
                        {
                            keyspace = state.rs.pickUnorderedSet(state.droppedSnapshots).split("\\.")[0];
                            picked = true;
                        }
                        break;
                    case 3:
                        if (!state.snapshots.isEmpty())
                        {
                            keyspace = state.rs.pickUnorderedSet(state.snapshots).getKeyspaceName();
                            picked = true;
                        }
                        break;
                    default:
                        // keyspace will be null so all snapshost will be listed
                        picked = true;
                        break;
                }
                if (picked)
                    break;
            }

            // we need to populate expected snapshots after listing
            SnapshotsHolder holder = new SnapshotsHolder();

            if (keyspace == null)
            {
                holder.normal.addAll(state.snapshots);
                holder.dropped.addAll(state.droppedSnapshots);
                holder.truncated.putAll(state.truncatedSnapshots);
            }
            else
            {
                listingParams.put("keyspace", keyspace);
                for (String s : state.droppedSnapshots)
                    if (s.startsWith(keyspace + '.'))
                        holder.dropped.add(s);

                for (Map.Entry<String, Integer> entry : state.truncatedSnapshots.entrySet())
                    if (entry.getKey().startsWith(keyspace + '.'))
                        holder.truncated.put(entry.getKey(), entry.getValue());

                for (TestSnapshot s : state.snapshots)
                    if (s.getKeyspaceName().equals(keyspace))
                        holder.normal.add(s);
            }

            return Pair.create(holder, listingParams);
        }

        private List<String> list(Map<String, String> parameters)
        {
            return getNode().applyOnInstance((SerializableFunction<Map<String, String>, List<String>>) (params) ->
            {
                Map<String, TabularData> listingResult = SnapshotManager.instance.listSnapshots(params);

                List<String> snapshots = new ArrayList<>();
                for (final Map.Entry<String, TabularData> snapshotDetail : listingResult.entrySet())
                {
                    Set<?> values = snapshotDetail.getValue().keySet();
                    for (Object eachValue : values)
                    {
                        final List<?> value = (List<?>) eachValue;
                        String tag = (String) value.get(0);
                        String keyspace = (String) value.get(1);
                        String table = (String) value.get(2);
                        snapshots.add(format("%s.%s.%s", keyspace, table, tag));
                    }
                }

                return snapshots;
            }, parameters);
        }

        private SnapshotsHolder categorize(List<String> listedSnapshots)
        {
            SnapshotsHolder holder = new SnapshotsHolder();
            for (String snapshot : listedSnapshots)
            {
                if (snapshot.contains("dropped-"))
                {
                    String[] split = snapshot.split("\\.");
                    holder.dropped.add(format("%s.%s", split[0], split[1]));
                }
                else if (snapshot.contains("truncated-"))
                {
                    String[] split = snapshot.split("\\.");
                    String ksTb = format("%s.%s", split[0], split[1]);
                    holder.truncated.merge(ksTb, 1, Integer::sum);
                }
                else
                {
                    String[] split = snapshot.split("\\.");
                    holder.normal.add(new TestSnapshot(split[0], split[1], split[2]));
                }
            }

            return holder;
        }

        private static class SnapshotsHolder
        {
            Set<TestSnapshot> normal = new HashSet<>();
            Set<String> dropped = new HashSet<>();
            Map<String, Integer> truncated = new HashMap<>();

            @Override
            public String toString()
            {
                return "SnapshotsHolder{" +
                       "normal=" + normal +
                       ", dropped=" + dropped +
                       ", truncated=" + truncated +
                       '}';
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                SnapshotsHolder holder = (SnapshotsHolder) o;
                return Objects.equals(normal, holder.normal) &&
                       Objects.equals(dropped, holder.dropped) &&
                       Objects.equals(truncated, holder.truncated);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(normal, dropped, truncated);
            }
        }
    }

    public static class CreateKeyspace extends AbstractCommand
    {
        private static final String CREATE_KEYSPACE_QUERY =
        "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }";

        private final Set<String> keyspaceToCreate = new HashSet<>();

        public CreateKeyspace(RandomSource rs, State state)
        {
            super(rs, state);
            keyspaceToCreate.add(state.addRandomKeyspace());
        }

        @Override
        public void doWork(State state)
        {
            createKeypace(state);
            assertEquals(state.schema.keySet(), state.getKeyspaces());
            state.populate();
        }

        public static void createKeypace(State state)
        {
            Set<String> existingKeyspaces = state.getKeyspaces();
            Set<String> keyspacesToBe = state.schema.keySet();

            Set<String> difference = difference(keyspacesToBe, existingKeyspaces);
            if (!difference.isEmpty())
                state.getNode().executeInternal(format(CREATE_KEYSPACE_QUERY, difference.iterator().next()));
        }

        @Override
        public String toString()
        {
            return "Create keyspace " + keyspaceToCreate;
        }
    }

    public static class DropKeyspace extends AbstractCommand
    {
        private static final String DROP_KEYSPACE_QUERY = "DROP KEYSPACE %s";

        private String keyspaceToDrop;

        private final boolean shouldApply;

        public DropKeyspace(RandomSource rs, State state)
        {
            super(rs, state);

            Optional<Pair<String, List<String>>> maybeDroppedKeyspaceWithTables = state.removeRandomKeyspace();
            shouldApply = maybeDroppedKeyspaceWithTables.isPresent();

            if (!shouldApply)
                return;

            // dropping of a keyspace will create dropped tables which create "dropped" snapshots
            String keyspace = maybeDroppedKeyspaceWithTables.get().left;
            List<String> tables = maybeDroppedKeyspaceWithTables.get().right;
            for (String table : tables)
                state.addDroppedSnapshot(keyspace, table);

            Set<String> existingKeyspaces = state.getKeyspaces();
            Set<String> keyspacesToBe = state.schema.keySet();
            Set<String> diff = difference(existingKeyspaces, keyspacesToBe);
            assert diff.size() == 1;
            keyspaceToDrop = diff.iterator().next();
        }

        @Override
        public void doWork(State state)
        {
            if (!shouldApply)
                return;

            executeQuery(format(DROP_KEYSPACE_QUERY, keyspaceToDrop));
            assertEquals(state.schema.keySet(), state.getKeyspaces());
            state.populate();
        }

        @Override
        public String toString()
        {
            if (keyspaceToDrop != null)
                return "Drop keyspace " + keyspaceToDrop;
            else
                return "Drop keyspace skipping";
        }
    }

    public static class CreateTable extends AbstractCommand
    {
        private static final String CREATE_TABLE_QUERY = "CREATE TABLE %s.%s (id uuid primary key, val uuid)";
        private static final String INSERT_QUERY = "INSERT INTO %s.%s (id, val) VALUES (%s, %s)";

        private final Pair<String, String> table;

        public CreateTable(RandomSource rs, State state)
        {
            super(rs, state);
            table = state.addRandomTable();
        }

        @Override
        public void doWork(State state)
        {
            Map<String, List<String>> expected = state.schema;
            createTable(state);
            assertEquals(expected, state.getSchema());
            state.populate();
        }

        public static Map<String, List<String>> createTable(State state)
        {
            Map<String, List<String>> existingSchema = state.getSchema();
            Map<String, List<String>> schemaToBe = state.schema;

            MapDifference<String, List<String>> difference = Maps.difference(schemaToBe, existingSchema);

            Map<String, List<String>> tablesToCreate = new HashMap<>();

            for (Entry<String, ValueDifference<List<String>>> entry : difference.entriesDiffering().entrySet())
            {
                String keyspaceName = entry.getKey();
                tablesToCreate.put(keyspaceName, new ArrayList<>());
                ValueDifference<List<String>> tableNames = entry.getValue();
                for (String table : difference(new HashSet<>(tableNames.leftValue()),
                                               new HashSet<>(tableNames.rightValue())))
                {
                    tablesToCreate.get(keyspaceName).add(table);
                }
            }

            for (Map.Entry<String, List<String>> entry : tablesToCreate.entrySet())
            {
                String keyspace = entry.getKey();
                for (String table : entry.getValue())
                {
                    state.executeQuery(format(CREATE_TABLE_QUERY, keyspace, table));
                    populateTable(state, keyspace, table);
                }
            }

            return tablesToCreate;
        }

        public static void populateTable(State state, String keypace, String table)
        {
            // create between 1 and 10 sstables per table
            for (int i = 0; i < state.rs.nextInt(1, 10); i++)
            {
                state.executeQuery(format(INSERT_QUERY, keypace, table, randomUUID(), randomUUID()));
                state.nodetool("flush", keypace, table);
            }
        }

        @Override
        public String toString()
        {
            return "Create table " + format("%s.%s", table.left, table.right);
        }
    }

    public static class CreateMoreData extends AbstractCommand
    {
        private Pair<String, String> tableToPopulate;
        private final boolean shouldApply;

        public CreateMoreData(RandomSource rs, State state)
        {
            super(rs, state);

            Optional<Pair<String, String>> randomTable = state.pickRandomTable(false);
            shouldApply = randomTable.isPresent();

            if (!shouldApply)
                return;

            tableToPopulate = randomTable.get();
        }

        @Override
        public void doWork(State state)
        {
            if (!shouldApply)
                return;

            CreateTable.populateTable(state, tableToPopulate.left, tableToPopulate.right);
        }

        @Override
        public String toString()
        {
            if (tableToPopulate != null)
                return "Create more data " + format("%s.%s", tableToPopulate.left, tableToPopulate.right);
            else
                return "Create more data skipped";
        }
    }

    public static class DropTable extends AbstractCommand
    {
        private static final String DROP_TABLE_QUERY = "DROP TABLE %s.%s";

        private Pair<String, String> tableToDrop;
        private final boolean shouldApply;

        public DropTable(RandomSource rs, State state)
        {
            super(rs, state);
            Optional<Pair<String, String>> randomTable = state.pickRandomTable(false);
            shouldApply = randomTable.isPresent();

            if (!shouldApply)
                return;

            tableToDrop = randomTable.get();
            state.removeTable(tableToDrop.left, tableToDrop.right);

            // if we drop a table, it will make a snapshot with "dropped-" prefix
            // hence it will be among snapshot as well, however we do not know its name
            // in advance because it contains timestamp in its name produced by Cassandra
            // hence we can not add it among "normal" snapshots, hence special "droppedSnapshots" set.
            state.addDroppedSnapshot(tableToDrop.left, tableToDrop.right);
        }

        @Override
        public void doWork(State state)
        {
            if (!shouldApply)
                return;

            Map<String, List<String>> existingSchema = state.getSchema();
            Map<String, List<String>> schemaToBe = state.schema;

            MapDifference<String, List<String>> difference = Maps.difference(schemaToBe, existingSchema);

            for (Entry<String, ValueDifference<List<String>>> entry : difference.entriesDiffering().entrySet())
            {
                String keyspaceName = entry.getKey();
                ValueDifference<List<String>> tableNames = entry.getValue();

                Set<String> left = new HashSet<>(tableNames.leftValue());
                Set<String> right = new HashSet<>(tableNames.rightValue());

                for (String table : difference(right, left))
                    executeQuery(format(DROP_TABLE_QUERY, keyspaceName, table));
            }

            assertEquals(schemaToBe, state.getSchema());

            state.populate();
        }

        @Override
        public String toString()
        {
            if (tableToDrop != null)
                return "Drop table " + format("%s.%s", tableToDrop.left, tableToDrop.right);
            else
                return "Drop table skipped";
        }
    }

    public static class TruncateTable extends AbstractCommand
    {
        private final Pair<String, String> toTruncate;

        public TruncateTable(RandomSource rs, State state)
        {
            super(rs, state);
            toTruncate = state.pickRandomTable(true).get(); // there is always in-built table to truncate
            state.addTruncatedSnapshot(toTruncate.left, toTruncate.right);
        }

        @Override
        public void doWork(State state)
        {
            // create missing tables if any
            createKeypace(state);
            createTable(state);

            executeQuery(format("TRUNCATE TABLE %s.%s", toTruncate.left, toTruncate.right));
            assertEquals(state.truncatedSnapshots, state.getSnapshotsOfTruncatedTables());

            state.populate();
        }

        @Override
        public String toString()
        {
            return "Truncate table " + format("%s.%s", toTruncate.left, toTruncate.right);
        }
    }

    public static class TakeSnapshot extends AbstractCommand
    {
        private final String snapshotToTake;

        public TakeSnapshot(RandomSource rs, State state)
        {
            super(rs, state);
            // there is always in-built table to take snapshot on
            Pair<String, String> randomTable = state.pickRandomTable(true).get();
            String tag = state.addSnapshot(randomTable.left, randomTable.right);
            snapshotToTake = format("%s.%s.%s", randomTable.left, randomTable.right, tag);
        }

        @Override
        public void doWork(State state)
        {
            // see what snapshots are to be taken
            Set<TestSnapshot> existing = state.getSnapshots();
            Set<TestSnapshot> toBe = state.snapshots;

            Set<TestSnapshot> diff = difference(toBe, existing);

            assert diff.size() == 1 : "expecting one snapshot to take!";

            state.takeSnapshots(diff);
            assertEquals(toBe, state.getSnapshots());
            state.populate();
        }

        @Override
        public String toString()
        {
            return "Snapshot " + snapshotToTake;
        }
    }

    public static class ClearSnapshot extends AbstractCommand
    {
        private static final String DROPPED_SNAPSHOT_PREFIX = "dropped-";
        private static final String TRUNCATED_SNAPSHOT_PREFIX = "truncated-";

        private final Set<TestSnapshot> normalDiff = new HashSet<>();
        private final Set<String> truncatedDiff = new HashSet<>();
        private final Set<String> droppedDiff = new HashSet<>();

        public ClearSnapshot(RandomSource rs, State state)
        {
            super(rs, state);
            prepare();
        }

        private void prepare()
        {
            List<Integer> toShuffle = state.getShuffledListOfInts(3);

            for (int i = 0; i < 3; i++)
            {
                boolean picked = false;
                switch (toShuffle.get(i))
                {
                    case 1:
                        if (!state.truncatedSnapshots.isEmpty())
                        {
                            String randomKsTb = state.rs.pickUnorderedSet(state.truncatedSnapshots.keySet());
                            Integer numberOfTruncatedSnapshots = state.truncatedSnapshots.get(randomKsTb);
                            if (numberOfTruncatedSnapshots == 1)
                                state.truncatedSnapshots.remove(randomKsTb);
                            else
                            {
                                int newNumberOfTruncatedTables = state.truncatedSnapshots.get(randomKsTb) - 1;
                                state.truncatedSnapshots.put(randomKsTb, newNumberOfTruncatedTables);
                            }
                            picked = true;
                        }
                        break;
                    case 2:
                        if (!state.droppedSnapshots.isEmpty())
                        {
                            state.droppedSnapshots.remove(state.rs.pickUnorderedSet(state.droppedSnapshots));
                            picked = true;
                        }
                        break;
                    case 3:
                        if (!state.snapshots.isEmpty())
                        {
                            TestSnapshot pickedSnapshot = state.rs.pickUnorderedSet(state.snapshots);
                            state.snapshots.remove(pickedSnapshot);
                            picked = true;
                        }
                        break;
                }
                if (picked)
                    break;
            }

            Set<TestSnapshot> existingNormal = state.getSnapshots();
            Set<String> existingDropped = state.getSnapshotsOfDroppedTables();
            Map<String, Integer> existingTruncated = state.getSnapshotsOfTruncatedTables();

            Set<TestSnapshot> normalToBe = state.snapshots;
            Set<String> droppedToBe = state.droppedSnapshots;
            Map<String, Integer> truncatedToBe = state.truncatedSnapshots;

            normalDiff.addAll(difference(existingNormal, normalToBe));
            truncatedDiff.addAll(difference(existingTruncated.keySet(), truncatedToBe.keySet()));

            Set<String> diff = new HashSet<>();

            for (Map.Entry<String, Integer> existingTruncatedEntry : existingTruncated.entrySet())
            {
                if (truncatedToBe.containsKey(existingTruncatedEntry.getKey()))
                {
                    int toBeCount = truncatedToBe.get(existingTruncatedEntry.getKey());
                    int existingCount = existingTruncatedEntry.getValue();
                    if (toBeCount < existingCount)
                    {
                        diff.add(existingTruncatedEntry.getKey());
                    }
                }
            }

            truncatedDiff.addAll(diff);

            droppedDiff.addAll(difference(existingDropped, droppedToBe));
        }

        @Override
        public void doWork(State state)
        {
            clearSnapshot(state, normalDiff);
            clearSnapshot(state, truncatedDiff, TRUNCATED_SNAPSHOT_PREFIX);
            clearSnapshot(state, droppedDiff, DROPPED_SNAPSHOT_PREFIX);

            assertEquals(state.snapshots, state.getSnapshots());
            assertEquals(state.droppedSnapshots, state.getSnapshotsOfDroppedTables());

            assertEquals(state.truncatedSnapshots, state.getSnapshotsOfTruncatedTables());

            state.populate();
        }

        @Override
        public String toString()
        {
            if (!normalDiff.isEmpty())
                return "Clear snapshot(s) " + normalDiff;
            else if (!truncatedDiff.isEmpty())
                return "Clear snapshot(s) " + truncatedDiff;
            else if (!droppedDiff.isEmpty())
                return "Clear snapshots(s) " + droppedDiff;
            else
                return "Clear snapshots";
        }

        private void clearSnapshot(State state, Set<String> toRemove, String prefix)
        {
            for (String snapshot : toRemove)
            {
                String[] split = snapshot.split("\\.");
                state.getNode().acceptsOnInstance((SerializableTriConsumer<String, String, String>) (ks, tb, pref) ->
                {
                    List<TableSnapshot> selectedSnapshots = SnapshotManager.instance.getSnapshots(s -> s.getKeyspaceName().equals(ks) &&
                                                                                                       s.getTableName().equals(tb) &&
                                                                                                       s.getTag().contains(pref));
                    if (!selectedSnapshots.isEmpty())
                    {
                        TableSnapshot selectedSnapshotForRemoval = selectedSnapshots.get(0);
                        SnapshotManager.instance.clearSnapshot(s -> s.equals(selectedSnapshotForRemoval));
                    }
                }).accept(split[0], split[1], prefix);
            }
        }

        private void clearSnapshot(State state, Set<TestSnapshot> snapshotsToClear)
        {
            for (TestSnapshot snapshot : snapshotsToClear)
            {
                state.getNode()
                     .acceptsOnInstance((SerializableTriConsumer<String, String, String>)
                                        (ks, tb, tag) -> SnapshotManager.instance.clearSnapshot(ks, tb, tag))
                     .accept(snapshot.getKeyspaceName(), snapshot.getTableName(), snapshot.getTag());
            }
        }
    }

    public static class State
    {
        private final Cluster.Builder builder = Cluster.build(1).withConfig(c -> c.with(Feature.NATIVE_PROTOCOL));
        private Cluster cluster;

        public Map<String, List<String>> schema = new HashMap<>();
        public Set<TestSnapshot> snapshots = new HashSet<>();
        public Set<String> droppedSnapshots = new HashSet<>();
        public Map<String, Integer> truncatedSnapshots = new HashMap<>();

        public final RandomSource rs;
        public final RandomnessSource randomnessSource;

        public String inBuiltKeyspace;
        public String inBuiltTable;

        public State(RandomSource rs)
        {
            this.rs = rs;
            this.randomnessSource = new JavaRandom(rs.asJdkRandom());
            start();
            populateSchema();
            populate();
        }

        public State populate()
        {
            this.schema = getSchema();
            this.snapshots = getSnapshots();
            this.droppedSnapshots = getSnapshotsOfDroppedTables();
            this.truncatedSnapshots = getSnapshotsOfTruncatedTables();
            return this;
        }

        private void populateSchema()
        {
            new CreateKeyspace(rs, this).applyUnit(this);
            new CreateTable(rs, this).applyUnit(this);

            this.inBuiltKeyspace = this.schema.keySet().iterator().next();
            this.inBuiltTable = this.schema.values().iterator().next().get(0);
        }

        @Override
        public String toString()
        {
            return "State{" +
                   "schema=" + schema +
                   ", snapshots=" + snapshots +
                   ", droppedSnapshots=" + droppedSnapshots +
                   ", truncatedSnapshots=" + truncatedSnapshots +
                   '}';
        }

        public Optional<String> pickRandomKeyspace(boolean inBuiltIncluded)
        {
            if (inBuiltIncluded)
                return pickRandomKeyspace();

            Set<String> withoutInBuilt = new HashSet<>(schema.keySet());
            withoutInBuilt.remove(inBuiltKeyspace);

            if (withoutInBuilt.isEmpty())
                return Optional.empty();
            else
                return Optional.of(rs.pickUnorderedSet(withoutInBuilt));
        }

        public Optional<String> pickRandomKeyspace()
        {
            if (schema.keySet().isEmpty())
                return Optional.empty();
            else
                return Optional.of(rs.pickUnorderedSet(schema.keySet()));
        }

        public Optional<Pair<String, String>> pickRandomTable(boolean inBuiltIncluded)
        {
            Map<String, List<String>> keyspacesWithTables = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : schema.entrySet())
            {
                if (!entry.getValue().isEmpty())
                {
                    if (inBuiltIncluded)
                    {
                        keyspacesWithTables.put(entry.getKey(), entry.getValue());
                    }
                    else
                    {
                        if (entry.getKey().equals(inBuiltKeyspace))
                        {
                            List<String> tablesInInBuiltKeyspace = new ArrayList<>();
                            for (String tableInInBuiltKeyspace : entry.getValue())
                            {
                                if (!tableInInBuiltKeyspace.equals(inBuiltTable))
                                    tablesInInBuiltKeyspace.add(tableInInBuiltKeyspace);
                            }
                            if (!tablesInInBuiltKeyspace.isEmpty())
                                keyspacesWithTables.put(entry.getKey(), tablesInInBuiltKeyspace);
                        }
                        else
                        {
                            if (!entry.getValue().isEmpty())
                                keyspacesWithTables.put(entry.getKey(), entry.getValue());
                        }
                    }
                }
            }

            if (keyspacesWithTables.isEmpty())
                return Optional.empty();

            String randomKeyspaceWithTables = rs.pickUnorderedSet(keyspacesWithTables.keySet());
            List<String> tables = keyspacesWithTables.get(randomKeyspaceWithTables);
            String randomTable = rs.pick(tables);

            return Optional.of(Pair.create(randomKeyspaceWithTables, randomTable));
        }

        public String addRandomKeyspace()
        {
            String keyspace = createRandomKeyspaceIdentifier();
            addKeyspace(keyspace);
            return keyspace;
        }

        public Pair<String, String> addRandomTable()
        {
            Optional<String> randomKeyspace = pickRandomKeyspace();

            assert randomKeyspace.isPresent();

            String randomTableName = createRandomTableIdentifier();
            addTable(randomKeyspace.get(), randomTableName);
            return Pair.create(randomKeyspace.get(), randomTableName);
        }

        public Optional<Pair<String, List<String>>> removeRandomKeyspace()
        {
            Optional<String> randomKeyspace = pickRandomKeyspace(false);

            if (randomKeyspace.isEmpty())
                return Optional.empty();

            return randomKeyspace.map(this::removeKeyspace);
        }

        public void addKeyspace(String keyspaceName)
        {
            schema.put(keyspaceName, new ArrayList<>());
        }

        public Pair<String, List<String>> removeKeyspace(String keyspaceName)
        {
            List<String> removedTables = schema.remove(keyspaceName);
            return Pair.create(keyspaceName, removedTables);
        }

        public void addTable(String keyspaceName, String tableName)
        {
            List<String> tables = schema.get(keyspaceName);
            if (tables == null)
                tables = new ArrayList<>();

            tables.add(tableName);
            schema.put(keyspaceName, tables);
        }

        public void removeTable(String keyspace, String table)
        {
            List<String> tables = schema.get(keyspace);
            if (tables == null)
                return;

            tables.remove(table);
        }

        public String addSnapshot(String keyspace, String table)
        {
            String randomSnapshotIdentifier = createRandomSnapshotIdentifier();
            addSnapshot(new TestSnapshot(keyspace, table, randomSnapshotIdentifier));

            return randomSnapshotIdentifier;
        }

        public void addDroppedSnapshot(String keyspace, String table)
        {
            droppedSnapshots.add(keyspace + '.' + table);
        }

        public void addTruncatedSnapshot(String keyspace, String table)
        {
            truncatedSnapshots.merge(keyspace + '.' + table, 1, Integer::sum);
        }

        public void addSnapshot(TableSnapshot snapshot)
        {
            snapshots.add(new TestSnapshot(snapshot.getKeyspaceName(), snapshot.getTableName(), snapshot.getTag()));
        }

        public void destroy()
        {
            snapshots.clear();
            droppedSnapshots.clear();
            truncatedSnapshots.clear();
            schema.clear();

            stop();
        }

        public List<Integer> getShuffledListOfInts(int number)
        {
            List<Integer> toShuffle = new ArrayList<>();

            for (int i = 0; i < number; i++)
                toShuffle.add(i);

            Collections.shuffle(toShuffle, rs.asJdkRandom());

            return toShuffle;
        }

        // we just need something to store in the state and whole thing is too much and not necessary
        public static class TestSnapshot extends TableSnapshot implements Comparable<TestSnapshot>
        {
            public TestSnapshot(String keyspaceName, String tableName, String tag)
            {
                this(keyspaceName, tableName, randomUUID(), tag, null, null, null, false);
            }

            public TestSnapshot(String keyspaceName, String tableName, UUID tableId, String tag,
                                Instant createdAt, Instant expiresAt, Set<File> snapshotDirs, boolean ephemeral)
            {
                super(keyspaceName, tableName, tableId, tag, createdAt, expiresAt, snapshotDirs, ephemeral);
            }

            @Override
            public long getManifestsSize()
            {
                return 0;
            }

            @Override
            public long getSchemasSize()
            {
                return 0;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                TableSnapshot snapshot = (TableSnapshot) o;
                return Objects.equals(getKeyspaceName(), snapshot.getKeyspaceName()) &&
                       Objects.equals(getTableName(), snapshot.getTableName()) &&
                       Objects.equals(getTag(), snapshot.getTag());
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(getKeyspaceName(), getTableName(), getTag());
            }

            @Override
            public String toString()
            {
                return format("%s.%s.%s", getKeyspaceName(), getTableName(), getTag());
            }

            @Override
            public int compareTo(TestSnapshot o)
            {
                return toString().compareTo(o.toString());
            }
        }


        private String createRandomKeyspaceIdentifier()
        {
            return trimIfNecessary("keyspace_" + getRandomString());
        }

        private String createRandomSnapshotIdentifier()
        {
            return trimIfNecessary("snapshot_" + getRandomString());
        }

        private String createRandomTableIdentifier()
        {
            return trimIfNecessary("table_" + getRandomString());
        }

        private String getRandomString()
        {
            return LocalizeString.toLowerCaseLocalized(Generators.regexWord(SourceDSL.integers().between(10, 50)).generate(randomnessSource));
        }

        private String trimIfNecessary(String maybeTooLongString)
        {
            if (maybeTooLongString.length() <= 48)
                return maybeTooLongString;

            return maybeTooLongString.substring(0, 48);
        }

        // taken from SUT

        public Map<String, List<String>> getSchema()
        {
            IIsolatedExecutor.SerializableCallable<Map<String, List<String>>> callable = () -> {
                Keyspaces keyspaces = Schema.instance.distributedKeyspaces();

                Map<String, List<String>> keyspacesWithTables = new HashMap<>();

                for (KeyspaceMetadata ksm : keyspaces)
                {
                    if (ksm.name.startsWith("system_"))
                        continue;

                    List<String> tables = new ArrayList<>();
                    for (TableMetadata tmd : ksm.tables)
                        tables.add(tmd.name);

                    keyspacesWithTables.put(ksm.name, tables);
                }

                return keyspacesWithTables;
            };

            return getNode().callOnInstance(callable);
        }

        public Set<String> getKeyspaces()
        {
            return getSchema().keySet();
        }

        public Set<String> getSnapshotsOfDroppedTables()
        {
            SerializableFunction<String, Set<String>> callable = (p) -> {
                Set<String> snapshots = new HashSet<>();
                for (TableSnapshot snapshot : SnapshotManager.instance.getSnapshots(snapshot -> snapshot.getTag().contains(p)))
                    snapshots.add(format("%s.%s", snapshot.getKeyspaceName(), snapshot.getTableName()));

                return snapshots;
            };

            return getNode().applyOnInstance(callable, "dropped-");
        }

        public Map<String, Integer> getSnapshotsOfTruncatedTables()
        {
            SerializableFunction<String, Map<String, Integer>> callable = (p) -> {
                Map<String, Integer> snapshots = new HashMap<>();
                for (TableSnapshot snapshot : SnapshotManager.instance.getSnapshots(snapshot -> snapshot.getTag().contains(p)))
                {
                    String ksTb = format("%s.%s", snapshot.getKeyspaceName(), snapshot.getTableName());
                    snapshots.merge(ksTb, 1, Integer::sum);
                }

                return snapshots;
            };

            return getNode().applyOnInstance(callable, "truncated-");
        }

        // these are not "dropped" nor "truncated"
        public Set<TestSnapshot> getSnapshots()
        {
            Set<TestSnapshot> existingSnapshots = new HashSet<>();

            IIsolatedExecutor.SerializableCallable<List<String>> callable = () -> {
                List<String> snapshots = new ArrayList<>();
                for (TableSnapshot snapshot : SnapshotManager.instance.getSnapshots(p -> {
                    String tag = p.getTag();
                    return !tag.contains("dropped-") && !tag.contains("truncated-");
                }))
                {
                    snapshots.add(format("%s.%s.%s", snapshot.getKeyspaceName(), snapshot.getTableName(), snapshot.getTag()));
                }

                return snapshots;
            };

            for (String tableSnapshot : getNode().callOnInstance(callable))
            {
                String[] components = tableSnapshot.split("\\.");
                existingSnapshots.add(new TestSnapshot(components[0],
                                                       components[1],
                                                       components[2]));
            }

            return existingSnapshots;
        }

        public void takeSnapshots(Set<TestSnapshot> snapshotsToTake)
        {
            for (TestSnapshot toTake : snapshotsToTake)
                getNode().acceptsOnInstance((SerializableTriConsumer<String, String, String>) (tag, ks, tb) -> {
                             try
                             {
                                 SnapshotManager.instance.takeSnapshot(tag, Map.of(), ks + '.' + tb);
                             }
                             catch (IOException ex)
                             {
                                 throw new RuntimeException(ex);
                             }
                         })
                         .accept(toTake.getTag(), toTake.getKeyspaceName(), toTake.getTableName());
        }

        public IInvokableInstance getNode()
        {
            if (cluster == null)
                throw new RuntimeException("not started yet");

            return cluster.get(1);
        }

        public void executeQuery(String query)
        {
            getNode().executeInternal(query);
        }

        public State start()
        {
            if (cluster != null)
                return null;

            try
            {
                cluster = builder.start();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            schema = getSchema();

            return this;
        }

        public void stop()
        {
            if (cluster != null)
            {
                try
                {
                    cluster.close();
                }
                catch (Throwable t)
                {
                    // ignore
                }
            }
        }

        public NodeToolResult nodetool(String... nodetoolArgs)
        {
            return getNode().nodetoolResult(nodetoolArgs);
        }
    }

    public static abstract class AbstractCommand implements StateOnlyCommand<State>
    {
        protected final RandomSource rs;
        protected final State state;

        public AbstractCommand(RandomSource rs, State state)
        {
            this.rs = rs;
            this.state = state;
        }

        public void executeQuery(String query)
        {
            state.getNode().executeInternal(query);
        }

        public IInvokableInstance getNode()
        {
            return state.getNode();
        }

        @Override
        public void applyUnit(State state)
        {
            Uninterruptibles.sleepUninterruptibly(Generators.TINY_TIME_SPAN_NANOS.generate(state.randomnessSource), TimeUnit.NANOSECONDS);
            doWork(state);
        }

        public abstract void doWork(State state);
    }
}
