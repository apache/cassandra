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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import com.google.common.util.concurrent.Futures;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.SnapshotManifest;
import org.apache.cassandra.service.snapshot.SnapshotOptions;
import org.apache.cassandra.service.snapshot.SnapshotType;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.schema.SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES;
import static org.apache.cassandra.schema.SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EphemeralSnapshotTest extends TestBaseImpl
{
    private static final String snapshotName = "snapshotname";
    private static final String snapshotName2 = "second-snapshot";
    private static final String tableName = "city";

    @Test
    public void testStartupRemovesEphemeralSnapshotOnEphemeralFlagInManifest() throws Exception
    {
        try (Cluster c = init(builder().withNodes(1)
                                       .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                       .start()))
        {
            Pair<String, String[]> initialisationData = initialise(c, tableName);

            rewriteManifestToEphemeral(initialisationData.left, initialisationData.right);

            c.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> SnapshotManager.instance.restart(true));

            verify(c.get(1));
        }
    }

    // TODO this test might be deleted once we get rid of ephemeral marker file for good in 4.3
    @Test
    public void testStartupRemovesEphemeralSnapshotOnMarkerFile() throws Exception
    {
        try (Cluster c = init(builder().withNodes(1)
                                       .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                       .start()))
        {
            Pair<String, String[]> initialisationData = initialise(c, tableName);

            String tableId = initialisationData.left;
            String[] dataDirs = initialisationData.right;

            // place ephemeral marker file into snapshot directory pretending it was created as ephemeral
            Path ephemeralMarkerFile = Paths.get(dataDirs[0])
                                            .resolve(KEYSPACE)
                                            .resolve(format("%s-%s", tableName, tableId))
                                            .resolve("snapshots")
                                            .resolve(snapshotName)
                                            .resolve("ephemeral.snapshot");

            Files.createFile(ephemeralMarkerFile);

            c.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> SnapshotManager.instance.restart(true));

            verify(c.get(1));
        }
    }

    @Test
    public void testEphemeralSnapshotIsNotClearableFromNodetool() throws Exception
    {
        try (Cluster c = init(builder().withNodes(1)
                                       .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                       .start()))
        {
            IInvokableInstance instance = c.get(1);

            Pair<String, String[]> initialisationData = initialise(c, tableName);
            rewriteManifestToEphemeral(initialisationData.left, initialisationData.right);

            c.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> SnapshotManager.instance.restart(true));

            assertTrue(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName));
            instance.nodetoolResult("clearsnapshot", "-t", snapshotName).asserts().success();
            // ephemeral snapshot was not removed as it can not be (from nodetool / user operation)
            assertTrue(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName));

            assertFalse(instance.logs().grep("Skipping deletion of ephemeral snapshot 'snapshotname' in keyspace distributed_test_keyspace. " +
                                             "Ephemeral snapshots are not removable by a user.").getResult().isEmpty());
        }
    }

    @Test
    public void testClearingAllSnapshotsFromNodetoolWillKeepEphemeralSnaphotsIntact() throws Exception
    {
        try (Cluster c = init(builder().withNodes(1)
                                       .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                       .start()))
        {
            IInvokableInstance instance = c.get(1);

            Pair<String, String[]> initialisationData = initialise(c, tableName);

            rewriteManifestToEphemeral(initialisationData.left, initialisationData.right);

            c.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> SnapshotManager.instance.restart(true));

            instance.nodetoolResult("clearsnapshot", "--all").asserts().success();
            assertTrue(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName));
            assertFalse(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName2));
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-20490">CASSANDRA-20490</a>
     */
    @Test
    public void testForceEphemeralSnapshotWhenAlreadyExists() throws Exception
    {
        try (Cluster c = init(builder().withNodes(1)
                                       .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                       .start()))
        {
            IInvokableInstance instance = c.get(1);

            c.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s." + tableName + " (cityid int PRIMARY KEY, name text)"));
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s." + tableName + "(cityid, name) VALUES (1, 'Canberra');"), ONE);

            instance.flush(KEYSPACE);

            takeEphemeralSnapshotForcibly(c, KEYSPACE, tableName, snapshotName);
            assertTrue(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName));
            float firstSnapshotSize = getSnapshotSizeOnDisk(c, KEYSPACE, tableName, snapshotName);

            SnapshotManifest snapshotManifest = SnapshotManifest.deserializeFromJsonFile(new File(findManifest(getDataDirs(c), getTableId(c, KEYSPACE, tableName))));
            assertEquals(1, snapshotManifest.getFiles().size());

            // list sstables
            List<String> snapshotFilesAfterFirstSnapshot = getSnapshotFiles(c, snapshotName);
            assertFalse(snapshotFilesAfterFirstSnapshot.isEmpty());

            // add more data
            insertData(c, tableName);

            takeEphemeralSnapshotForcibly(c, KEYSPACE, tableName, snapshotName);
            assertTrue(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName));
            SnapshotManifest secondSnapshotManifest = SnapshotManifest.deserializeFromJsonFile(new File(findManifest(getDataDirs(c), getTableId(c, KEYSPACE, tableName))));
            assertEquals(2, secondSnapshotManifest.getFiles().size());

            List<String> snapshotFilesAfterSecondSnapshot = getSnapshotFiles(c, snapshotName);
            assertFalse(snapshotFilesAfterSecondSnapshot.isEmpty());

            // list again and check it is superset of previous listing
            assertTrue(snapshotFilesAfterSecondSnapshot.size() > snapshotFilesAfterFirstSnapshot.size());
            assertTrue(snapshotFilesAfterSecondSnapshot.containsAll(snapshotFilesAfterFirstSnapshot));
            assertTrue(secondSnapshotManifest.getFiles().containsAll(snapshotManifest.getFiles()));

            float secondSnapshotSize = getSnapshotSizeOnDisk(c, KEYSPACE, tableName, snapshotName);

            assertTrue(secondSnapshotSize > firstSnapshotSize);
        }
    }

    private Float getSnapshotSizeOnDisk(Cluster c, String keyspace, String table, String snapshotName)
    {
        return c.get(1).applyOnInstance((IIsolatedExecutor.SerializableTriFunction<String, String, String, Float>) (ks, tb, name) -> {

            Map<String, TabularData> stringTabularDataMap = SnapshotManager.instance.listSnapshots(Map.of("include_ephemeral", "true"));

            TabularDataSupport tabularData = (TabularDataSupport) stringTabularDataMap.get(name);
            for (Object value : tabularData.values())
            {
                CompositeDataSupport cds = (CompositeDataSupport) value;
                return Float.parseFloat(((String) cds.get("Size on disk")).split(" ")[0]);
            }

            return 0F;
        }, keyspace, table, snapshotName);
    }

    private void takeEphemeralSnapshotForcibly(Cluster c, String keyspace, String table, String snapshotName)
    {
        c.get(1).applyOnInstance((IIsolatedExecutor.SerializableTriFunction<String, String, String, Void>) (ks, tb, name) ->
        {
            ColumnFamilyStore cfs = Keyspace.getValidKeyspace(ks).getColumnFamilyStore(tb);
            try
            {
                SnapshotManager.instance.takeSnapshot(SnapshotOptions.systemSnapshot(name, SnapshotType.REPAIR, (sstable) -> true, cfs.getKeyspaceTableName())
                                                                     .ephemeral()
                                                                     .build());
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t.getMessage());
            }
            return null;
        }, keyspace, table, snapshotName);
    }

    private void insertData(Cluster c, String tableName)
    {
        c.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s." + tableName + " (cityid int PRIMARY KEY, name text)"));
        c.coordinator(1).execute(withKeyspace("INSERT INTO %s." + tableName + "(cityid, name) VALUES (1, 'Canberra');"), ONE);
        IInvokableInstance instance = c.get(1);
        instance.flush(KEYSPACE);
    }

    private Pair<String, String[]> initialise(Cluster c, String tableName)
    {
        insertData(c, tableName);
        IInvokableInstance instance = c.get(1);

        assertEquals(0, instance.nodetool("snapshot", "-kt", withKeyspace("%s." + tableName), "-t", snapshotName));
        waitForSnapshot(instance, snapshotName);

        // take one more snapshot, this one is not ephemeral,
        // starting Cassandra will clear ephemerals, but it will not affect non-ephemeral snapshots
        assertEquals(0, instance.nodetool("snapshot", "-kt", withKeyspace("%s." + tableName), "-t", snapshotName2));
        waitForSnapshot(instance, snapshotName2);

        String tableId = getTableId(c, KEYSPACE, tableName);

        String[] dataDirs = getDataDirs(c);

        return Pair.create(tableId, dataDirs);
    }

    private String[] getDataDirs(Cluster c)
    {
        return (String[]) c.get(1).config().get("data_file_directories");
    }

    private void verify(IInvokableInstance instance)
    {
        // by default, we do not see ephemerals
        assertFalse(instance.nodetoolResult("listsnapshots").getStdout().contains(snapshotName));

        // we see them via -e flag
        assertTrue(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName));

        Futures.getUnchecked(instance.shutdown());

        // startup should remove ephemeral snapshot
        instance.startup();

        assertFalse(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName));
        assertTrue(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName2));
    }

    private void waitForSnapshot(IInvokableInstance instance, String snapshotName)
    {
        await().timeout(20, SECONDS)
               .pollInterval(1, SECONDS)
               .until(() -> instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName));
    }

    private void rewriteManifestToEphemeral(String tableId, String[] dataDirs) throws Exception
    {
        // rewrite manifest, pretend that it is ephemeral
        Path manifestPath = findManifest(dataDirs, tableId);
        SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(new File(manifestPath));
        SnapshotManifest manifestWithEphemeralFlag = new SnapshotManifest(manifest.files, null, manifest.createdAt, true);
        manifestWithEphemeralFlag.serializeToJsonFile(new File(manifestPath));
    }

    private Path findManifest(String[] dataDirs, String tableId)
    {
        for (String dataDir : dataDirs)
        {
            Path manifest = Paths.get(dataDir)
                                 .resolve(KEYSPACE)
                                 .resolve(format("%s-%s", tableName, tableId))
                                 .resolve("snapshots")
                                 .resolve(snapshotName)
                                 .resolve("manifest.json");

            if (Files.exists(manifest))
            {
                return manifest;
            }
        }

        throw new IllegalStateException("Unable to find manifest!");
    }

    private List<String> getSnapshotFiles(Cluster cluster, String snapshotName)
    {
        return cluster.get(1).applyOnInstance((IIsolatedExecutor.SerializableFunction<String, List<String>>) (name) -> {
            List<String> result = new ArrayList<>();

            for (Keyspace keyspace : Keyspace.all())
            {
                if (LOCAL_SYSTEM_KEYSPACE_NAMES.contains(keyspace.getName()) || REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(keyspace.getName()))
                    continue;

                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                {
                    for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
                    {
                        File snapshotDir = new File(dataDir, format("%s/%s-%s/snapshots/%s", keyspace.getName(), cfs.name, cfs.metadata().id.toHexString(), name));
                        if (snapshotDir.exists())
                        {
                            try
                            {
                                Files.list(snapshotDir.toPath()).forEach(p -> result.add(p.toString()));
                            }
                            catch (IOException e)
                            {
                                throw new RuntimeException("Unable to list " + snapshotDir.toPath(), e);
                            }
                        }
                    }
                }
            }

            return result;
        }, snapshotName);
    }

    private String getTableId(Cluster c, String keyspace, String tableName)
    {
        return c.get(1).applyOnInstance((IIsolatedExecutor.SerializableBiFunction<String, String, String>) (ks, tb) -> {
            return Keyspace.open(ks).getMetadata().tables.get(tb).get().id.asUUID().toString().replaceAll("-", "");
        }, keyspace, tableName);
    }
}
