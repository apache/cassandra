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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.util.concurrent.Futures;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.snapshot.SnapshotManifest;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
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
            Pair<String, String[]> initialisationData = initialise(c);

            rewriteManifestToEphemeral(initialisationData.left, initialisationData.right);

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
            Pair<String, String[]> initialisationData = initialise(c);

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

            Pair<String, String[]> initialisationData = initialise(c);
            rewriteManifestToEphemeral(initialisationData.left, initialisationData.right);

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

            Pair<String, String[]> initialisationData = initialise(c);

            rewriteManifestToEphemeral(initialisationData.left, initialisationData.right);

            instance.nodetoolResult("clearsnapshot", "--all").asserts().success();
            assertTrue(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName));
            assertFalse(instance.nodetoolResult("listsnapshots", "-e").getStdout().contains(snapshotName2));
        }
    }

    private Pair<String, String[]> initialise(Cluster c)
    {
        c.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s." + tableName + " (cityid int PRIMARY KEY, name text)"));
        c.coordinator(1).execute(withKeyspace("INSERT INTO %s." + tableName + "(cityid, name) VALUES (1, 'Canberra');"), ONE);
        IInvokableInstance instance = c.get(1);

        instance.flush(KEYSPACE);

        assertEquals(0, instance.nodetool("snapshot", "-kt", withKeyspace("%s." + tableName), "-t", snapshotName));
        waitForSnapshot(instance, snapshotName);

        // take one more snapshot, this one is not ephemeral,
        // starting Cassandra will clear ephemerals, but it will not affect non-ephemeral snapshots
        assertEquals(0, instance.nodetool("snapshot", "-kt", withKeyspace("%s." + tableName), "-t", snapshotName2));
        waitForSnapshot(instance, snapshotName2);

        String tableId = instance.callOnInstance((IIsolatedExecutor.SerializableCallable<String>) () -> {
            return Keyspace.open(KEYSPACE).getMetadata().tables.get(tableName).get().id.asUUID().toString().replaceAll("-", "");
        });

        String[] dataDirs = (String[]) instance.config().get("data_file_directories");

        return Pair.create(tableId, dataDirs);
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
}
