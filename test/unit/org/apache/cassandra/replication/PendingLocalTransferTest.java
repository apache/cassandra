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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.TimeUUID;

import static java.lang.String.format;
import static org.apache.cassandra.io.sstable.format.SSTableFormat.Components.STATS;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers recovery of transfers staged in {@code pending/<planId>/} from their manifests. A transfer is staged into one
 * such directory per data directory it spans, so these tests exercise the multi-directory case.
 */
public class PendingLocalTransferTest
{
    @ClassRule
    public static final TemporaryFolder tmp = new TemporaryFolder();

    private static final String KS = "pending_local_transfer_test";
    private static final String TBL = "tbl";
    private static final String MANIFEST_FILE_NAME = "transfer.manifest";
    // manifest format: <version><transferId><sstableCount><activationStarted><activated><crc32>
    private static final int MANIFEST_TRANSFER_ID_OFFSET = 4;

    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setUpClass()
    {
        SchemaLoader.prepareServer();
        TableMetadata metadata = CreateTableStatement.parse(format("CREATE TABLE %s.%s (k int PRIMARY KEY, v int)", KS, TBL), KS).build();
        SchemaLoader.createKeyspace(KS, KeyspaceParams.simple(1, ReplicationType.tracked), metadata);
        cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
    }

    @Test
    public void testRecoversTransferStagedAcrossSeveralDirectories() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        ShortMutationId transferId = transferId();

        List<File> dirs = List.of(pendingDirectory(planId), pendingDirectory(planId));
        List<SSTableReader> staged = stage(dirs);

        new PendingLocalTransfer(planId, transferId, staged).writeManifestFile();

        for (File dir : dirs)
            assertThat(new File(dir, MANIFEST_FILE_NAME).exists()).describedAs("Manifest missing in %s", dir).isTrue();

        PendingLocalTransfer recovered = PendingLocalTransfer.load(cfs, planId, dirs);
        assertThat(recovered).isNotNull();
        assertThat(recovered.planId).isEqualTo(planId);
        assertThat(recovered.transferId).isEqualTo(transferId);
        assertThat(recovered.activated).isFalse();
        assertThat(descriptors(recovered.sstables)).isEqualTo(descriptors(staged));

        assertThat(PendingLocalTransfer.load(cfs, planId, Collections.singletonList(dirs.get(0)))).isNull();
        assertThat(dirs.get(0).exists()).isTrue();
    }

    @Test
    public void testRecoveredActivatedTransferIsDeleted() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = List.of(pendingDirectory(planId), pendingDirectory(planId));
        PendingLocalTransfer transfer = new PendingLocalTransfer(planId, transferId(), stage(dirs));
        transfer.writeManifestFile();

        transfer.activationStarted = true;
        transfer.activated = true;
        transfer.writeManifestFile();

        assertThat(PendingLocalTransfer.load(cfs, planId, dirs)).isNull();
        for (File dir : dirs)
            assertThat(dir.exists()).describedAs("%s was not deleted", dir).isFalse();
    }

    @Test
    public void testRecoveredActivatedTransferWithSSTablesAlreadyMovedIsDeleted() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = List.of(pendingDirectory(planId), pendingDirectory(planId));
        PendingLocalTransfer transfer = new PendingLocalTransfer(planId, transferId(), stage(dirs));
        transfer.writeManifestFile();

        transfer.activationStarted = true;
        transfer.activated = true;
        transfer.writeManifestFile();

        // Simulate activation moved every SSTable out of pending/ before this node restarted
        for (File dir : dirs)
            for (File file : dir.listUnchecked(f -> f.isFile() && !f.name().equals(MANIFEST_FILE_NAME)))
                file.delete();

        assertThat(PendingLocalTransfer.load(cfs, planId, dirs)).isNull();
        for (File dir : dirs)
            assertThat(dir.exists()).describedAs("%s was not deleted", dir).isFalse();
    }

    @Test
    public void testRecoversInterruptedActivationWithSomeSSTablesRemaining() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = List.of(pendingDirectory(planId), pendingDirectory(planId));
        List<SSTableReader> staged = stage(dirs);
        PendingLocalTransfer transfer = new PendingLocalTransfer(planId, transferId(), staged);
        transfer.writeManifestFile();

        transfer.activationStarted = true;
        transfer.writeManifestFile();

        // Simulate activation being interrupted after moving the SSTable out of one directory, but not the other
        for (File file : dirs.get(1).listUnchecked(f -> f.isFile() && !f.name().equals(MANIFEST_FILE_NAME)))
            file.delete();

        PendingLocalTransfer recovered = PendingLocalTransfer.load(cfs, planId, dirs);
        assertThat(recovered).isNotNull();
        assertThat(recovered.activationStarted).isTrue();
        assertThat(recovered.activated).isFalse();
        assertThat(descriptors(recovered.sstables)).isEqualTo(descriptors(List.of(staged.get(0))));

        // The empty directory must still be tracked, so a later write reaches it too
        assertThat(recovered.directories()).containsExactlyInAnyOrderElementsOf(dirs);
        recovered.activated = true;
        recovered.writeManifestFile();

        // If the empty directory hadn't been reached by the write above, its stale manifest would disagree with
        // the fresh one in the other directory, and recovery would reject the transfer instead of cleaning it up
        assertThat(PendingLocalTransfer.load(cfs, planId, dirs)).isNull();
        for (File dir : dirs)
            assertThat(dir.exists()).describedAs("%s was not deleted", dir).isFalse();
    }

    @Test
    public void testRecoversInterruptedActivationWithNoSSTablesRemaining() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = List.of(pendingDirectory(planId), pendingDirectory(planId));
        PendingLocalTransfer transfer = new PendingLocalTransfer(planId, transferId(), stage(dirs));
        transfer.writeManifestFile();

        transfer.activationStarted = true;
        transfer.writeManifestFile();

        // Simulate activation finishing every SSTable move before being interrupted, just before the final manifest write
        for (File dir : dirs)
            for (File file : dir.listUnchecked(f -> f.isFile() && !f.name().equals(MANIFEST_FILE_NAME)))
                file.delete();

        assertThat(PendingLocalTransfer.load(cfs, planId, dirs)).isNull();
        for (File dir : dirs)
            assertThat(dir.exists()).describedAs("%s was not deleted", dir).isFalse();
    }

    @Test
    public void testPurgeDeletesEveryStagedDirectory() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = List.of(pendingDirectory(planId), pendingDirectory(planId));
        PendingLocalTransfer transfer = new PendingLocalTransfer(planId, transferId(), stage(dirs));
        transfer.writeManifestFile();

        new TransferTrackingService().purge(transfer);

        // Leaving a directory behind would keep the plan looking staged forever, which fails later activations for it
        for (File dir : dirs)
            assertThat(dir.exists()).describedAs("%s was not deleted", dir).isFalse();
    }

    @Test
    public void testPurgingCollidedTransferDoesNotEvictTrackedOne() throws IOException
    {
        TimeUUID planId = nextTimeUUID();

        List<File> trackedDirs = List.of(pendingDirectory(planId));
        PendingLocalTransfer tracked = new PendingLocalTransfer(planId, transferId(), stage(trackedDirs));
        tracked.writeManifestFile();

        List<File> collidedDirs = List.of(pendingDirectory(planId));
        PendingLocalTransfer collided = new PendingLocalTransfer(planId, new ShortMutationId(2, 200), stage(collidedDirs));
        collided.writeManifestFile();

        TransferTrackingService transferTrackingService = new TransferTrackingService();
        transferTrackingService.received(tracked);

        // Simulate recovering a second, unrelated transfer that collided on the same plan id (e.g. a different table)
        transferTrackingService.purge(collided);

        // The transfer actually tracked under this plan id must survive purging an unrelated collision
        assertThat(transferTrackingService.getPendingTransfer(planId)).isEqualTo(tracked);
        for (File dir : trackedDirs)
            assertThat(dir.exists()).describedAs("%s was deleted", dir).isTrue();

        // The collided transfer's own resources are still cleaned up
        for (File dir : collidedDirs)
            assertThat(dir.exists()).describedAs("%s was not deleted", dir).isFalse();
    }

    @Test
    public void testTransferFailedPurgesOrphanedPendingDirectories() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = new ArrayList<>();
        for (DataDirectory dataDir : cfs.getDirectories().getWriteableLocations())
        {
            File dir = cfs.getDirectories().getPendingLocationForDisk(dataDir, planId);
            dirs.add(dir);
        }
        assertThat(dirs).isNotEmpty();
        PendingLocalTransfer transfer = new PendingLocalTransfer(planId, transferId(), stage(dirs));
        transfer.writeManifestFile();

        for (File dir : dirs)
            assertThat(dir.exists()).describedAs("%s does not exist", dir).isTrue();

        TransferTrackingService transferTrackingService = new TransferTrackingService();
        // Transfer is not tracked in memory, simulating coordinator crash/restart before failure notice
        transferTrackingService.purge(new TransferFailed(planId));

        for (File dir : dirs)
            assertThat(dir.exists()).describedAs("%s was not deleted by TransferFailed", dir).isFalse();
    }

    @Test
    public void testDoesNotRecoverTransferWithoutManifest() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = List.of(pendingDirectory(planId));
        stage(dirs);

        // The transfer ID the SSTables have to be activated with is only known from the manifest
        assertThat(PendingLocalTransfer.load(cfs, planId, dirs)).isNull();
    }

    @Test
    public void testDoesNotRecoverTransferWithMissingSSTables() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = List.of(pendingDirectory(planId), pendingDirectory(planId));
        List<SSTableReader> staged = stage(dirs);

        new PendingLocalTransfer(planId, transferId(), staged).writeManifestFile();

        for (File file : dirs.get(1).listUnchecked(f -> f.isFile() && !f.name().equals(MANIFEST_FILE_NAME)))
            file.delete();

        assertThat(PendingLocalTransfer.load(cfs, planId, dirs)).isNull();
        for (File dir : dirs)
            assertThat(dir.exists()).describedAs("%s was deleted", dir).isTrue();
    }

    @Test
    public void testDoesNotRecoverTransferWithUnreadableManifest() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = List.of(pendingDirectory(planId));
        new PendingLocalTransfer(planId, transferId(), stage(dirs)).writeManifestFile();

        // A crash can leave an empty, or half written, manifest behind. Recovery has to skip such a transfer instead
        // of failing, as it runs while the node is starting up.
        File manifest = new File(dirs.get(0), MANIFEST_FILE_NAME);
        try (FileOutputStreamPlus ignored = manifest.newOutputStream(File.WriteMode.OVERWRITE)) {}
        assertThat(manifest.length()).isEqualTo(0);

        assertThat(PendingLocalTransfer.load(cfs, planId, dirs)).isNull();
    }

    @Test
    public void testDoesNotRecoverTransferWithUnreadableSSTable() throws IOException
    {
        Config.DiskFailurePolicy policy = DatabaseDescriptor.getDiskFailurePolicy();
        DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.ignore);
        try
        {
            TimeUUID planId = nextTimeUUID();
            List<File> dirs = List.of(pendingDirectory(planId), pendingDirectory(planId));
            List<SSTableReader> staged = stage(dirs);
            new PendingLocalTransfer(planId, transferId(), staged).writeManifestFile();

            // Simulate an SSTable that was only partially written when the node went down
            staged.get(1).descriptor.fileFor(STATS).delete();

            assertThat(PendingLocalTransfer.load(cfs, planId, dirs)).isNull();
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(policy);
        }
    }

    @Test
    public void testDoesNotRecoverTransferWithCorruptManifest() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        List<File> dirs = List.of(pendingDirectory(planId));
        new PendingLocalTransfer(planId, transferId(), stage(dirs)).writeManifestFile();

        // Corrupt the transfer ID. Without a checksum this decodes into a plausible, but wrong, ID, which would then be
        // the ID its SSTables are activated under.
        File manifest = new File(dirs.get(0), MANIFEST_FILE_NAME);
        byte[] contents = Files.readAllBytes(manifest.toPath());
        contents[MANIFEST_TRANSFER_ID_OFFSET] ^= 0x01;
        Files.write(manifest.toPath(), contents);

        assertThat(PendingLocalTransfer.load(cfs, planId, dirs)).isNull();
    }

    @Test
    public void testLookingUpStagedTransfersDoesNotCreatePendingDirectories()
    {
        assertThat(TransferTrackingService.hasPendingDirectories(KS, nextTimeUUID())).isFalse();

        for (File dataDirectory : cfs.getDirectories().getCFDirectories())
            assertThat(new File(dataDirectory, "pending").exists()).describedAs("%s was created", dataDirectory).isFalse();

        assertThat(cfs.getDirectories().getPendingLocations()).isEmpty();
    }

    @Test
    public void testPlanIdFromDirectory() throws IOException
    {
        TimeUUID planId = nextTimeUUID();
        assertThat(PendingLocalTransfer.planIdFromDirectory(pendingDirectory(planId))).isEqualTo(planId);
        assertThat(PendingLocalTransfer.planIdFromDirectory(new File(tmp.newFolder(), "not-a-plan-id"))).isNull();
    }

    private static ShortMutationId transferId()
    {
        return new ShortMutationId(1, 100);
    }

    /**
     * Stages a copy of the same SSTable in each of the given directories, standing in for a stream that wrote into the
     * pending directory of more than one data directory.
     */
    private static List<SSTableReader> stage(Collection<File> dirs) throws IOException
    {
        QueryProcessor.executeInternal(format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", KS, TBL));
        Util.flush(cfs);
        SSTableReader source = Iterables.getFirst(cfs.getLiveSSTables(), null);
        assertThat(source).isNotNull();

        List<SSTableReader> staged = new ArrayList<>(dirs.size());
        for (File dir : dirs)
        {
            Set<Component> components = source.getComponents();
            Descriptor target = cfs.getUniqueDescriptorFor(source.descriptor, dir);
            for (Component component : components)
            {
                File file = source.descriptor.fileFor(component);
                if (file.exists())
                    Files.copy(file.toPath(), target.fileFor(component).toPath());
            }
            staged.add(SSTableReader.open(cfs, target, components, cfs.metadata));
        }
        return staged;
    }

    private static Set<Descriptor> descriptors(Collection<SSTableReader> sstables)
    {
        return sstables.stream().map(sstable -> sstable.descriptor).collect(Collectors.toSet());
    }

    /**
     * @return a freshly created {@code <disk>/pending/<planId>} directory, one per call, so that a single plan can be
     *         staged into as many directories as a transfer spanning several disks would use
     */
    private static File pendingDirectory(TimeUUID planId) throws IOException
    {
        File dir = new File(new File(tmp.newFolder(), "pending"), planId.toString());
        dir.tryCreateDirectories();
        assertThat(dir.isDirectory()).isTrue();
        return dir;
    }
}
