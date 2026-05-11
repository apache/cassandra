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

package org.apache.cassandra.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.SystemKeyspaceStorage;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.apache.cassandra.db.SystemKeyspace.METADATA_LOG;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for OfflineClusterMetadataDump tool that verify gap detection and metadata output.
 * <p>
 * These tests write entries directly to the system keyspace storage and then
 * call OfflineClusterMetadataDump.BaseCommand.getLogState() directly to verify gap detection behavior.
 */
public class OfflineClusterMetadataDumpIntegrationTest extends OfflineToolUtils
{
    private SystemKeyspaceStorage storage;

    @BeforeClass
    public static void setupClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    @Before
    public void setup()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, METADATA_LOG);
        if (cfs != null)
            cfs.truncateBlockingWithoutSnapshot();

        storage = new SystemKeyspaceStorage(() -> MetadataSnapshots.NO_OP);
    }

    private Entry entry(long epoch)
    {
        return new Entry(new Entry.Id(epoch),
                         Epoch.create(epoch),
                         CustomTransformation.make((int) epoch));
    }

    @Test
    public void testGapDetectionInEpochs()
    {
        // Write entries with gap at epoch 3
        storage.append(entry(1));
        storage.append(entry(2));
        storage.append(entry(4));  // Gap: skipping 3
        storage.append(entry(5));

        TestOutput testOutput = new TestOutput();
        LogState logState = OfflineClusterMetadataDump.BaseCommand.getLogState(storage, MetadataSnapshots.NO_OP, null, null, testOutput.getOutput());
        String stderr = testOutput.getStderr();

        // Verify gap is detected and reported
        assertThat(stderr).contains("Gap detected");
        assertThat(stderr).contains("expected epoch 3 but found 4");

        // All epochs should still be in the log state
        assertThat(logState.entries).hasSize(4);
        assertThat(logState.entries.get(0).epoch.getEpoch()).isEqualTo(1);
        assertThat(logState.entries.get(1).epoch.getEpoch()).isEqualTo(2);
        assertThat(logState.entries.get(2).epoch.getEpoch()).isEqualTo(4);
        assertThat(logState.entries.get(3).epoch.getEpoch()).isEqualTo(5);
    }

    @Test
    public void testMultipleGapsDetection()
    {
        // Write entries with multiple gaps: missing 2, 4, 6, 7
        storage.append(entry(1));
        storage.append(entry(3));  // Gap: skipping 2
        storage.append(entry(5));  // Gap: skipping 4
        storage.append(entry(8));  // Gap: skipping 6, 7

        TestOutput testOutput = new TestOutput();
        LogState logState = OfflineClusterMetadataDump.BaseCommand.getLogState(storage, MetadataSnapshots.NO_OP, null, null, testOutput.getOutput());
        String stderr = testOutput.getStderr();

        // Verify multiple gaps are detected
        assertThat(stderr).contains("Gap detected");
        assertThat(stderr).contains("expected epoch 2 but found 3");
        assertThat(stderr).contains("expected epoch 4 but found 5");
        assertThat(stderr).contains("expected epoch 6 but found 8");

        // All available epochs should still be in the log state
        assertThat(logState.entries).hasSize(4);
        assertThat(logState.entries.get(0).epoch.getEpoch()).isEqualTo(1);
        assertThat(logState.entries.get(1).epoch.getEpoch()).isEqualTo(3);
        assertThat(logState.entries.get(2).epoch.getEpoch()).isEqualTo(5);
        assertThat(logState.entries.get(3).epoch.getEpoch()).isEqualTo(8);
    }

    @Test
    public void testNoGapsNoWarnings()
    {
        // No gaps
        storage.append(entry(1));
        storage.append(entry(2));
        storage.append(entry(3));
        storage.append(entry(4));
        storage.append(entry(5));

        TestOutput testOutput = new TestOutput();
        LogState logState = OfflineClusterMetadataDump.BaseCommand.getLogState(storage, MetadataSnapshots.NO_OP, null, null, testOutput.getOutput());
        String stderr = testOutput.getStderr();

        // Gap warnings should not appear
        assertThat(stderr).doesNotContain("Gap detected");
        assertThat(stderr).doesNotContain("WARNING");

        // All entries should be in the log state
        assertThat(logState.entries).hasSize(5);
        for (int i = 0; i < 5; i++)
        {
            assertThat(logState.entries.get(i).epoch.getEpoch()).isEqualTo(i + 1);
        }
    }

    @Test
    public void testEmptyLogReturnsEmptyState()
    {
        // Don't write any entries - log is empty
        TestOutput testOutput = new TestOutput();
        LogState logState = OfflineClusterMetadataDump.BaseCommand.getLogState(storage, MetadataSnapshots.NO_OP, null, null, testOutput.getOutput());

        // Should return empty log state
        assertThat(logState.isEmpty()).isTrue();
        assertThat(logState.entries).isEmpty();
    }

    @Test
    public void testSingleEntryNoGap()
    {
        // Single entry at epoch 1
        storage.append(entry(1));

        TestOutput testOutput = new TestOutput();
        LogState logState = OfflineClusterMetadataDump.BaseCommand.getLogState(storage, MetadataSnapshots.NO_OP, null, null, testOutput.getOutput());
        String stderr = testOutput.getStderr();

        // No gap warnings
        assertThat(stderr).doesNotContain("Gap detected");

        // Single entry should be present
        assertThat(logState.entries).hasSize(1);
        assertThat(logState.entries.get(0).epoch.getEpoch()).isEqualTo(1);
    }

    @Test
    public void testGapAtBeginning()
    {
        // Start with epoch 3 instead of 1 - gap at the beginning
        storage.append(entry(3));
        storage.append(entry(4));
        storage.append(entry(5));

        TestOutput testOutput = new TestOutput();
        LogState logState = OfflineClusterMetadataDump.BaseCommand.getLogState(storage, MetadataSnapshots.NO_OP, null, null, testOutput.getOutput());
        String stderr = testOutput.getStderr();

        // Should detect gap at beginning (expected 1 but found 3)
        assertThat(stderr).contains("Gap detected");
        assertThat(stderr).contains("expected epoch 1 but found 3");

        // All entries should still be present
        assertThat(logState.entries).hasSize(3);
        assertThat(logState.entries.get(0).epoch.getEpoch()).isEqualTo(3);
        assertThat(logState.entries.get(1).epoch.getEpoch()).isEqualTo(4);
        assertThat(logState.entries.get(2).epoch.getEpoch()).isEqualTo(5);
    }

    @Test
    public void testTargetEpochFilter()
    {
        // Write entries 1-10
        for (int i = 1; i <= 10; i++)
        {
            storage.append(entry(i));
        }

        // Get log state up to epoch 5
        TestOutput testOutput = new TestOutput();
        LogState logState = OfflineClusterMetadataDump.BaseCommand.getLogState(storage, MetadataSnapshots.NO_OP, null, 5L, testOutput.getOutput());
        String stderr = testOutput.getStderr();

        // No gaps
        assertThat(stderr).doesNotContain("Gap detected");

        // Should only have epochs up to 5
        assertThat(logState.entries).hasSizeLessThanOrEqualTo(5);
        for (Entry e : logState.entries)
        {
            assertThat(e.epoch.getEpoch()).isLessThanOrEqualTo(5);
        }
    }

    /**
     * Helper class to capture output from the gap detection logic.
     */
    private static class TestOutput
    {
        private final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        private final ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        private final Output output = new Output(new PrintStream(outStream), new PrintStream(errStream));

        public Output getOutput()
        {
            return output;
        }

        public String getStderr()
        {
            return errStream.toString();
        }
    }
}
