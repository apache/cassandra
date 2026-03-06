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

package org.apache.cassandra.distributed.test.thresholds;

import java.io.IOException;
import java.util.List;

import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed tests for write tombstone threshold warnings.
 * Tests that writes to partitions with many tombstones (tracked in TopPartitionTracker) trigger warnings.
 */
public class WriteTombstoneWarningTest extends AbstractWriteThresholdWarning
{
    private static final long WARN_THRESHOLD_COUNT = 1000;

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AbstractWriteThresholdWarning.setupClass();

        // Setup write tombstone threshold after cluster init
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            // Set minimum tracked count first, before the threshold (validation requires threshold >= min)
            DatabaseDescriptor.setMinTrackedPartitionTombstoneCount(100);
            DatabaseDescriptor.setWriteTombstoneWarnThreshold((int) WARN_THRESHOLD_COUNT);
            DatabaseDescriptor.setCoordinatorWriteWarnInterval(new DurationSpec.LongMillisecondsBound("0ms"));
        }));
    }

    @Override
    protected long getWarnThreshold()
    {
        return WARN_THRESHOLD_COUNT;
    }

    @Override
    protected void populateTopPartitions(int pk, long tombstoneCount)
    {
        CLUSTER.stream().forEach(node -> node.runOnInstance(() -> {
            // Get the DecoratedKey for the partition
            var key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(pk));

            // Get the ColumnFamilyStore
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");

            // Populate TopPartitionTracker with the tombstone count
            if (cfs.topPartitions != null)
            {
                cfs.topPartitions.topTombstones().track(key, tombstoneCount);
            }
        }));
    }

    @Override
    protected long totalWarnings()
    {
        return CLUSTER.stream()
                      .mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.WriteTombstoneWarnings." + KEYSPACE))
                      .sum();
    }

    @Override
    protected void assertWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0))
            .contains(KEYSPACE + ".tbl")
            .contains("many tombstones")
            .contains("estimated count is")
            .contains("write_tombstone_warn_threshold");
    }
}