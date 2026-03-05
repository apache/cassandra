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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.MEBIBYTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed tests for write size threshold warnings.
 * Tests that writes to large partitions (tracked in TopPartitionTracker) trigger warnings.
 */
public class WriteSizeWarningTest extends AbstractWriteThresholdWarning
{
    private static final long WARN_THRESHOLD_BYTES = 5 * 1024 * 1024; // 5MB
    private static final Logger log = LoggerFactory.getLogger(WriteSizeWarningTest.class);

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AbstractWriteThresholdWarning.setupClass();

        // Setup write size threshold after cluster init
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            DatabaseDescriptor.setWriteSizeWarnThreshold(new DataStorageSpec.LongBytesBound(5, MEBIBYTES));
            // Set minimum tracked partition size to ensure partitions are tracked
            // This should be lower than the test value (10MB) to allow tracking
            DatabaseDescriptor.setMinTrackedPartitionSizeInBytes(new DataStorageSpec.LongBytesBound(1, MEBIBYTES));
            DatabaseDescriptor.setWriteThresholdsEnabled(true);
        }));
    }

    @Override
    protected long getWarnThreshold()
    {
        return WARN_THRESHOLD_BYTES;
    }

    @Override
    protected void populateTopPartitions(int pk, long sizeBytes)
    {
        CLUSTER.stream().forEach(node -> node.runOnInstance(() -> {
            var key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(pk));
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            cfs.topPartitions.topSizes().track(key, sizeBytes);
        }));
    }

    @Override
    protected long totalWarnings()
    {
        return CLUSTER.stream()
                      .mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.WriteSizeWarnings." + KEYSPACE))
                      .sum();
    }

    @Override
    protected void assertWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0))
            .contains(KEYSPACE + ".tbl")
            .contains("large partition")
            .contains("estimated size is")
            .contains("bytes")
            .contains("write_size_warn_threshold");
    }
}