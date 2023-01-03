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

import org.junit.Assume;
import org.junit.BeforeClass;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.big.BigFormat;

import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.assertj.core.api.Assertions.assertThat;

public class RowIndexSizeWarningTest extends AbstractClientSizeWarning
{
    @BeforeClass
    public static void setupClass() throws IOException
    {
        AbstractClientSizeWarning.setupClass();

        //noinspection Convert2MethodRef
        Assume.assumeTrue(CLUSTER.get(1).callOnInstance(() -> BigFormat.isSelected()));

        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            DatabaseDescriptor.setRowIndexReadSizeWarnThreshold(new DataStorageSpec.LongBytesBound(1, KIBIBYTES));
            DatabaseDescriptor.setRowIndexReadSizeFailThreshold(new DataStorageSpec.LongBytesBound(2, KIBIBYTES));

            // hack to force multiple index entries
            DatabaseDescriptor.setColumnIndexCacheSize(1 << 20);
            DatabaseDescriptor.setColumnIndexSizeInKiB(0);
        }));
    }

    @Override
    protected boolean shouldFlush()
    {
        // need to flush as RowIndexEntry is at the SSTable level
        return true;
    }

    @Override
    protected int warnThresholdRowCount()
    {
        return 15;
    }

    @Override
    protected int failThresholdRowCount()
    {
        // since the RowIndexEntry grows slower than a partition, need even more rows to trigger
        return 40;
    }

    @Override
    public void noWarningsScan()
    {
        Assume.assumeFalse("Ignore Scans", true);
    }

    @Override
    public void warnThresholdScan()
    {
        Assume.assumeFalse("Ignore Scans", true);
    }

    @Override
    public void warnThresholdScanWithReadRepair()
    {
        Assume.assumeFalse("Ignore Scans", true);
    }

    @Override
    public void failThresholdScanTrackingEnabled()
    {
        Assume.assumeFalse("Ignore Scans", true);
    }

    @Override
    public void failThresholdScanTrackingDisabled()
    {
        Assume.assumeFalse("Ignore Scans", true);
    }

    @Override
    protected void assertWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("(see row_index_size_warn_threshold)").contains("bytes in RowIndexEntry and issued warnings for query");
    }

    @Override
    protected void assertAbortWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("(see row_index_size_fail_threshold)").contains("bytes in RowIndexEntry and aborted the query");
    }

    @Override
    protected long[] getHistogram()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.RowIndexSize." + KEYSPACE)).toArray();
    }

    @Override
    protected long totalWarnings()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.RowIndexSizeWarnings." + KEYSPACE)).sum();
    }

    @Override
    protected long totalAborts()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.RowIndexSizeAborts." + KEYSPACE)).sum();
    }
}
