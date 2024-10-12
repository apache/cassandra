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

package org.apache.cassandra.test.microbench;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@Threads(4)
@State(Scope.Benchmark)
public class CassandraMetricsRegistryBench
{
    private static final String KEYSPACE = "keyspace";
    private static final int collectionSize = 10000;

    private MetricRegistry registry;
    private String key;

    @Setup(Level.Trial)
    public void setup() throws NoSuchAlgorithmException
    {
        registry = new CassandraMetricsRegistry();
        for (int i = 0; i < collectionSize; i++)
        {
            for (String template : KEYSPACE_TEMPLATES)
            {
                String metricName = String.format(template, (KEYSPACE + i));
                int finalI = i;
                registry.registerGauge(metricName, () -> finalI);
            }
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();
        key = String.format(KEYSPACE_TEMPLATES[random.nextInt(0, KEYSPACE_TEMPLATES.length)],
                            KEYSPACE + random.nextInt(0, collectionSize));
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor",
    })
    public Metric getKey()
    {
        return registry.getMetrics().get(key);
    }

    public static void main(String[] args) throws Exception
    {
        Options opt = new OptionsBuilder()
                          .include(CassandraMetricsRegistryBench.class.getSimpleName())
                          .build();
        new Runner(opt).run();
    }

    private static final String[] KEYSPACE_TEMPLATES = new String[]{
        "org.apache.cassandra.metrics.keyspace.AdditionalWrites.%s",
        "org.apache.cassandra.metrics.keyspace.AllMemtablesLiveDataSize.%s",
        "org.apache.cassandra.metrics.keyspace.AllMemtablesOffHeapDataSize.%s",
        "org.apache.cassandra.metrics.keyspace.AllMemtablesOnHeapDataSize.%s",
        "org.apache.cassandra.metrics.keyspace.AntiCompactionTime.%s",
        "org.apache.cassandra.metrics.keyspace.BloomFilterDiskSpaceUsed.%s",
        "org.apache.cassandra.metrics.keyspace.BloomFilterFalsePositives.%s",
        "org.apache.cassandra.metrics.keyspace.BloomFilterFalseRatio.%s",
        "org.apache.cassandra.metrics.keyspace.BloomFilterOffHeapMemoryUsed.%s",
        "org.apache.cassandra.metrics.keyspace.BytesValidated.%s",
        "org.apache.cassandra.metrics.keyspace.CasCommitLatency.%s",
        "org.apache.cassandra.metrics.keyspace.CasCommitTotalLatency.%s",
        "org.apache.cassandra.metrics.keyspace.CasPrepareLatency.%s",
        "org.apache.cassandra.metrics.keyspace.CasPrepareTotalLatency.%s",
        "org.apache.cassandra.metrics.keyspace.CasProposeLatency.%s",
        "org.apache.cassandra.metrics.keyspace.CasProposeTotalLatency.%s",
        "org.apache.cassandra.metrics.keyspace.ClientTombstoneAborts.%s",
        "org.apache.cassandra.metrics.keyspace.ClientTombstoneWarnings.%s",
        "org.apache.cassandra.metrics.keyspace.ColUpdateTimeDeltaHistogram.%s",
        "org.apache.cassandra.metrics.keyspace.CompressionMetadataOffHeapMemoryUsed.%s",
        "org.apache.cassandra.metrics.keyspace.CoordinatorReadSize.%s",
        "org.apache.cassandra.metrics.keyspace.CoordinatorReadSizeAborts.%s",
        "org.apache.cassandra.metrics.keyspace.CoordinatorReadSizeWarnings.%s",
        "org.apache.cassandra.metrics.keyspace.IdealCLWriteLatency.%s",
        "org.apache.cassandra.metrics.keyspace.IdealCLWriteTotalLatency.%s",
        "org.apache.cassandra.metrics.keyspace.IndexSummaryOffHeapMemoryUsed.%s",
        "org.apache.cassandra.metrics.keyspace.KeyCacheHitRate.%s",
        "org.apache.cassandra.metrics.keyspace.LiveDiskSpaceUsed.%s",
        "org.apache.cassandra.metrics.keyspace.LiveScannedHistogram.%s",
        "org.apache.cassandra.metrics.keyspace.LocalReadSize.%s",
        "org.apache.cassandra.metrics.keyspace.LocalReadSizeAborts.%s",
        "org.apache.cassandra.metrics.keyspace.LocalReadSizeWarnings.%s",
        "org.apache.cassandra.metrics.keyspace.MemtableColumnsCount.%s",
        "org.apache.cassandra.metrics.keyspace.MemtableLiveDataSize.%s",
        "org.apache.cassandra.metrics.keyspace.MemtableOffHeapDataSize.%s",
        "org.apache.cassandra.metrics.keyspace.MemtableOnHeapDataSize.%s",
        "org.apache.cassandra.metrics.keyspace.MemtableSwitchCount.%s",
        "org.apache.cassandra.metrics.keyspace.PartitionsValidated.%s",
        "org.apache.cassandra.metrics.keyspace.PaxosOutOfRangeToken.%s",
        "org.apache.cassandra.metrics.keyspace.PendingCompactions.%s",
        "org.apache.cassandra.metrics.keyspace.PendingFlushes.%s",
        "org.apache.cassandra.metrics.keyspace.RangeLatency.%s",
        "org.apache.cassandra.metrics.keyspace.RangeTotalLatency.%s",
        "org.apache.cassandra.metrics.keyspace.ReadLatency.%s",
        "org.apache.cassandra.metrics.keyspace.ReadOutOfRangeToken.%s",
        "org.apache.cassandra.metrics.keyspace.ReadTotalLatency.%s",
        "org.apache.cassandra.metrics.keyspace.RecentBloomFilterFalsePositives.%s",
        "org.apache.cassandra.metrics.keyspace.RecentBloomFilterFalseRatio.%s",
        "org.apache.cassandra.metrics.keyspace.RepairJobsCompleted.%s",
        "org.apache.cassandra.metrics.keyspace.RepairJobsStarted.%s",
        "org.apache.cassandra.metrics.keyspace.RepairPrepareTime.%s",
        "org.apache.cassandra.metrics.keyspace.RepairSyncTime.%s",
        "org.apache.cassandra.metrics.keyspace.RepairTime.%s",
        "org.apache.cassandra.metrics.keyspace.RepairedDataInconsistenciesConfirmed.%s",
        "org.apache.cassandra.metrics.keyspace.RepairedDataInconsistenciesUnconfirmed.%s",
        "org.apache.cassandra.metrics.keyspace.RepairedDataTrackingOverreadRows.%s",
        "org.apache.cassandra.metrics.keyspace.RepairedDataTrackingOverreadTime.%s",
        "org.apache.cassandra.metrics.keyspace.RowIndexSize.%s",
        "org.apache.cassandra.metrics.keyspace.RowIndexSizeAborts.%s",
        "org.apache.cassandra.metrics.keyspace.RowIndexSizeWarnings.%s",
        "org.apache.cassandra.metrics.keyspace.SSTablesPerRangeReadHistogram.%s",
        "org.apache.cassandra.metrics.keyspace.SSTablesPerReadHistogram.%s",
        "org.apache.cassandra.metrics.keyspace.SpeculativeFailedRetries.%s",
        "org.apache.cassandra.metrics.keyspace.SpeculativeInsufficientReplicas.%s",
        "org.apache.cassandra.metrics.keyspace.SpeculativeRetries.%s",
        "org.apache.cassandra.metrics.keyspace.TombstoneScannedHistogram.%s",
        "org.apache.cassandra.metrics.keyspace.TooManySSTableIndexesReadAborts.%s",
        "org.apache.cassandra.metrics.keyspace.TooManySSTableIndexesReadWarnings.%s",
        "org.apache.cassandra.metrics.keyspace.TotalDiskSpaceUsed.%s",
        "org.apache.cassandra.metrics.keyspace.UncompressedLiveDiskSpaceUsed.%s",
        "org.apache.cassandra.metrics.keyspace.UnreplicatedLiveDiskSpaceUsed.%s",
        "org.apache.cassandra.metrics.keyspace.UnreplicatedUncompressedLiveDiskSpaceUsed.%s",
        "org.apache.cassandra.metrics.keyspace.ValidationTime.%s",
        "org.apache.cassandra.metrics.keyspace.ViewLockAcquireTime.%s",
        "org.apache.cassandra.metrics.keyspace.ViewReadTime.%s",
        "org.apache.cassandra.metrics.keyspace.WriteFailedIdealCL.%s",
        "org.apache.cassandra.metrics.keyspace.WriteLatency.%s",
        "org.apache.cassandra.metrics.keyspace.WriteOutOfRangeToken.%s",
        "org.apache.cassandra.metrics.keyspace.WriteTotalLatency.%s" };
}
