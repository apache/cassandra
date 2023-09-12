/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionPick;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MovingAverage;
import org.apache.cassandra.utils.PageAware;

/**
 * An implementation of {@link Environment} that returns
 * real values.
 */
class RealEnvironment implements Environment
{
    private final CompactionRealm realm;

    RealEnvironment(CompactionRealm realm)
    {
        assert realm != null;
        this.realm = realm;
    }

    private TableMetrics metrics()
    {
        return realm.metrics();
    }

    @Override
    public MovingAverage makeExpMovAverage()
    {
        return ExpMovingAverage.decayBy100();
    }

    @Override
    public double cacheMissRatio()
    {
        double hitRate = ChunkCache.instance.metrics.hitRate();
        if (Double.isNaN(hitRate))
            return 1; // if the cache is not yet initialized then assume all requests are a cache miss

        return 1 - Math.min(1, hitRate); // hit rate should never be > 1 but just in case put a check
    }

    @Override
    public double bloomFilterFpRatio()
    {
        return metrics() == null ? 0.0 : metrics().bloomFilterFalseRatio.getValue();
    }

    @Override
    public int chunkSize()
    {
        CompressionParams compressionParams = realm.metadata().params.compression;
        if (compressionParams.isEnabled())
            return compressionParams.chunkLength();

        return PageAware.PAGE_SIZE;
    }

    @Override
    public long partitionsRead()
    {
        return metrics() == null ? 0 : metrics().readRequests.getCount();
    }

    @Override
    public double sstablePartitionReadLatencyNanos()
    {
        return metrics() == null ? 0.0 : metrics().sstablePartitionReadLatency.get();
    }

    @Override
    public double compactionTimePerKbInNanos()
    {
        return metrics() == null ? 0.0 : metrics().compactionTimePerKb.get();
    }

    @Override
    public double flushTimePerKbInNanos()
    {
        return metrics() == null ? 0.0 : metrics().flushTimePerKb.get();
    }

    @Override
    public long bytesInserted()
    {
        return metrics() == null ? 0 : metrics().bytesInserted.getCount();
    }

    @Override
    public double WA()
    {
        return realm.getWA();
    }

    @Override
    public double flushSize()
    {
        return metrics() == null ? 0.0 : metrics().flushSizeOnDisk().get();
    }

    @Override
    public int maxConcurrentCompactions()
    {
        return DatabaseDescriptor.getConcurrentCompactors();
    }

    @Override
    public double maxThroughput()
    {
        final int compactionThroughputMbPerSec = DatabaseDescriptor.getCompactionThroughputMbPerSec();
        if (compactionThroughputMbPerSec <= 0)
            return Double.MAX_VALUE;
        return compactionThroughputMbPerSec * 1024.0 * 1024.0;
    }

    @Override
    public long getOverheadSizeInBytes(CompactionPick compactionPick)
    {
        // The estimate the compaction overhead to be the same as the size of the input sstables
        return compactionPick.totSizeInBytes();
    }

    @Override
    public String toString()
    {
        return String.format("Default Environment for %s - Read latency: %d us / partition, flush latency: %d us / KiB, " +
                             "compaction latency: %d us / KiB, bfpr: %f, measured WA: %.2f, flush size %s",
                             realm.metadata(),
                             TimeUnit.NANOSECONDS.toMicros((long) sstablePartitionReadLatencyNanos()),
                             TimeUnit.NANOSECONDS.toMicros((long) flushTimePerKbInNanos()),
                             TimeUnit.NANOSECONDS.toMicros((long) compactionTimePerKbInNanos()),
                             bloomFilterFpRatio(),
                             WA(),
                             FBUtilities.prettyPrintMemory((long)flushSize()));
    }
}
