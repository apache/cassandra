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

package org.apache.cassandra.harry.stress;

import java.util.concurrent.atomic.AtomicLong;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricCollector
{
    public static final Logger LOGGER = LoggerFactory.getLogger(MetricCollector.class);
    private final Histogram reads = new Histogram(3);
    private final Histogram writes = new Histogram(3);

    // nanos
    private long startNs = Long.MAX_VALUE;
    private long endNs = Long.MIN_VALUE;

    // discrete
    public final AtomicLong activePartitionCount = new AtomicLong();
    public final AtomicLong partitionCount = new AtomicLong();
    public final AtomicLong failedReads = new AtomicLong();
    public final AtomicLong failedWrites = new AtomicLong();

    public void merge(StressWorker.WorkerMetrics metrics)
    {
        try
        {
            reads.add(metrics.reads);
            writes.add(metrics.writes);
            failedReads.addAndGet(metrics.failedReads.get());
            failedWrites.addAndGet(metrics.failedWrites.get());
        }
        catch (Throwable t)
        {
            LOGGER.error("Could not merge histograms, reported values will be unreliable", t);

        }
    }

    public synchronized double readRate()
    {
        return readCount() / ((endNs - startNs) * 0.000000001d);
    }

    public synchronized double writeRate()
    {
        return writeCount() / ((endNs - startNs) * 0.000000001d);
    }

    public synchronized double partitionRate()
    {
        return partitionCount.get() / ((endNs - startNs) * 0.000000001d);
    }

    public synchronized double meanReadLatencyMs()
    {
        return readLatencies().getMean() * 0.000001d;
    }

    public synchronized double maxReadLatencyMs()
    {
        return readLatencies().getMaxValue() * 0.000001d;
    }

    public synchronized double medianReadLatencyMs()
    {
        return readLatencies().getValueAtPercentile(50.0) * 0.000001d;
    }

    public synchronized double meanWriteLatencyMs()
    {
        return writeLatencies().getMean() * 0.000001d;
    }

    public synchronized double maxWriteLatencyMs()
    {
        return writeLatencies().getMaxValue() * 0.000001d;
    }

    public synchronized double medianWriteLatencyMs()
    {
        return writeLatencies().getValueAtPercentile(50.0) * 0.000001d;
    }


    /**
     * @param percentile between 0.0 and 100.0
     * @return latency in milliseconds at percentile
     */
    public synchronized double readLatencyAtPercentileMs(double percentile)
    {
        return readLatencies().getValueAtPercentile(percentile) * 0.000001d;
    }

    public synchronized double writeLatencyAtPercentileMs(double percentile)
    {
        return writeLatencies().getValueAtPercentile(percentile) * 0.000001d;
    }

    public synchronized long runTimeMs()
    {
        return (endNs - startNs) / 1000000;
    }

    public long end()
    {
        return endNs;
    }

    public long start()
    {
        return startNs;
    }

    private Histogram readLatencies()
    {
        return reads;
    }

    private Histogram writeLatencies()
    {
        return writes;
    }

    public synchronized long readCount()
    {
        return readLatencies().getTotalCount() + failedReads.get();
    }

    public synchronized long writeCount()
    {
        return writeLatencies().getTotalCount() + failedWrites.get();
    }


    public void start(long started)
    {
        this.startNs = started;
        readLatencies().reset();
        writeLatencies().reset();
    }

    public void end(long ended)
    {
        this.endNs = ended;
    }
}