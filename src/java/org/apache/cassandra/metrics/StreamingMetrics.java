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
package org.apache.cassandra.metrics;

import java.util.concurrent.ConcurrentMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for streaming.
 */
public class StreamingMetrics
{
    public static final String TYPE_NAME = "Streaming";

    private static final ConcurrentMap<InetAddressAndPort, StreamingMetrics> instances = new NonBlockingHashMap<>();

    /** @deprecated See CASSANDRA-11174 */
    @Deprecated(since = "4.0")
    public static final Counter activeStreamsOutbound = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "ActiveOutboundStreams", null));
    public static final Counter totalIncomingBytes = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalIncomingBytes", null));
    public static final Counter totalOutgoingBytes = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalOutgoingBytes", null));
    public static final Counter totalOutgoingRepairBytes = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalOutgoingRepairBytes", null));
    public static final Counter totalOutgoingRepairSSTables = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalOutgoingRepairSSTables", null));
    // The counters below cover partial-stream slicing on the entire-sstable (zero-copy) path: sending a synthesised
    // chunk-run slice covering only the requested token ranges instead of a whole sstable, without deserialising
    // rows. The price is bytes no read can reach - the head of the slice's first compression chunk, anything between
    // sections less than a chunk apart, the final chunk's tail past the last live byte - and
    // zero_copy_partial_stream_max_dead_space_ratio is what caps it.
    /** Sstables sent as a slice. Not the per-peer {@code PartialSSTablesStreamedIn}, which counts receives. */
    public static final Counter slicedZeroCopyStreamsOut = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "SlicedZeroCopyStreamsOut", null));
    /** Slices that were planned but could not be synthesised, so the stream failed. */
    public static final Counter slicedZeroCopyStreamsFailed = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "SlicedZeroCopyStreamsFailed", null));
    /**
     * Sliceable transfers sent partition-by-partition instead. Not a failure. It is the sum of the two counters
     * below, which say whether configuration could have made them slices; increment it only through
     * {@link #countSliceRefusedByDeadSpaceRatio()} and {@link #countSliceRefusedAsUnsliceable()} so it keeps
     * matching them.
     */
    public static final Counter slicedZeroCopyStreamsRefused = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "SlicedZeroCopyStreamsRefused", null));
    /**
     * The subset of {@link #slicedZeroCopyStreamsRefused} that {@code zero_copy_partial_stream_max_dead_space_ratio}
     * alone refused: the slice was expressible and would have carried more dead space than the ratio permits.
     * Raising the ratio accepts these, and this is the ratio's only observable effect -- set too low it otherwise
     * just leaves {@link #slicedZeroCopyStreamsOut} at zero, with the reason at DEBUG.
     */
    public static final Counter slicedZeroCopyStreamsRefusedDeadSpace = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "SlicedZeroCopyStreamsRefusedDeadSpace", null));
    /**
     * The subset of {@link #slicedZeroCopyStreamsRefused} that no configuration would change: the sstable's format,
     * version, bloom filter or compression dictionary, index components a slice cannot synthesise, legacy counter
     * shards, a request shape the arithmetic cannot express, or too little room on the sender for the synthesised
     * components. On a table where slicing is structurally impossible this equals
     * {@link #slicedZeroCopyStreamsRefused} for ever, which is what tells it apart from a ratio set too low.
     */
    public static final Counter slicedZeroCopyStreamsRefusedUnsliceable = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "SlicedZeroCopyStreamsRefusedUnsliceable", null));
    /**
     * Unreachable bytes those slices carried, counted UNCOMPRESSED -- unlike {@link #totalOutgoingBytes} and
     * {@link #totalOutgoingRepairBytes} in this same MBean type, which are wire bytes, so for a COMPRESSED table
     * dividing one by the other is wrong by the compression ratio. There is no honest wire figure to report instead:
     * a dead region shares its compression chunk with live bytes and so has no compressed size of its own. The two
     * units coincide for an uncompressed sstable.
     */
    public static final Counter slicedZeroCopyStreamsDeadBytes = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "SlicedZeroCopyStreamsDeadBytes", null));
    public final Counter incomingBytes;
    public final Counter outgoingBytes;
    /* Measures the time taken for processing the incoming stream message after being deserialized, including the time to flush to disk. */
    public final Timer incomingProcessTime;
    private final Counter entireSSTablesStreamedIn;
    private final Counter partialSSTablesStreamedIn;

    public static StreamingMetrics get(InetAddressAndPort ip)
    {
       /*
         computeIfAbsent doesn't work for this situation. Since JMX metrics register themselves in their ctor, we need
         to create the metric exactly once, otherwise we'll get duplicate name exceptions. Although computeIfAbsent is
         thread safe in the context of the map, it uses compare and swap to add the computed value to the map. This
         means it eagerly allocates new metric instances, which can cause the jmx name collision we're trying to avoid
         if multiple calls interleave. So here we use synchronized to ensure we only instantiate metrics exactly once.
        */
       StreamingMetrics metrics = instances.get(ip);
       if (metrics == null)
       {
           synchronized (instances)
           {
               metrics = instances.get(ip);
               if (metrics == null)
               {
                   metrics = new StreamingMetrics(ip);
                   instances.put(ip, metrics);
               }
           }
       }
       return metrics;
    }

    public StreamingMetrics(final InetAddressAndPort peer)
    {
        MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME, peer.toString().replace(':', '.'));
        incomingBytes = Metrics.counter(factory.createMetricName("IncomingBytes"));
        outgoingBytes= Metrics.counter(factory.createMetricName("OutgoingBytes"));
        incomingProcessTime = Metrics.timer(factory.createMetricName("IncomingProcessTime"));

        entireSSTablesStreamedIn = Metrics.counter(factory.createMetricName("EntireSSTablesStreamedIn"));
        partialSSTablesStreamedIn = Metrics.counter(factory.createMetricName("PartialSSTablesStreamedIn"));
    }

    public void countStreamedIn(boolean isEntireSSTable)
    {
        (isEntireSSTable ? entireSSTablesStreamedIn : partialSSTablesStreamedIn).inc();
    }

    /**
     * Counts a transfer that could have gone as a slice but was sent partition-by-partition because
     * {@code zero_copy_partial_stream_max_dead_space_ratio} refused it.
     */
    public static void countSliceRefusedByDeadSpaceRatio()
    {
        slicedZeroCopyStreamsRefused.inc();
        slicedZeroCopyStreamsRefusedDeadSpace.inc();
    }

    /**
     * Counts a transfer that was sent partition-by-partition for a reason no configuration would change; see
     * {@link #slicedZeroCopyStreamsRefusedUnsliceable}.
     */
    public static void countSliceRefusedAsUnsliceable()
    {
        slicedZeroCopyStreamsRefused.inc();
        slicedZeroCopyStreamsRefusedUnsliceable.inc();
    }
}
