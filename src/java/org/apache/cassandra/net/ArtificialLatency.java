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

package org.apache.cassandra.net;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.agrona.collections.Object2LongHashMap;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SystemThreadTag.DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.config.CassandraRelevantProperties.ARTIFICIAL_LATENCY_LIMIT;
import static org.apache.cassandra.net.MessagingService.instance;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;

/*
 * Mechanism to delay the sending of messages to peers
 */
public class ArtificialLatency extends ExecutorLocals.Impl
{
    private static volatile Set<Verb> artificialLatencyVerbs;
    private static volatile boolean artificialLatencyOnlyPermittedConsistencyLevels = true;
    private static volatile ToLongFunction<InetAddressAndPort> artificialLatencyNanos;
    private static String artificialLatencies;

    private static Sink running;

    static
    {
        setArtificialLatencyVerbs(CassandraRelevantProperties.ARTIFICIAL_LATENCY_VERBS.getString());
        String latencies = CassandraRelevantProperties.ARTIFICIAL_LATENCIES.getString();
        String unsafeLatencies = CassandraRelevantProperties.ARTIFICIAL_LATENCIES_UNSAFE.getString();
        if (latencies != null) setArtificialLatencies(latencies);
        else if (unsafeLatencies != null) unsafeSetArtificialLatencies(unsafeLatencies);
        if (artificialLatencyNanos != null && !artificialLatencyVerbs.isEmpty())
            setEnabled(true);
    }

    // ensure initialised
    public static void touch() {}

    public static synchronized boolean isEnabled()
    {
        return running != null;
    }

    public static synchronized void setEnabled(boolean enabled)
    {
        if (enabled) start();
        else stop();
    }

    public static synchronized void start()
    {
        if (running == null)
            running = Sink.start();
    }

    public static synchronized void stop()
    {
        if (running != null)
        {
            running.stop();
            running = null;
        }
    }

    public static boolean isEligibleForArtificialLatency()
    {
        return ExecutorLocals.current().eligibleForArtificialLatency;
    }

    public static void setEligibleForArtificialLatency(boolean eligibleForArtificialLatency)
    {
        ExecutorLocals current = ExecutorLocals.current();
        set(current.traceState, current.clientWarnState, eligibleForArtificialLatency);
    }

    static class Sink implements OutboundSink.Filter, Interruptible.Task
    {
        final Interruptible executor = executorFactory().infiniteLoop("ArtificialLatency", this, SAFE, DAEMON, UNSYNCHRONIZED);

        static Sink start()
        {
            Sink sink = new Sink();
            instance().outboundSink.add(sink);
            return sink;
        }

        void stop()
        {
            isShutdown = true;
            instance().outboundSink.remove(this);
            executor.shutdownNow();
            try
            {
                executor.awaitTermination(1, TimeUnit.DAYS);
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
        }

        static class Delayed implements Comparable<Delayed>
        {
            final Message<?> message;
            final InetAddressAndPort to;
            final ConnectionType type;
            final long deadline;

            Delayed(Message<?> message, InetAddressAndPort to, ConnectionType type, long deadline)
            {
                this.message = message;
                this.to = to;
                this.type = type;
                this.deadline = deadline;
            }

            @Override
            public int compareTo(Delayed that)
            {
                return Long.compare(this.deadline, that.deadline);
            }
        }

        volatile boolean isShutdown;

        final BlockingQueue<Delayed> in = newBlockingQueue();
        // messages we have stashed in order to apply an artificial delay
        // note that this queue is not ordered, so that if the artificial delay is modified
        // it may not take effect until the difference between the two delays elapses
        final PriorityQueue<Delayed> out = new PriorityQueue<>();

        @Override
        public boolean test(Message<?> message, InetAddressAndPort to, ConnectionType type)
        {
            if (isShutdown)
                return true;

            if (artificialLatencyOnlyPermittedConsistencyLevels && !message.header.permitsArtificialLatency())
                return true;

            if (!artificialLatencyVerbs.contains(message.verb()))
                return true;

            long deadline = nanoTime() + artificialLatencyNanos.applyAsLong(to);
            Delayed delay = new Delayed(message, to, type, deadline);
            in.add(delay);
            return isShutdown && in.remove(delay);
        }

        public void run(Interruptible.State state) throws InterruptedException
        {
            switch (state)
            {
                default: throw new IllegalStateException();
                case SHUTTING_DOWN:
                {
                    in.drainTo(out);
                    out.forEach(d -> instance().send(d.message, d.to, d.type));
                    return;
                }
                case NORMAL:
                {
                    long blockFor = out.isEmpty()
                                    ? Long.MAX_VALUE
                                    : Math.max(0, out.peek().deadline - nanoTime());

                    Delayed delayed = in.poll(blockFor, TimeUnit.NANOSECONDS);
                    if (delayed != null)
                    {
                        out.add(delayed);
                        in.drainTo(out);
                    }
                }
                case INTERRUPTED:
                {
                    Delayed delayed;
                    long now = nanoTime();
                    while (null != (delayed = out.peek()) && delayed.deadline <= now)
                    {
                        instance().send(delayed.message, delayed.to, delayed.type);
                        out.poll();
                    }
                }
            }
        }
    }

    public static String getArtificialLatencies()
    {
        return artificialLatencies;
    }

    private static long parseNanos(String latency)
    {
        if (!latency.endsWith("ms"))
            throw new IllegalArgumentException("Latency must be specified in terms of milliseconds (with 'ms' suffix)");

        return TimeUnit.MILLISECONDS.toNanos(Long.parseLong(latency.substring(0, latency.length() - 2)));
    }

    public static void setArtificialLatencies(String latencies)
    {
        setArtificialLatencies(latencies, parseNanos(ARTIFICIAL_LATENCY_LIMIT.getString()));
    }

    public static void unsafeSetArtificialLatencies(String latencies)
    {
        setArtificialLatencies(latencies, Long.MAX_VALUE);
    }

    private static synchronized void setArtificialLatencies(String latencies, long nanoLimit)
    {
        if (latencies.indexOf(',') < 0)
        {
            long nanos = parseNanos(latencies);
            if (nanos >= nanoLimit)
                throw new IllegalArgumentException("Artificial latency limit is " + nanoLimit + "ns; tried to set " + nanos + "ns");
            artificialLatencyNanos = ignore -> nanos;
        }
        else
        {
            String[] parse = latencies.split(",");
            Object2LongHashMap<String> dcLatencies = new Object2LongHashMap<>(-1L);
            for (int i = 0 ; i < parse.length ; ++i)
            {
                String[] subparse = parse[i].split(":");
                String dc = subparse[0];
                long nanos = parseNanos(subparse[1]);
                if (nanos >= nanoLimit)
                    throw new IllegalArgumentException("Artificial latency limit is " + nanoLimit + "ns; tried to set " + nanos + "ns");
                dcLatencies.put(dc, nanos);
            }
            artificialLatencyNanos = addr -> {
                Directory directory = ClusterMetadata.current().directory;
                NodeId nodeId = directory.peerId(addr);
                if (nodeId == null)
                    return 0;
                Location location = directory.location(nodeId);
                if (location == null)
                    return 0;
                return dcLatencies.getOrDefault(location.datacenter, 0L);
            };
        }
        artificialLatencies = latencies;
//        artificialLatencyNanos = TimeUnit.MILLISECONDS.toNanos(ms);
    }

    public static String getArtificialLatencyVerbs()
    {
        return artificialLatencyVerbs.stream()
                                     .map(Verb::toString)
                                     .collect(Collectors.joining(","));
    }

    public static boolean getArtificialLatencyOnlyPermittedConsistencyLevels()
    {
        return artificialLatencyOnlyPermittedConsistencyLevels;
    }

    public static void setArtificialLatencyVerbs(String commaDelimitedVerbs)
    {
        if (commaDelimitedVerbs.isEmpty())
            artificialLatencyVerbs = Collections.emptySet();
        else
            artificialLatencyVerbs = Arrays.stream(commaDelimitedVerbs.split(","))
                                           .filter(s -> !s.isEmpty())
                                           .map(s -> {
                                               try
                                               {
                                                   return EnumSet.of(Verb.valueOf(s));
                                               }
                                               catch (IllegalArgumentException iae)
                                               {
                                                   try
                                                   {
                                                       return EnumSet.of(Verb.valueOf(s + "_REQ"), Verb.valueOf(s + "_RSP"));
                                                   }
                                                   catch (IllegalArgumentException ignore) {}
                                                   throw iae;
                                               }
                                           })
                                           .collect(Collector.of(() -> EnumSet.noneOf(Verb.class), Set::addAll, (left, right) -> { left.addAll(right); return left; }));

    }

    public static void setArtificialLatencyOnlyPermittedConsistencyLevels(boolean onlyPermitted)
    {
        artificialLatencyOnlyPermittedConsistencyLevels = onlyPermitted;
    }
}
