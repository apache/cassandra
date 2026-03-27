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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * A message sink that all outbound messages go through.
 *
 * Default sink {@link Sink} used by {@link MessagingService} is {@link MessagingService#doSend(Message, InetAddressAndPort, ConnectionType)}, which proceeds to
 * send messages over the network, but it can be overridden to filter out certain messages, record the fact
 * of attempted delivery, or delay they delivery.
 *
 * This facility is most useful for test code.
 */
public class OutboundSink
{
    public interface Sink
    {
        void accept(Message<?> message, InetAddressAndPort to, ConnectionType connectionType);
    }

    public interface Filter
    {
        public boolean test(Message<?> message, InetAddressAndPort to, ConnectionType type);
    }

    public interface AsyncFilter
    {
        void filter(Message<?> message, InetAddressAndPort to, ConnectionType type, Sink next);
    }

    private static abstract class AbstractFiltered implements Sink
    {
        final Sink next;

        private AbstractFiltered(Sink next)
        {
            this.next = next;
        }

        abstract AbstractFiltered withNext(Sink next);
    }

    private static class Filtered extends AbstractFiltered
    {
        final Filter condition;

        private Filtered(Filter condition, Sink next)
        {
            super(next);
            this.condition = condition;
        }

        public void accept(Message<?> message, InetAddressAndPort to, ConnectionType connectionType)
        {
            if (condition.test(message, to, connectionType))
                next.accept(message, to, connectionType);
        }

        @Override
        AbstractFiltered withNext(Sink next)
        {
            return new Filtered(condition, next);
        }
    }

    private static class AsyncFiltered extends AbstractFiltered
    {
        final AsyncFilter filter;

        private AsyncFiltered(AsyncFilter filter, Sink next)
        {
            super(next);
            this.filter = filter;
        }

        public void accept(Message<?> message, InetAddressAndPort to, ConnectionType connectionType)
        {
            filter.filter(message, to, connectionType, next);
        }

        @Override
        AbstractFiltered withNext(Sink next)
        {
            return new AsyncFiltered(filter, next);
        }
    }

    private volatile Sink sink;
    private static final AtomicReferenceFieldUpdater<OutboundSink, Sink> sinkUpdater
        = AtomicReferenceFieldUpdater.newUpdater(OutboundSink.class, Sink.class, "sink");

    OutboundSink(Sink sink)
    {
        this.sink = sink;
    }

    public void accept(Message<?> message, InetAddressAndPort to, ConnectionType connectionType)
    {
        sink.accept(message, to, connectionType);
    }

    public void add(Filter allow)
    {
        sinkUpdater.updateAndGet(this, sink -> new Filtered(allow, sink));
    }

    public void remove(Filter allow)
    {
        sinkUpdater.updateAndGet(this, sink -> without(sink, allow));
    }

    public void add(AsyncFilter filter)
    {
        sinkUpdater.updateAndGet(this, sink -> new AsyncFiltered(filter, sink));
    }

    public void remove(AsyncFilter filter)
    {
        sinkUpdater.updateAndGet(this, sink -> without(sink, filter));
    }

    public void clear()
    {
        sinkUpdater.updateAndGet(this, OutboundSink::clear);
    }

    private static Sink clear(Sink sink)
    {
        while (sink instanceof OutboundSink.Filtered)
            sink = ((OutboundSink.Filtered) sink).next;
        return sink;
    }

    private static Sink without(Sink sink, Filter condition)
    {
        return without(sink, f -> f instanceof Filtered && condition.equals(((Filtered) f).condition));
    }

    private static Sink without(Sink sink, AsyncFilter filter)
    {
        return without(sink, f -> f instanceof AsyncFiltered && filter.equals(((AsyncFiltered) f).filter));
    }

    private static Sink without(Sink sink, Predicate<AbstractFiltered> remove)
    {
        if (!(sink instanceof AbstractFiltered))
            return sink;

        AbstractFiltered filtered = (AbstractFiltered) sink;
        if (remove.test(filtered))
            return filtered.next;

        Sink next = without(filtered.next, remove);
        if (next == filtered.next)
            return filtered;
        return filtered.withNext(next);
    }
}
