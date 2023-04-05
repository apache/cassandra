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
import java.util.function.BiPredicate;

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

    private static class Filtered implements Sink
    {
        final BiPredicate<Message<?>, InetAddressAndPort> condition;
        final Sink next;

        private Filtered(BiPredicate<Message<?>, InetAddressAndPort> condition, Sink next)
        {
            this.condition = condition;
            this.next = next;
        }

        public void accept(Message<?> message, InetAddressAndPort to, ConnectionType connectionType)
        {
            if (condition.test(message, to))
                next.accept(message, to, connectionType);
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

    public void add(BiPredicate<Message<?>, InetAddressAndPort> allow)
    {
        sinkUpdater.updateAndGet(this, sink -> new Filtered(allow, sink));
    }

    public void remove(BiPredicate<Message<?>, InetAddressAndPort> allow)
    {
        sinkUpdater.updateAndGet(this, sink -> without(sink, allow));
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

    private static Sink without(Sink sink, BiPredicate<Message<?>, InetAddressAndPort> condition)
    {
        if (!(sink instanceof Filtered))
            return sink;

        Filtered filtered = (Filtered) sink;
        Sink next = without(filtered.next, condition);
        return condition.equals(filtered.condition) ? next
                                                    : next == filtered.next
                                                      ? sink
                                                      : new Filtered(filtered.condition, next);
    }

}
