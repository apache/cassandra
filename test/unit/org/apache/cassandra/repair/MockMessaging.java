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

package org.apache.cassandra.repair;

import accord.utils.Gens;
import accord.utils.RandomSource;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.SharedContext;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

public class MockMessaging<Node extends SharedContext> implements MessageDelivery
{
    private static final Logger logger = LoggerFactory.getLogger(MockMessaging.class);

    public enum Faults
    {
        DELAY, DROP;

        public static final Set<Faults> NONE = Collections.emptySet();
        public static final Set<Faults> DROPPED = EnumSet.of(DELAY, DROP);
    }

    interface MessageListener<Node extends SharedContext>
    {
        void preHandle(Node node, Message<?> msg);
    }

    final Node node;
    final RandomSource rs;
    final Consumer<Throwable> onError;
    final Function<Verb, IVerbHandler<?>> verbToHandler;
    final Function<InetAddressAndPort, MockMessaging<Node>> messagingLookup;
    final EndpointMessagingVersions versions = new EndpointMessagingVersions();
    final Long2ObjectHashMap<CallbackContext> callbacks = new Long2ObjectHashMap<>();
    // TODO (nice to have): push this to the caller, so latency/drops can be time based and not run based
    private final Map<Connection, LongSupplier> networkLatencies = new HashMap<>();
    private final Map<Connection, Supplier<Boolean>> networkDrops = new HashMap<>();
    private Function<Message<?>, Set<Faults>> allowedMessageFaults = ignore -> Collections.emptySet();
    private List<MessageListener<Node>> messageListeners = new ArrayList<>();
    private boolean shutdown = false;

    public MockMessaging(Node node,
                         Consumer<Throwable> onError,
                         Function<Verb, IVerbHandler<?>> verbToHandler,
                         Function<InetAddressAndPort, MockMessaging<Node>> messagingLookup)
    {
        this.node = node;
        this.rs = RandomSource.wrap(node.random().get());
        this.onError = onError;
        this.verbToHandler = verbToHandler;
        this.messagingLookup = messagingLookup;
    }

    public void allowedMessageFaults(Function<Message<?>, Set<Faults>> allowedMessageFaults)
    {
        this.allowedMessageFaults = allowedMessageFaults;
    }

    public void addListener(MessageListener<Node> listener)
    {
        messageListeners.add(listener);
    }

    @Override
    public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
    {
        message = message.withFrom(node.broadcastAddressAndPort());
        maybeEnqueue(message, to, null);
    }

    @Override
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
    {
        message = message.withFrom(node.broadcastAddressAndPort());
        maybeEnqueue(message, to, cb);
    }

    @Override
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
    {
        message = message.withFrom(node.broadcastAddressAndPort());
        maybeEnqueue(message, to, cb);
    }

    public void handleCallback(Message<?> msg)
    {
        if (callbacks.containsKey(msg.id()))
        {
            CallbackContext callback = callbacks.remove(msg.id());
            if (callback == null) return;
            if (msg.isFailureResponse()) callback.onFailure(msg.from(), (RequestFailureReason) msg.payload);
            else callback.onResponse(msg);
        }
    }

    public void handle(Message<?> msg)
    {
        msg = serde(msg);
        if (msg == null)
        {
            logger.warn("Got a message that failed to serialize/deserialize");
            return;
        }
        for (MessageListener<Node> l : messageListeners)
            l.preHandle(node, msg);
        try
        {
            if (msg.verb().isResponse()) handleCallback(msg);
            else handler(msg).doVerb((Message<Object>) msg);
        }
        catch (Throwable t)
        {
            onError.accept(t);
        }
    }

    private <T> IVerbHandler<T> handler(Message<?> msg)
    {
        return (IVerbHandler<T>) verbToHandler.apply(msg.verb());
    }

    private Message serde(Message msg)
    {
        try (DataOutputBuffer b = DataOutputBuffer.scratchBuffer.get())
        {
            int messagingVersion = MessagingService.Version.CURRENT.value;
            Message.serializer.serialize(msg, b, messagingVersion);
            DataInputBuffer in = new DataInputBuffer(b.unsafeGetBufferAndFlip(), false);
            return Message.serializer.deserialize(in, msg.from(), messagingVersion);
        }
        catch (Throwable e)
        {
            onError.accept(e);
            return null;
        }
    }

    private <REQ, RSP> void maybeEnqueue(Message<REQ> message, InetAddressAndPort to, @Nullable RequestCallback<RSP> callback)
    {
        //  MessagingService drops all messages when shutdown...
        if (shutdown) return;
        CallbackContext cb;
        if (callback != null)
        {
            cb = new CallbackContext(callback);
            callbacks.put(message.id(), cb);
        }
        else
        {
            cb = null;
        }
        boolean toSelf = node.broadcastAddressAndPort().equals(to);
        MockMessaging<Node> peer = messagingLookup.apply(to);
        if (peer == null)
        {
            // was removed, ignore it
            return;
        }
        Set<Faults> allowedFaults = allowedMessageFaults.apply(message);
        if (allowedFaults.isEmpty())
        {
            // enqueue so stack overflow doesn't happen with the inlining
            peer.node.optionalTasks().submit(() -> peer.handle(message));
        }
        else
        {
            Runnable enqueue = () -> {
                if (!allowedFaults.contains(Faults.DELAY))
                {
                    peer.node.optionalTasks().submit(() -> peer.handle(message));
                }
                else
                {
                    if (toSelf) peer.node.optionalTasks().submit(() -> peer.handle(message));
                    else
                        peer.node.optionalTasks().schedule(() -> peer.handle(message), networkJitterNanos(to), TimeUnit.NANOSECONDS);
                }
            };

            if (!allowedFaults.contains(Faults.DROP)) enqueue.run();
            else
            {
                if (!toSelf && networkDrops(to))
                {
                    // dropped
                }
                else
                {
                    enqueue.run();
                }
            }

            if (cb != null)
            {
                peer.node.optionalTasks().schedule(() -> {
                    CallbackContext ctx = callbacks.remove(message.id());
                    if (ctx != null)
                    {
                        assert ctx == cb;
                        ctx.onFailure(to, RequestFailureReason.TIMEOUT);
                    }
                }, message.verb().expiresAfterNanos(), TimeUnit.NANOSECONDS);
            }
        }
    }

    private long networkJitterNanos(InetAddressAndPort to)
    {
        return networkLatencies.computeIfAbsent(new Connection(node.broadcastAddressAndPort(), to), ignore -> {
            long min = TimeUnit.MICROSECONDS.toNanos(500);
            long maxSmall = TimeUnit.MILLISECONDS.toNanos(5);
            long max = TimeUnit.SECONDS.toNanos(5);
            LongSupplier small = () -> rs.nextLong(min, maxSmall);
            LongSupplier large = () -> rs.nextLong(maxSmall, max);
            return Gens.bools().runs(rs.nextInt(1, 11) / 100.0D, rs.nextInt(3, 15)).mapToLong(b -> b ? large.getAsLong() : small.getAsLong()).asLongSupplier(rs);
        }).getAsLong();
    }

    private boolean networkDrops(InetAddressAndPort to)
    {
        return networkDrops.computeIfAbsent(new Connection(node.broadcastAddressAndPort(), to), ignore -> Gens.bools().runs(rs.nextInt(1, 11) / 100.0D, rs.nextInt(3, 15)).asSupplier(rs)).get();
    }

    @Override
    public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
    {
        AsyncPromise<Message<RSP>> promise = new AsyncPromise<>();
        sendWithCallback(message, to, new RequestCallback<RSP>()
        {
            @Override
            public void onResponse(Message<RSP> msg)
            {
                promise.trySuccess(msg);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                promise.tryFailure(new MessagingService.FailureResponseException(from, failureReason));
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }
        });
        return promise;
    }

    @Override
    public <V> void respond(V response, Message<?> message)
    {
        send(message.responseWith(response), message.respondTo());
    }

    @Override
    public void listen()
    {
        shutdown = false;
    }

    @Override
    public void waitUntilListening()
    {
        listen();
    }

    @Override
    public EndpointMessagingVersions versions()
    {
        return versions;
    }

    @Override
    public void closeOutbound(InetAddressAndPort to)
    {
        // TODO (coverage): remove pending messages; this logic works on queued messages, but this class doesn't diferenciat between queued to the NIC and on the NIC... so not trivial to implement 
    }

    @Override
    public void interruptOutbound(InetAddressAndPort to)
    {
        // TODO (coverage): remove pending messages; this logic works on queued messages, but this class doesn't diferenciat between queued to the NIC and on the NIC... so not trivial to implement
    }

    @Override
    public void removeInbound(InetAddressAndPort from)
    {
        // TODO (coverage): remove pending messages; this logic works on queued messages, but this class doesn't diferenciat between queued to the NIC and on the NIC... so not trivial to implement
    }

    @Override
    public void shutdown()
    {
        shutdown = true;
    }

    private static class CallbackContext
    {
        final RequestCallback callback;

        private CallbackContext(RequestCallback callback)
        {
            this.callback = Objects.requireNonNull(callback);
        }

        public void onResponse(Message msg)
        {
            callback.onResponse(msg);
        }

        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            if (callback.invokeOnFailure()) callback.onFailure(from, failureReason);
        }
    }

    private static class Connection
    {
        final InetAddressAndPort from, to;

        private Connection(InetAddressAndPort from, InetAddressAndPort to)
        {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Connection that = (Connection) o;
            return from.equals(that.from) && to.equals(that.to);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(from, to);
        }

        @Override
        public String toString()
        {
            return "Connection{" + "from=" + from + ", to=" + to + '}';
        }
    }
}
