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

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;

import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

public class SimulatedMessageDelivery implements MessageDelivery
{
    public enum Action { DELIVER, DELIVER_WITH_FAILURE, DROP, DROP_PARTITIONED, FAILURE }

    public interface ActionSupplier
    {
        Action get(InetAddressAndPort self, Message<?> message, InetAddressAndPort to);
    }

    public interface NetworkDelaySupplier
    {
        @Nullable
        Duration jitter(Message<?> message, InetAddressAndPort to);
    }

    public static NetworkDelaySupplier noDelay()
    {
        return (i1, i2) -> null;
    }

    public static NetworkDelaySupplier randomDelay(RandomSource rs)
    {
        class Connection
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
        final Map<Connection, LongSupplier> networkLatencies = new HashMap<>();
        return (msg, to) -> {
            InetAddressAndPort from = msg.from();
            long delayNanos = networkLatencies.computeIfAbsent(new Connection(from, to), ignore -> {
                long min = TimeUnit.MICROSECONDS.toNanos(500);
                long maxSmall = TimeUnit.MILLISECONDS.toNanos(5);
                long max = TimeUnit.SECONDS.toNanos(5);
                LongSupplier small = () -> rs.nextLong(min, maxSmall);
                LongSupplier large = () -> rs.nextLong(maxSmall, max);
                return Gens.bools().biasedRepeatingRuns(rs.nextInt(1, 11) / 100.0D, rs.nextInt(3, 15))
                           .mapToLong(b -> b ? large.getAsLong() : small.getAsLong())
                           .asLongSupplier(rs.fork());
            }).getAsLong();
            return Duration.ofNanos(delayNanos);
        };
    }

    public interface Scheduler
    {
        void schedule(Runnable command, long delay, TimeUnit unit);
    }

    public interface DropListener
    {
        void onDrop(Action action, InetAddressAndPort from, Message<?> msg);
    }

    private final InetAddressAndPort self;
    private final ActionSupplier actions;
    private final NetworkDelaySupplier networkDelay;
    private final BiConsumer<InetAddressAndPort, Message<?>> reciever;
    private final DropListener onDropped;
    private final Scheduler scheduler;
    private final Consumer<Throwable> onError;
    private final Map<CallbackKey, CallbackContext> callbacks = new HashMap<>();
    private enum Status { Up, Down }
    private Status status = Status.Up;

    public SimulatedMessageDelivery(InetAddressAndPort self,
                                    ActionSupplier actions,
                                    NetworkDelaySupplier networkDelay,
                                    BiConsumer<InetAddressAndPort, Message<?>> reciever,
                                    DropListener onDropped,
                                    Scheduler scheduler,
                                    Consumer<Throwable> onError)
    {
        this.self = self;
        this.actions = actions;
        this.networkDelay = networkDelay;
        this.reciever = reciever;
        this.onDropped = onDropped;
        this.scheduler = scheduler;
        this.onError = onError;
    }

    public void stop()
    {
        callbacks.clear();
        status = Status.Down;
    }

    @Override
    public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
    {
        message = message.withFrom(self);
        maybeEnqueue(message, to, null);
    }

    @Override
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
    {
        message = message.withFrom(self);
        maybeEnqueue(message, to, cb);
    }

    @Override
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
    {
        message = message.withFrom(self);
        maybeEnqueue(message, to, cb);
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
            public void onFailure(InetAddressAndPort from, RequestFailureReason failure)
            {
                promise.tryFailure(new MessagingService.FailureResponseException(from, failure));
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

    private <REQ, RSP> void maybeEnqueue(Message<REQ> message, InetAddressAndPort to, @Nullable RequestCallback<RSP> callback)
    {
        if (status != Status.Up)
            return;
        CallbackContext cb;
        if (callback != null)
        {
            CallbackKey key = new CallbackKey(message.id(), to);
            if (callbacks.containsKey(key))
                throw new AssertionError("Message id " + message.id() + " to " + to + " already has a callback");
            cb = new CallbackContext(callback);
            callbacks.put(key, cb);
        }
        else
        {
            cb = null;
        }
        Action action = actions.get(self, message, to);
        switch (action)
        {
            case DELIVER:
                deliver(message, to);
                break;
            case DROP:
            case DROP_PARTITIONED:
                onDropped.onDrop(action, to, message);
                break;
            case DELIVER_WITH_FAILURE:
                deliver(message, to);
            case FAILURE:
                if (action == Action.FAILURE)
                    onDropped.onDrop(action, to, message);
                if (callback != null)
                    scheduler.schedule(() -> callback.onFailure(to, RequestFailureReason.UNKNOWN),
                                       message.verb().expiresAfterNanos(), TimeUnit.NANOSECONDS);
                return;
            default:
                throw new UnsupportedOperationException("Unknown action type: " + action);
        }
        if (cb != null)
        {
            scheduler.schedule(() -> {
                CallbackContext ctx = callbacks.remove(new CallbackKey(message.id(), to));
                if (ctx != null)
                {
                    assert ctx == cb;
                    try
                    {
                        ctx.onFailure(to, RequestFailureReason.TIMEOUT);
                    }
                    catch (Throwable t)
                    {
                        onError.accept(t);
                    }
                }
            }, message.verb().expiresAfterNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private void deliver(Message<?> message, InetAddressAndPort to)
    {
        Duration delay = networkDelay.jitter(message, to);
        if (delay == null) reciever.accept(to, message);
        else               scheduler.schedule(() -> reciever.accept(to, message), delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    @SuppressWarnings("rawtypes")
    public SimulatedMessageReceiver receiver(IVerbHandler onMessage)
    {
        return new SimulatedMessageReceiver(onMessage);
    }

    public class SimulatedMessageReceiver
    {
        @SuppressWarnings("rawtypes")
        final IVerbHandler onMessage;

        @SuppressWarnings("rawtypes")
        public SimulatedMessageReceiver(IVerbHandler onMessage)
        {
            this.onMessage = onMessage;
        }

        public void recieve(Message<?> msg)
        {
            if (status != Status.Up)
                return;
            if (msg.verb().isResponse())
            {
                CallbackKey key = new CallbackKey(msg.id(), msg.from());
                if (callbacks.containsKey(key))
                {
                    CallbackContext callback = callbacks.remove(key);
                    if (callback == null)
                        return;
                    try
                    {
                        if (msg.isFailureResponse())
                            callback.onFailure(msg.from(), (RequestFailureReason) msg.payload);
                        else callback.onResponse(msg);
                    }
                    catch (Throwable t)
                    {
                        onError.accept(t);
                    }
                }
            }
            else
            {
                try
                {
                    //noinspection unchecked
                    onMessage.doVerb(msg);
                }
                catch (Throwable t)
                {
                    onError.accept(t);
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static class SimpleVerbHandler implements IVerbHandler
    {
        private final Map<Verb, IVerbHandler<?>> handlers;

        public SimpleVerbHandler(Map<Verb, IVerbHandler<?>> handlers)
        {
            this.handlers = handlers;
        }

        @Override
        public void doVerb(Message msg) throws IOException
        {
            IVerbHandler<?> handler = handlers.get(msg.verb());
            if (handler == null)
                throw new AssertionError("Unexpected verb: " + msg.verb());
            //noinspection unchecked
            handler.doVerb(msg);
        }
    }

    private static class CallbackContext
    {
        @SuppressWarnings("rawtypes")
        final RequestCallback callback;

        @SuppressWarnings("rawtypes")
        private CallbackContext(RequestCallback callback)
        {
            this.callback = Objects.requireNonNull(callback);
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public void onResponse(Message msg)
        {
            callback.onResponse(msg);
        }

        public void onFailure(InetAddressAndPort from, RequestFailureReason failure)
        {
            if (callback.invokeOnFailure()) callback.onFailure(from, failure);
        }
    }

    private static class CallbackKey
    {
        private final long id;
        private final InetAddressAndPort peer;

        private CallbackKey(long id, InetAddressAndPort peer)
        {
            this.id = id;
            this.peer = peer;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CallbackKey that = (CallbackKey) o;
            return id == that.id && peer.equals(that.peer);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, peer);
        }

        @Override
        public String toString()
        {
            return "CallbackKey{" +
                   "id=" + id +
                   ", peer=" + peer +
                   '}';
        }
    }
}
