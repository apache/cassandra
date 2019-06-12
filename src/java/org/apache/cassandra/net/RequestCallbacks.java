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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.Stage.INTERNAL_RESPONSE;
import static org.apache.cassandra.utils.MonotonicClock.preciseTime;

/**
 * An expiring map of request callbacks.
 *
 * Used to match response (id, peer) pairs to corresponding {@link RequestCallback}s, or, if said responses
 * don't arrive in a timely manner (within verb's timeout), to expire the callbacks.
 *
 * Since we reuse the same request id for multiple messages now, the map is keyed by (id, peer) tuples
 * rather than just id as it used to before 4.0.
 */
public class RequestCallbacks implements OutboundMessageCallbacks
{
    private static final Logger logger = LoggerFactory.getLogger(RequestCallbacks.class);

    private final MessagingService messagingService;
    private final ScheduledExecutorService executor = new DebuggableScheduledThreadPoolExecutor("Callback-Map-Reaper");
    private final ConcurrentMap<CallbackKey, CallbackInfo> callbacks = new ConcurrentHashMap<>();

    RequestCallbacks(MessagingService messagingService)
    {
        this.messagingService = messagingService;

        long expirationInterval = DatabaseDescriptor.getMinRpcTimeout(NANOSECONDS) / 2;
        executor.scheduleWithFixedDelay(this::expire, expirationInterval, expirationInterval, NANOSECONDS);
    }

    /**
     * @return the registered {@link CallbackInfo} for this id and peer, or {@code null} if unset or expired.
     */
    @Nullable
    CallbackInfo get(long id, InetAddressAndPort peer)
    {
        return callbacks.get(key(id, peer));
    }

    /**
     * Remove and return the {@link CallbackInfo} associated with given id and peer, if known.
     */
    @Nullable
    CallbackInfo remove(long id, InetAddressAndPort peer)
    {
        return callbacks.remove(key(id, peer));
    }

    /**
     * Register the provided {@link RequestCallback}, inferring expiry and id from the provided {@link Message}.
     */
    void addWithExpiration(RequestCallback cb, Message message, InetAddressAndPort to)
    {
        // mutations need to call the overload with a ConsistencyLevel
        assert message.verb() != Verb.MUTATION_REQ && message.verb() != Verb.COUNTER_MUTATION_REQ && message.verb() != Verb.PAXOS_COMMIT_REQ;
        CallbackInfo previous = callbacks.put(key(message.id(), to), new CallbackInfo(message, to, cb));
        assert previous == null : format("Callback already exists for id %d/%s! (%s)", message.id(), to, previous);
    }

    // FIXME: shouldn't need a special overload for writes; hinting should be part of AbstractWriteResponseHandler
    public void addWithExpiration(AbstractWriteResponseHandler<?> cb,
                                  Message<?> message,
                                  Replica to,
                                  ConsistencyLevel consistencyLevel,
                                  boolean allowHints)
    {
        assert message.verb() == Verb.MUTATION_REQ || message.verb() == Verb.COUNTER_MUTATION_REQ || message.verb() == Verb.PAXOS_COMMIT_REQ;
        CallbackInfo previous = callbacks.put(key(message.id(), to.endpoint()), new WriteCallbackInfo(message, to, cb, consistencyLevel, allowHints));
        assert previous == null : format("Callback already exists for id %d/%s! (%s)", message.id(), to.endpoint(), previous);
    }

    <T> IVersionedAsymmetricSerializer<?, T> responseSerializer(long id, InetAddressAndPort peer)
    {
        CallbackInfo info = get(id, peer);
        return info == null ? null : info.responseVerb.serializer();
    }

    @VisibleForTesting
    public void removeAndRespond(long id, InetAddressAndPort peer, Message message)
    {
        CallbackInfo ci = remove(id, peer);
        if (null != ci) ci.callback.onResponse(message);
    }

    private void removeAndExpire(long id, InetAddressAndPort peer)
    {
        CallbackInfo ci = remove(id, peer);
        if (null != ci) onExpired(ci);
    }

    private void expire()
    {
        long start = preciseTime.now();
        int n = 0;
        for (Map.Entry<CallbackKey, CallbackInfo> entry : callbacks.entrySet())
        {
            if (entry.getValue().isReadyToDieAt(start))
            {
                if (callbacks.remove(entry.getKey(), entry.getValue()))
                {
                    n++;
                    onExpired(entry.getValue());
                }
            }
        }
        logger.trace("Expired {} entries", n);
    }

    private void forceExpire()
    {
        for (Map.Entry<CallbackKey, CallbackInfo> entry : callbacks.entrySet())
            if (callbacks.remove(entry.getKey(), entry.getValue()))
                onExpired(entry.getValue());
    }

    private void onExpired(CallbackInfo info)
    {
        messagingService.latencySubscribers.maybeAdd(info.callback, info.peer, info.timeout(), NANOSECONDS);

        InternodeOutboundMetrics.totalExpiredCallbacks.mark();
        messagingService.markExpiredCallback(info.peer);

        if (info.callback.supportsBackPressure())
            messagingService.updateBackPressureOnReceive(info.peer, info.callback, true);

        if (info.invokeOnFailure())
            StageManager.getStage(INTERNAL_RESPONSE).submit(() -> info.callback.onFailure(info.peer, RequestFailureReason.TIMEOUT));

        // FIXME: this has never belonged here, should be part of onFailure() in AbstractWriteResponseHandler
        if (info.shouldHint())
        {
            WriteCallbackInfo writeCallbackInfo = ((WriteCallbackInfo) info);
            Mutation mutation = writeCallbackInfo.mutation();
            StorageProxy.submitHint(mutation, writeCallbackInfo.getReplica(), null);
        }
    }

    void shutdownNow(boolean expireCallbacks)
    {
        executor.shutdownNow();
        if (expireCallbacks)
            forceExpire();
    }

    void shutdownGracefully()
    {
        expire();
        if (!callbacks.isEmpty())
            executor.schedule(this::shutdownGracefully, 100L, MILLISECONDS);
        else
            executor.shutdownNow();
    }

    void awaitTerminationUntil(long deadlineNanos) throws TimeoutException, InterruptedException
    {
        if (!executor.isTerminated())
        {
            long wait = deadlineNanos - System.nanoTime();
            if (wait <= 0 || !executor.awaitTermination(wait, NANOSECONDS))
                throw new TimeoutException();
        }
    }

    @VisibleForTesting
    public void unsafeClear()
    {
        callbacks.clear();
    }

    private static CallbackKey key(long id, InetAddressAndPort peer)
    {
        return new CallbackKey(id, peer);
    }

    private static class CallbackKey
    {
        final long id;
        final InetAddressAndPort peer;

        CallbackKey(long id, InetAddressAndPort peer)
        {
            this.id = id;
            this.peer = peer;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof CallbackKey))
                return false;
            CallbackKey that = (CallbackKey) o;
            return this.id == that.id && this.peer.equals(that.peer);
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(id) + 31 * peer.hashCode();
        }

        @Override
        public String toString()
        {
            return "{id:" + id + ", peer:" + peer + '}';
        }
    }

    static class CallbackInfo
    {
        final long createdAtNanos;
        final long expiresAtNanos;

        final InetAddressAndPort peer;
        final RequestCallback callback;

        @Deprecated // for 3.0 compatibility purposes only
        public final Verb responseVerb;

        private CallbackInfo(Message message, InetAddressAndPort peer, RequestCallback callback)
        {
            this.createdAtNanos = message.createdAtNanos();
            this.expiresAtNanos = message.expiresAtNanos();
            this.peer = peer;
            this.callback = callback;
            this.responseVerb = message.verb().responseVerb;
        }

        public long timeout()
        {
            return expiresAtNanos - createdAtNanos;
        }

        boolean isReadyToDieAt(long atNano)
        {
            return atNano > expiresAtNanos;
        }

        boolean shouldHint()
        {
            return false;
        }

        boolean invokeOnFailure()
        {
            return callback.invokeOnFailure();
        }

        public String toString()
        {
            return "{peer:" + peer + ", callback:" + callback + ", invokeOnFailure:" + invokeOnFailure() + '}';
        }
    }

    // FIXME: shouldn't need a specialized container for write callbacks; hinting should be part of
    //        AbstractWriteResponseHandler implementation.
    static class WriteCallbackInfo extends CallbackInfo
    {
        // either a Mutation, or a Paxos Commit (MessageOut)
        private final Object mutation;
        private final Replica replica;

        @VisibleForTesting
        WriteCallbackInfo(Message message, Replica replica, RequestCallback<?> callback, ConsistencyLevel consistencyLevel, boolean allowHints)
        {
            super(message, replica.endpoint(), callback);
            this.mutation = shouldHint(allowHints, message, consistencyLevel) ? message.payload : null;
            //Local writes shouldn't go through messaging service (https://issues.apache.org/jira/browse/CASSANDRA-10477)
            //noinspection AssertWithSideEffects
            assert !peer.equals(FBUtilities.getBroadcastAddressAndPort());
            this.replica = replica;
        }

        public boolean shouldHint()
        {
            return mutation != null && StorageProxy.shouldHint(replica);
        }

        public Replica getReplica()
        {
            return replica;
        }

        public Mutation mutation()
        {
            return getMutation(mutation);
        }

        private static Mutation getMutation(Object object)
        {
            assert object instanceof Commit || object instanceof Mutation : object;
            return object instanceof Commit ? ((Commit) object).makeMutation()
                                            : (Mutation) object;
        }

        private static boolean shouldHint(boolean allowHints, Message sentMessage, ConsistencyLevel consistencyLevel)
        {
            return allowHints && sentMessage.verb() != Verb.COUNTER_MUTATION_REQ && consistencyLevel != ConsistencyLevel.ANY;
        }
    }

    @Override
    public void onOverloaded(Message<?> message, InetAddressAndPort peer)
    {
        removeAndExpire(message, peer);
    }

    @Override
    public void onExpired(Message<?> message, InetAddressAndPort peer)
    {
        removeAndExpire(message, peer);
    }

    @Override
    public void onFailedSerialize(Message<?> message, InetAddressAndPort peer, int messagingVersion, int bytesWrittenToNetwork, Throwable failure)
    {
        removeAndExpire(message, peer);
    }

    @Override
    public void onDiscardOnClose(Message<?> message, InetAddressAndPort peer)
    {
        removeAndExpire(message, peer);
    }

    private void removeAndExpire(Message message, InetAddressAndPort peer)
    {
        removeAndExpire(message.id(), peer);

        /* in case of a write sent to a different DC, also expire all forwarding targets */
        ForwardingInfo forwardTo = message.forwardTo();
        if (null != forwardTo)
            forwardTo.forEach(this::removeAndExpire);
    }
}
