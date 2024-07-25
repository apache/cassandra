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
package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Command;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.messages.AbstractEpochRequest;
import accord.messages.Commit;
import accord.messages.LocalRequest;
import accord.messages.Message;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.concurrent.ManyToOneConcurrentLinkedQueue;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordConsumer;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseContext;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.interop.AccordInteropApply;
import org.apache.cassandra.service.accord.interop.AccordInteropCommit;
import org.apache.cassandra.service.accord.serializers.AcceptSerializers;
import org.apache.cassandra.service.accord.serializers.ApplySerializers;
import org.apache.cassandra.service.accord.serializers.BeginInvalidationSerializers;
import org.apache.cassandra.service.accord.serializers.CommitSerializers;
import org.apache.cassandra.service.accord.serializers.EnumSerializer;
import org.apache.cassandra.service.accord.serializers.FetchSerializers;
import org.apache.cassandra.service.accord.serializers.InformDurableSerializers;
import org.apache.cassandra.service.accord.serializers.InformOfTxnIdSerializers;
import org.apache.cassandra.service.accord.serializers.PreacceptSerializers;
import org.apache.cassandra.service.accord.serializers.RecoverySerializers;
import org.apache.cassandra.service.accord.serializers.SetDurableSerializers;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.messages.MessageType.ACCEPT_INVALIDATE_REQ;
import static accord.messages.MessageType.ACCEPT_REQ;
import static accord.messages.MessageType.APPLY_MAXIMAL_REQ;
import static accord.messages.MessageType.APPLY_MINIMAL_REQ;
import static accord.messages.MessageType.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ;
import static accord.messages.MessageType.BEGIN_INVALIDATE_REQ;
import static accord.messages.MessageType.BEGIN_RECOVER_REQ;
import static accord.messages.MessageType.COMMIT_INVALIDATE_REQ;
import static accord.messages.MessageType.COMMIT_MAXIMAL_REQ;
import static accord.messages.MessageType.COMMIT_SLOW_PATH_REQ;
import static accord.messages.MessageType.INFORM_DURABLE_REQ;
import static accord.messages.MessageType.INFORM_OF_TXN_REQ;
import static accord.messages.MessageType.PRE_ACCEPT_REQ;
import static accord.messages.MessageType.PROPAGATE_APPLY_MSG;
import static accord.messages.MessageType.PROPAGATE_OTHER_MSG;
import static accord.messages.MessageType.PROPAGATE_PRE_ACCEPT_MSG;
import static accord.messages.MessageType.PROPAGATE_STABLE_MSG;
import static accord.messages.MessageType.SET_GLOBALLY_DURABLE_REQ;
import static accord.messages.MessageType.SET_SHARD_DURABLE_REQ;
import static accord.messages.MessageType.STABLE_FAST_PATH_REQ;
import static accord.messages.MessageType.STABLE_MAXIMAL_REQ;
import static accord.messages.MessageType.STABLE_SLOW_PATH_REQ;
import static org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType.INTEROP_APPLY_MAXIMAL_REQ;
import static org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType.INTEROP_APPLY_MINIMAL_REQ;
import static org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType.INTEROP_COMMIT_MAXIMAL_REQ;
import static org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType.INTEROP_COMMIT_MINIMAL_REQ;
import static org.apache.cassandra.service.accord.serializers.ReadDataSerializers.applyThenWaitUntilApplied;

public class AccordJournal implements IJournal, Shutdownable
{
    static
    {
        // make noise early if we forget to update our version mappings
        Invariants.checkState(MessagingService.current_version == MessagingService.VERSION_51, "Expected current version to be %d but given %d", MessagingService.VERSION_51, MessagingService.current_version);
    }

    private static final Logger logger = LoggerFactory.getLogger(AccordJournal.class);

    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

    static final ThreadLocal<byte[]> keyCRCBytes = ThreadLocal.withInitial(() -> new byte[23]);

    public final Journal<JournalKey, Object> journal;
    private final AccordEndpointMapper endpointMapper;

    private final DelayedRequestProcessor delayedRequestProcessor = new DelayedRequestProcessor();

    Node node;

    enum Status { INITIALIZED, STARTING, STARTED, TERMINATING, TERMINATED }
    private volatile Status status = Status.INITIALIZED;

    @VisibleForTesting
    public AccordJournal(AccordEndpointMapper endpointMapper, Params params)
    {
        File directory = new File(DatabaseDescriptor.getAccordJournalDirectory());
        this.journal = new Journal<>("AccordJournal", directory, params, JournalKey.SUPPORT, RECORD_SERIALIZER);
        this.endpointMapper = endpointMapper;
    }

    public AccordJournal start(Node node)
    {
        Invariants.checkState(status == Status.INITIALIZED);
        this.node = node;
        status = Status.STARTING;
        journal.start();
        this.delayedRequestProcessor.start();
        status = Status.STARTED;
        return this;
    }

    @VisibleForTesting
    public void closeCurrentSegmentForTesting()
    {
        journal.closeCurrentSegmentForTesting();
    }

    @VisibleForTesting
    public void replay()
    {
        // TODO: should use an accumulator instead
        Map<JournalKey, List<SavedCommand.LoadedDiff>> loaded = new HashMap<>();
        journal.replayStaticSegments(new RecordConsumer<JournalKey>()
        {
            public void accept(long segment, int position, JournalKey key, ByteBuffer buffer, IntHashSet hosts, int userVersion)
            {
                try (DataInputBuffer in = new DataInputBuffer(buffer, false))
                {
                    if (key.type == Type.SAVED_COMMAND)
                    {
                        SavedCommand.LoadedDiff diff = (SavedCommand.LoadedDiff) RECORD_SERIALIZER.deserialize(key, in, userVersion);
                        loaded.computeIfAbsent(key, (k_) -> new ArrayList<>())
                              .add(diff);
                    }

                }
                catch (IOException e)
                {
                    // can only throw if serializer is buggy
                    throw new RuntimeException(e);
                }
            }
        });
        Map<JournalKey, Command> commands = new HashMap<>();
        for (Map.Entry<JournalKey, List<SavedCommand.LoadedDiff>> e : loaded.entrySet())
        {
            List<SavedCommand.LoadedDiff> diff = e.getValue();
            Command command = SavedCommand.reconstructFromDiff(diff);
            commands.put(e.getKey(), command);
            node.commandStores()
                .forId(e.getKey().commandStoreId)
                .execute(PreLoadContext.empty(), safeStore -> safeStore.load(command));
            // TODO: populate cache
        }

    }
    @Override
    public boolean isTerminated()
    {
        return status == Status.TERMINATED;
    }

    @Override
    public void shutdown()
    {
        Invariants.checkState(status == Status.STARTED);
        this.delayedRequestProcessor.interrupt();
        status = Status.TERMINATING;
        journal.shutdown();
        status = Status.TERMINATED;
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        try
        {
            ExecutorUtils.awaitTermination(timeout, units, Arrays.asList(journal));
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    /**
     * Accord protocol messages originating from remote nodes.
     */
    public void processRemoteRequest(Request request, ResponseContext context)
    {
        RemoteRequestContext requestContext = RemoteRequestContext.forLive(request, context);
        if (node.topology().hasEpoch(request.waitForEpoch()))
            requestContext.process(node, endpointMapper);
        else
            delayedRequestProcessor.delay(requestContext);
    }

    /**
     * Accord protocol messages originating from local node, e.g. Propagate.
     */
    @SuppressWarnings("rawtypes, unchecked")
    public <R> void processLocalRequest(LocalRequest request, BiConsumer<? super R, Throwable> callback)
    {
        LocalRequestContext requestContext = LocalRequestContext.create(request, callback);
        if (node.topology().hasEpoch(request.waitForEpoch()))
            request.process(node, requestContext.callback);
        else
            delayedRequestProcessor.delay(requestContext);
    }

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId)
    {
        var diffs = loadDiffs(commandStoreId, txnId);
        if (diffs.isEmpty())
            return null;
        return SavedCommand.reconstructFromDiff(diffs);
    }

    @VisibleForTesting
    public List<SavedCommand.LoadedDiff> loadDiffs(int commandStoreId, Timestamp txnId)
    {
        return (List<SavedCommand.LoadedDiff>)(List<?>) journal.readAll(new JournalKey(txnId, Type.SAVED_COMMAND, commandStoreId));
    }

    @Override
    public void appendCommand(int commandStoreId, List<SavedCommand.SavedDiff> outcomes, List<Command> sanityCheck, Runnable onFlush)
    {
        RecordPointer pointer = null;
        for (int i = 0; i < outcomes.size(); i++)
        {
            SavedCommand.SavedDiff outcome = outcomes.get(i);
            JournalKey key = new JournalKey(outcome.txnId, Type.SAVED_COMMAND, commandStoreId);
            pointer = journal.asyncWrite(key, outcome, SENTINEL_HOSTS);
        }

        // If we need to perform sanity check, we can only rely on blocking flushes. Otherwise, we may see into the future.
        if (sanityCheck != null)
        {
            Condition condition = Condition.newOneTimeCondition();
            journal.onFlush(pointer, condition::signal);
            condition.awaitUninterruptibly();

            for (Command check : sanityCheck)
                sanityCheck(commandStoreId, check);

            onFlush.run();
        }
        else
        {
            journal.onFlush(pointer, onFlush);
        }
    }

    public void sanityCheck(int commandStoreId, Command orig)
    {
        List<SavedCommand.LoadedDiff> diffs = loadDiffs(commandStoreId, orig.txnId());
        // We can only use strict equality if we supply result.
        Command reconstructed = SavedCommand.reconstructFromDiff(diffs, orig.result());
        Invariants.checkState(orig.equals(reconstructed),
                              "\n" +
                              "Original:      %s\n" +
                              "Reconstructed: %s\n" +
                              "Diffs:         %s", orig, reconstructed, diffs);
    }

    /*
     * Context necessary to process log records
     */
    static abstract class RequestContext implements ReplyContext
    {
        final long waitForEpoch;

        RequestContext(long waitForEpoch)
        {
            this.waitForEpoch = waitForEpoch;
        }

        public abstract void process(Node node, AccordEndpointMapper endpointMapper);
    }

    private static class LocalRequestContext<T> extends RequestContext
    {
        private final BiConsumer<T, Throwable> callback;
        private final LocalRequest<T> request;

        LocalRequestContext(long waitForEpoch, LocalRequest<T> request, BiConsumer<T, Throwable> callback)
        {
            super(waitForEpoch);
            this.callback = callback;
            this.request = request;
        }

        public void process(Node node, AccordEndpointMapper endpointMapper)
        {
            request.process(node, callback);
        }

        static <R> LocalRequestContext<R> create(LocalRequest<R> request, BiConsumer<R, Throwable> callback)
        {
            return new LocalRequestContext<>(request.waitForEpoch(), request, callback);
        }
    }

    /**
     * Barebones response context not holding a reference to the entire message
     */
    private abstract static class RemoteRequestContext extends RequestContext implements ResponseContext
    {
        private final Request request;

        RemoteRequestContext(long waitForEpoch, Request request)
        {
            super(waitForEpoch);
            this.request = request;
        }

        static LiveRemoteRequestContext forLive(Request request, ResponseContext context)
        {
            return new LiveRemoteRequestContext(request, context.id(), context.from(), context.verb(), context.expiresAtNanos());
        }

        @Override
        public void process(Node node, AccordEndpointMapper endpointMapper)
        {
            this.request.process(node, endpointMapper.mappedId(from()), this);
        }

        @Override public abstract long id();
        @Override public abstract InetAddressAndPort from();
        @Override public abstract Verb verb();
        @Override public abstract long expiresAtNanos();
    }

    // TODO: avoid distinguishing between live and non live
    private static class LiveRemoteRequestContext extends RemoteRequestContext
    {
        private final long id;
        private final InetAddressAndPort from;
        private final Verb verb;
        private final long expiresAtNanos;

        LiveRemoteRequestContext(Request request, long id, InetAddressAndPort from, Verb verb, long expiresAtNanos)
        {
            super(request.waitForEpoch(), request);
            this.id = id;
            this.from = from;
            this.verb = verb;
            this.expiresAtNanos = expiresAtNanos;
        }


        @Override
        public long id()
        {
            return id;
        }
        @Override
        public InetAddressAndPort from()
        {
            return from;
        }
        @Override
        public Verb verb()
        {
            return verb;
        }
        @Override
        public long expiresAtNanos()
        {
            return expiresAtNanos;
        }
    }

    /*
     * Records ser/de in the Journal
     */

    private static final ValueSerializer<JournalKey, Object> RECORD_SERIALIZER = new ValueSerializer<>()
    {
        @Override
        public int serializedSize(JournalKey key, Object record, int userVersion)
        {
            return Ints.checkedCast(key.type.serializedSize(key, record, userVersion));
        }

        @Override
        public void serialize(JournalKey key, Object record, DataOutputPlus out, int userVersion) throws IOException
        {
            key.type.serialize(key, record, out, userVersion);
        }

        @Override
        public Object deserialize(JournalKey key, DataInputPlus in, int userVersion) throws IOException
        {
            return key.type.deserialize(key, in, userVersion);
        }
    };

    /* Adapts vanilla message serializers to journal-expected signatures; converts user version to MS version */
    static final class MessageSerializer implements ValueSerializer<JournalKey, Object>
    {
        final IVersionedSerializer<Message> wrapped;

        private MessageSerializer(IVersionedSerializer<Message> wrapped)
        {
            this.wrapped = wrapped;
        }

        static MessageSerializer wrap(IVersionedSerializer<Message> wrapped)
        {
            return new MessageSerializer(wrapped);
        }

        @Override
        public int serializedSize(JournalKey key, Object message, int userVersion)
        {
            return Ints.checkedCast(wrapped.serializedSize((Message) message, msVersion(userVersion)));
        }

        @Override
        public void serialize(JournalKey key, Object message, DataOutputPlus out, int userVersion) throws IOException
        {
            wrapped.serialize((Message) message, out, msVersion(userVersion));
        }

        @Override
        public Object deserialize(JournalKey key, DataInputPlus in, int userVersion) throws IOException
        {
            return wrapped.deserialize(in, msVersion(userVersion));
        }
    }

    @FunctionalInterface
    interface TxnIdProvider
    {
        TxnId txnId(Message message);
    }

    private static final TxnIdProvider EPOCH = msg -> ((AbstractEpochRequest<?>) msg).txnId;
    private static final TxnIdProvider TXN   = msg -> ((TxnRequest<?>) msg).txnId;
    private static final TxnIdProvider LOCAL = msg -> ((LocalRequest<?>) msg).primaryTxnId();
    private static final TxnIdProvider INVL  = msg -> ((Commit.Invalidate) msg).primaryTxnId();

    /**
     * Accord Message type - consequently the kind of persisted record.
     * <p>
     * Note: {@link EnumSerializer} is intentionally not being reused here, for two reasons:
     *  1. This is an internal enum, fully under our control, not part of an external library
     *  2. It's persisted in the record key, so has the additional constraint of being fixed size and
     *     shouldn't be using varint encoding
     */
    public enum Type implements ValueSerializer<JournalKey, Object>
    {
        /* Auxiliary journal records */
        SAVED_COMMAND                 (1, SavedCommand.serializer),

        /* Accord protocol requests */
        PRE_ACCEPT                    (64, PRE_ACCEPT_REQ,                    PreacceptSerializers.request, TXN  ),
        ACCEPT                        (65, ACCEPT_REQ,                        AcceptSerializers.request,    TXN  ),
        ACCEPT_INVALIDATE             (66, ACCEPT_INVALIDATE_REQ,             AcceptSerializers.invalidate, EPOCH),
        COMMIT_SLOW_PATH              (67, COMMIT_SLOW_PATH_REQ,              CommitSerializers.request,    TXN  ),
        COMMIT_MAXIMAL                (68, COMMIT_MAXIMAL_REQ,                CommitSerializers.request,    TXN  ),
        STABLE_FAST_PATH              (87, STABLE_FAST_PATH_REQ,              CommitSerializers.request,    TXN  ),
        STABLE_SLOW_PATH              (88, STABLE_SLOW_PATH_REQ,              CommitSerializers.request,    TXN  ),
        STABLE_MAXIMAL                (89, STABLE_MAXIMAL_REQ,                CommitSerializers.request,    TXN  ),
        COMMIT_INVALIDATE             (69, COMMIT_INVALIDATE_REQ,             CommitSerializers.invalidate, INVL ),
        APPLY_MINIMAL                 (70, APPLY_MINIMAL_REQ,                 ApplySerializers.request,     TXN  ),
        APPLY_MAXIMAL                 (71, APPLY_MAXIMAL_REQ,                 ApplySerializers.request,     TXN  ),
        APPLY_THEN_WAIT_UNTIL_APPLIED (72, APPLY_THEN_WAIT_UNTIL_APPLIED_REQ, applyThenWaitUntilApplied,    EPOCH),

        BEGIN_RECOVER                 (73, BEGIN_RECOVER_REQ,        RecoverySerializers.request,           TXN  ),
        BEGIN_INVALIDATE              (74, BEGIN_INVALIDATE_REQ,     BeginInvalidationSerializers.request,  EPOCH),
        INFORM_OF_TXN                 (75, INFORM_OF_TXN_REQ,        InformOfTxnIdSerializers.request,      EPOCH),
        INFORM_DURABLE                (76, INFORM_DURABLE_REQ,       InformDurableSerializers.request,      TXN  ),
        SET_SHARD_DURABLE             (77, SET_SHARD_DURABLE_REQ,    SetDurableSerializers.shardDurable,    EPOCH),
        SET_GLOBALLY_DURABLE          (78, SET_GLOBALLY_DURABLE_REQ, SetDurableSerializers.globallyDurable, EPOCH),

        /* Accord local messages */
        PROPAGATE_PRE_ACCEPT          (79, PROPAGATE_PRE_ACCEPT_MSG, FetchSerializers.propagate, LOCAL),
        PROPAGATE_STABLE              (80, PROPAGATE_STABLE_MSG,     FetchSerializers.propagate, LOCAL),
        PROPAGATE_APPLY               (81, PROPAGATE_APPLY_MSG,      FetchSerializers.propagate, LOCAL),
        PROPAGATE_OTHER               (82, PROPAGATE_OTHER_MSG,      FetchSerializers.propagate, LOCAL),

        /* C* interop messages */
        INTEROP_COMMIT                (83, INTEROP_COMMIT_MINIMAL_REQ,  STABLE_FAST_PATH_REQ, AccordInteropCommit.serializer, TXN),
        INTEROP_COMMIT_MAXIMAL        (84, INTEROP_COMMIT_MAXIMAL_REQ, STABLE_MAXIMAL_REQ,   AccordInteropCommit.serializer, TXN),
        INTEROP_APPLY_MINIMAL         (85, INTEROP_APPLY_MINIMAL_REQ,  APPLY_MINIMAL_REQ,    AccordInteropApply.serializer,  TXN),
        INTEROP_APPLY_MAXIMAL         (86, INTEROP_APPLY_MAXIMAL_REQ,  APPLY_MAXIMAL_REQ,    AccordInteropApply.serializer,  TXN),
        ;

        final int id;

        /**
         * An incoming message of a given type from Accord's perspective might have multiple
         * concrete implementations some of which are supplied by the Cassandra integration.
         * The incoming type specifies the handling for writing out a message to the journal.
         */
        final MessageType incomingType;

        /**
         * The outgoing type is the type that will be returned to Accord; must be a subclass of the incoming type.
         * <p>
         * This type will always be from accord.messages.MessageType and never from the extended types in the integration.
         */
        final MessageType outgoingType;

        final TxnIdProvider txnIdProvider;
        final ValueSerializer<JournalKey, Object> serializer;

        Type(int id,  ValueSerializer<JournalKey, Object> serializer)
        {
            this(id, null, null, serializer, null);
        }


        Type(int id, MessageType incomingType, MessageType outgoingType, IVersionedSerializer<?> serializer, TxnIdProvider txnIdProvider)
        {
            //noinspection unchecked
            this(id, incomingType, outgoingType, MessageSerializer.wrap((IVersionedSerializer<Message>) serializer), txnIdProvider);
        }

        Type(int id, MessageType type, IVersionedSerializer<?> serializer, TxnIdProvider txnIdProvider)
        {
            //noinspection unchecked
            this(id, type, type, MessageSerializer.wrap((IVersionedSerializer<Message>) serializer), txnIdProvider);
        }

        Type(int id, MessageType incomingType, MessageType outgoingType, ValueSerializer<JournalKey, ?> serializer, TxnIdProvider txnIdProvider)
        {
            if (id < 0)
                throw new IllegalArgumentException("Negative Type id " + id);
            if (id > Byte.MAX_VALUE)
                throw new IllegalArgumentException("Type id doesn't fit in a single byte: " + id);

            this.id = id;
            this.incomingType = incomingType;
            this.outgoingType = outgoingType;
            //noinspection unchecked
            this.serializer = (ValueSerializer<JournalKey, Object>) serializer;
            this.txnIdProvider = txnIdProvider;
        }

        private static final Type[] idToTypeMapping;

        static
        {
            Type[] types = values();

            int maxId = -1;
            for (Type type : types)
                maxId = Math.max(type.id, maxId);

            Type[] idToType = new Type[maxId + 1];
            for (Type type : types)
            {
                if (null != idToType[type.id])
                    throw new IllegalStateException("Duplicate Type id " + type.id);
                idToType[type.id] = type;
            }
            idToTypeMapping = idToType;

            Map<MessageType, Type> msgTypeToType = new HashMap<>();
            for (Type type : types)
            {
                if (null != type.incomingType && null != msgTypeToType.put(type.incomingType, type))
                    throw new IllegalStateException("Duplicate MessageType " + type.incomingType);
            }
            ImmutableMap.copyOf(msgTypeToType);
        }

        static Type fromId(int id)
        {
            if (id < 0 || id >= idToTypeMapping.length)
                throw new IllegalArgumentException("Out or range Type id " + id);
            Type type = idToTypeMapping[id];
            if (null == type)
                throw new IllegalArgumentException("Unknown Type id " + id);
            return type;
        }

        @Override
        public int serializedSize(JournalKey key, Object record, int userVersion)
        {
            return serializer.serializedSize(key, record, userVersion);
        }

        @Override
        public void serialize(JournalKey key, Object record, DataOutputPlus out, int userVersion) throws IOException
        {
            serializer.serialize(key, record, out, userVersion);
        }

        @Override
        public Object deserialize(JournalKey key, DataInputPlus in, int userVersion) throws IOException
        {
            return serializer.deserialize(key, in, userVersion);
        }
    }

    private static int msVersion(int version)
    {
        switch (version)
        {
            default: throw new IllegalArgumentException();
            case 1: return MessagingService.VERSION_51;
        }
    }

    /*
     * Handling topology changes / epoch shift
     */

    private final class DelayedRequestProcessor extends Thread
    {
        private final ManyToOneConcurrentLinkedQueue<RequestContext> delayedRequests = new ManyToOneConcurrentLinkedQueue<>();
        private final LongArrayList waitForEpochs = new LongArrayList();
        private final Long2ObjectHashMap<List<RequestContext>> byEpoch = new Long2ObjectHashMap<>();
        private final AtomicReference<Condition> signal = new AtomicReference<>(Condition.newOneTimeCondition());

        private void delay(RequestContext requestContext)
        {
            delayedRequests.add(requestContext);
            runOnce();
        }

        private void runOnce()
        {
            signal.get().signal();
        }

        public void run()
        {
            while (!Thread.currentThread().isInterrupted() && !isTerminated())
            {
                try
                {
                    Condition signal = Condition.newOneTimeCondition();
                    this.signal.set(signal);
                    // First, poll delayed requests, put them into by epoch
                    while (!delayedRequests.isEmpty())
                    {
                        RequestContext context = delayedRequests.poll();
                        long waitForEpoch = context.waitForEpoch;

                        List<RequestContext> l = byEpoch.computeIfAbsent(waitForEpoch, (ignore) -> new ArrayList<>());
                        if (l.isEmpty())
                            waitForEpochs.pushLong(waitForEpoch);
                        l.add(context);
                        node.withEpoch(waitForEpoch, this::runOnce);
                    }

                    // Next, process all delayed epochs
                    for (int i = 0; i < waitForEpochs.size(); i++)
                    {
                        long epoch = waitForEpochs.getLong(i);
                        if (node.topology().hasEpoch(epoch))
                        {
                            List<RequestContext> requests = byEpoch.remove(epoch);
                            assert requests != null : String.format("%s %s (%d)", byEpoch, waitForEpochs, epoch);
                            for (RequestContext request : requests)
                            {
                                try
                                {
                                    request.process(node, endpointMapper);
                                }
                                catch (Throwable t)
                                {
                                    logger.error(String.format("Caught an exception while processing a delayed request %s", request), t);
                                }
                            }
                        }
                    }

                    waitForEpochs.removeIfLong(epoch -> !byEpoch.containsKey(epoch));

                    signal.await();
                }
                catch (InterruptedException e)
                {
                    logger.info("Delayed request processor thread interrupted. Shutting down.");
                    return;
                }
                catch (Throwable t)
                {
                    logger.error("Caught an exception in delayed processor", t);
                }
            }
        }
    }

    @VisibleForTesting
    public void truncateForTesting()
    {
        journal.truncateForTesting();
    }
}