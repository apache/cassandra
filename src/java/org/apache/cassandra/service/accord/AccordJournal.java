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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.Timeout;
import accord.local.Command;
import accord.local.Node;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.concurrent.InfiniteLoopExecutor;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.ManyToOneConcurrentLinkedQueue;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseContext;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.local.Status.PreApplied;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;

public class AccordJournal implements IJournal, Shutdownable
{
    static
    {
        // make noise early if we forget to update our version mappings
        Invariants.checkState(MessagingService.current_version == MessagingService.VERSION_51, "Expected current version to be %d but given %d", MessagingService.VERSION_51, MessagingService.current_version);
    }

    private static final Logger logger = LoggerFactory.getLogger(AccordJournal.class);

    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

    static final ThreadLocal<byte[]> keyCRCBytes = ThreadLocal.withInitial(() -> new byte[22]);

    private final Journal<JournalKey, Object> journal;
    private final AccordJournalTable<JournalKey, Object> journalTable;
    private final AccordEndpointMapper endpointMapper;

    private final DelayedRequestProcessor delayedRequestProcessor = new DelayedRequestProcessor();

    Node node;

    enum Status { INITIALIZED, STARTING, STARTED, TERMINATING, TERMINATED }
    private volatile Status status = Status.INITIALIZED;

    @VisibleForTesting
    public AccordJournal(AccordEndpointMapper endpointMapper, Params params)
    {
        File directory = new File(DatabaseDescriptor.getAccordJournalDirectory());
        this.journal = new Journal<>("AccordJournal", directory, params, JournalKey.SUPPORT,
                                     // In Accord, we are using streaming serialization, i.e. Reader/Writer interfaces instead of materializing objects
                                     new ValueSerializer<>()
                                     {
                                         public int serializedSize(JournalKey key, Object value, int userVersion)
                                         {
                                             throw new UnsupportedOperationException();
                                         }

                                         public void serialize(JournalKey key, Object value, DataOutputPlus out, int userVersion)
                                         {
                                             throw new UnsupportedOperationException();
                                         }

                                         public Object deserialize(JournalKey key, DataInputPlus in, int userVersion)
                                         {
                                             throw new UnsupportedOperationException();
                                         }
                                     },
                                     new AccordSegmentCompactor<>());
        this.endpointMapper = endpointMapper;
        this.journalTable = new AccordJournalTable<>(journal, JournalKey.SUPPORT, params.userVersion());
    }

    public AccordJournal start(Node node)
    {
        Invariants.checkState(status == Status.INITIALIZED);
        this.node = node;
        status = Status.STARTING;
        journal.start();
        delayedRequestProcessor.start();
        status = Status.STARTED;
        return this;
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
        status = Status.TERMINATING;
        delayedRequestProcessor.shutdown();
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
            ExecutorUtils.awaitTermination(timeout, units, Collections.singletonList(journal));
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

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId)
    {
        try
        {
            return loadDiffs(commandStoreId, txnId).construct();
        }
        catch (IOException e)
        {
            // can only throw if serializer is buggy
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public SavedCommand.Builder loadDiffs(int commandStoreId, TxnId txnId)
    {
        JournalKey key = new JournalKey(txnId, commandStoreId);
        SavedCommand.Builder builder = new SavedCommand.Builder();
        journalTable.readAll(key, builder::deserializeNext);
        return builder;
    }

    @Override
    public void appendCommand(int commandStoreId, List<SavedCommand.Writer<TxnId>> outcomes, List<Command> sanityCheck, Runnable onFlush)
    {
        RecordPointer pointer = null;
        for (SavedCommand.Writer<TxnId> outcome : outcomes)
        {
            JournalKey key = new JournalKey(outcome.key(), commandStoreId);
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

    @VisibleForTesting
    public void closeCurrentSegmentForTesting()
    {
        journal.closeCurrentSegmentForTesting();
    }

    public void sanityCheck(int commandStoreId, Command orig)
    {
        try
        {
            SavedCommand.Builder diffs = loadDiffs(commandStoreId, orig.txnId());
            diffs.forceResult(orig.result());
            // We can only use strict equality if we supply result.
            Command reconstructed = diffs.construct();
            Invariants.checkState(orig.equals(reconstructed),
                                  '\n' +
                                  "Original:      %s\n" +
                                  "Reconstructed: %s\n" +
                                  "Diffs:         %s", orig, reconstructed, diffs);
        }
        catch (IOException e)
        {
            // can only throw if serializer is buggy
            throw new RuntimeException(e);
        }
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
     * Handling topology changes / epoch shift
     */

    private class DelayedRequestProcessor implements Interruptible.Task
    {
        private final ManyToOneConcurrentLinkedQueue<RequestContext> delayedRequests = new ManyToOneConcurrentLinkedQueue<>();
        private final LongArrayList waitForEpochs = new LongArrayList();
        private final Long2ObjectHashMap<List<RequestContext>> byEpoch = new Long2ObjectHashMap<>();
        private final AtomicReference<Condition> signal = new AtomicReference<>(Condition.newOneTimeCondition());
        private volatile Interruptible executor;

        public void start()
        {
             executor = executorFactory().infiniteLoop("AccordJournal-delayed-request-processor", this, SAFE, InfiniteLoopExecutor.Daemon.NON_DAEMON, InfiniteLoopExecutor.Interrupts.SYNCHRONIZED);
        }

        private void delay(RequestContext requestContext)
        {
            delayedRequests.add(requestContext);
            runOnce();
        }

        private void runOnce()
        {
            signal.get().signal();
        }

        @Override
        public void run(Interruptible.State state)
        {
            if (state != NORMAL || Thread.currentThread().isInterrupted() || !isRunnable(status))
                return;

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
                    BiConsumer<Void, Throwable> withEpochCallback = new BiConsumer<>()
                    {
                        @Override
                        public void accept(Void unused, Throwable withEpochFailure)
                        {
                            if (withEpochFailure != null)
                            {
                                // Nothing to do but keep waiting
                                if (withEpochFailure instanceof Timeout)
                                {
                                    node.withEpoch(waitForEpoch, this);
                                    return;
                                }
                                else
                                    throw new RuntimeException(withEpochFailure);
                            }
                            runOnce();
                        }
                    };
                    node.withEpoch(waitForEpoch, withEpochCallback);
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
                                logger.error("Caught an exception while processing a delayed request {}", request, t);
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
            }
            catch (Throwable t)
            {
                logger.error("Caught an exception in delayed processor", t);
            }
        }

        private void shutdown()
        {
            executor.shutdown();
            try
            {
                executor.awaitTermination(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean isRunnable(Status status)
    {
        return status != Status.TERMINATING && status != Status.TERMINATED;
    }

    @VisibleForTesting
    public void truncateForTesting()
    {
        journal.truncateForTesting();
    }

    @VisibleForTesting
    public void replay()
    {
        // TODO: optimize replay memory footprint
        class ToApply
        {
            final JournalKey key;
            final Command command;

            ToApply(JournalKey key, Command command)
            {
                this.key = key;
                this.command = command;
            }
        }

        List<ToApply> toApply = new ArrayList<>();
        try (AccordJournalTable.KeyOrderIterator<JournalKey> iter = journalTable.readAll())
        {
            JournalKey key = null;
            final SavedCommand.Builder builder = new SavedCommand.Builder();
            while ((key = iter.key()) != null)
            {
                JournalKey finalKey = key;
                iter.readAllForKey(key, (segment, position, local, buffer, hosts, userVersion) -> {
                    Invariants.checkState(finalKey.equals(local));
                    try (DataInputBuffer in = new DataInputBuffer(buffer, false))
                    {
                        builder.deserializeNext(in, userVersion);
                    }
                    catch (IOException e)
                    {
                        // can only throw if serializer is buggy
                        throw new RuntimeException(e);
                    }
                });

                Command command = builder.construct();
                AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().forId(key.commandStoreId);
                commandStore.loader().load(command).get();
                if (command.hasBeen(PreApplied))
                    toApply.add(new ToApply(key, command));
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Can not replay journal.", t);
        }
        toApply.sort(Comparator.comparing(v -> v.command.executeAt()));
        for (ToApply apply : toApply)
        {
            AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().forId(apply.key.commandStoreId);
            commandStore.loader().apply(apply.command);
        }
    }

}