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
package org.apache.cassandra.hints;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableTask.RunnableDebuggableTask;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.accord.IAccordService.IAccordResult;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper.SplitMutation;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Condition;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.FAILURE;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.INTERRUPTED;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.RETRY_DIFFERENT_SYSTEM;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.SUCCESS;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.TIMEOUT;
import static org.apache.cassandra.hints.HintsService.RETRY_ON_DIFFERENT_SYSTEM_UUID;
import static org.apache.cassandra.metrics.HintsServiceMetrics.ACCORD_HINT_ENDPOINT;
import static org.apache.cassandra.metrics.HintsServiceMetrics.updateDelayMetrics;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.retry_new_protocol;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

/**
 * Dispatches a single hints file to a specified node in a batched manner.
 *
 * Uses either {@link HintMessage.Encoded} - when dispatching hints into a node with the same messaging version as the hints file,
 * or {@link HintMessage}, when conversion is required.
 */
final class HintsDispatcher implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(HintsDispatcher.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private enum Action { CONTINUE, ABORT }

    private final HintsReader reader;
    final UUID hostId;

    @Nullable
    final InetAddressAndPort address;
    private final int messagingVersion;
    private final BooleanSupplier abortRequested;

    private InputPosition currentPagePosition;

    // Hints from the batch log that were attempted on Accord don't have a list of hosts that need hinting
    // since Accord doesn't expose that on failure. If Accord no longer manages the range for this hint then we need
    // to send the hint to all replicas after the page succeeds
    private final Queue<Hint> hintsNeedingRehinting = new LinkedList<>();

    private HintsDispatcher(HintsReader reader, UUID hostId, @Nullable InetAddressAndPort address, int messagingVersion, BooleanSupplier abortRequested)
    {
        checkArgument(address != null ^ hostId.equals(RETRY_ON_DIFFERENT_SYSTEM_UUID), "address must be nonnull or hostId must be " + RETRY_ON_DIFFERENT_SYSTEM_UUID);
        currentPagePosition = null;

        this.reader = reader;
        this.hostId = hostId;
        this.address = address;
        this.messagingVersion = messagingVersion;
        this.abortRequested = abortRequested;
    }

    static HintsDispatcher create(File file, RateLimiter rateLimiter, @Nullable InetAddressAndPort address, UUID hostId, BooleanSupplier abortRequested)
    {
        int messagingVersion = address == null ? MessagingService.current_version : MessagingService.instance().versions.get(address);
        HintsDispatcher dispatcher = new HintsDispatcher(HintsReader.open(file, rateLimiter), hostId, address, messagingVersion, abortRequested);
        HintDiagnostics.dispatcherCreated(dispatcher);
        return dispatcher;
    }

    public void close()
    {
        HintDiagnostics.dispatcherClosed(this);
        reader.close();
    }

    String destination()
    {
        return address == null ? "RETRY_ON_DIFFERENT_SYSTEM" : address.toString();
    }

    void seek(InputPosition position)
    {
        reader.seek(position);
    }

    /**
     * @return whether or not dispatch completed entirely and successfully
     */
    boolean dispatch()
    {
        for (HintsReader.Page page : reader)
        {
            currentPagePosition = page.position;
            if (dispatch(page) != Action.CONTINUE)
                return false;
        }

        return true;
    }

    /**
     * @return offset of the first non-delivered page
     */
    InputPosition dispatchPosition()
    {
        return currentPagePosition;
    }


    // retry in case of a timeout; stop in case of a failure, host going down, or delivery paused
    private Action dispatch(HintsReader.Page page)
    {
        HintDiagnostics.dispatchPage(this);
        return sendHintsAndAwait(page);
    }

    private Action sendHintsAndAwait(HintsReader.Page page)
    {
        try
        {
            return doSendHintsAndAwait(page, null);
        }
        finally
        {
            hintsNeedingRehinting.clear();
        }
    }

    private Action doSendHintsAndAwait(HintsReader.Page page, @Nullable BitSet hintsFilter)
    {
        List<Callback> callbacks = new ArrayList<>();

        /*
         * If hints file messaging version matches the version of the target host, we'll use the optimised path -
         * skipping the redundant decoding/encoding cycle of the already encoded hint.
         *
         * If that is not the case, we'll need to perform conversion to a newer (or an older) format, and decoding the hint
         * is an unavoidable intermediate step.
         *
         * If these hints are from the batchlog and were originally attempted on Accord then
         * we also need to decode so we can route the Hint contents appropriately.
         *
         * If filtering of hints is requested, because this is retrying a page that had some retry on different system
         * errors, then also don't go down the sendEncodedHints path since it won't re-route the mutation and will trigger
         * the same retry on different system error.
         */
        boolean isBatchLogHints = hostId.equals(RETRY_ON_DIFFERENT_SYSTEM_UUID);
        boolean sendEncodedHints = reader.descriptor().messagingVersion() == messagingVersion && !isBatchLogHints && hintsFilter == null;
        // If the hints filter is set then splitting the hints is needed and encoded hints can't do that
        checkState(!sendEncodedHints || hintsFilter == null, "Should not send encoded hints if hints filter is set");
        Action action = sendEncodedHints
                      ? sendHints(page.buffersIterator(), null, callbacks, this::sendEncodedHint)
                      : sendHints(page.hintsIterator(), hintsFilter, callbacks, this::sendHint);

        if (action == Action.ABORT)
            return action;

        BitSet retryDifferentSystemHints = new BitSet(callbacks.size());
        long success = 0, failures = 0, timeouts = 0, retryDifferentSystem = 0;
        for (int i = 0; i < callbacks.size(); i++)
        {
            Callback cb = callbacks.get(i);
            Callback.Outcome outcome = cb.await();
            if (outcome == Callback.Outcome.SUCCESS) success++;
            else if (outcome == Callback.Outcome.FAILURE) failures++;
            else if (outcome == Callback.Outcome.TIMEOUT) timeouts++;
            else if (outcome == RETRY_DIFFERENT_SYSTEM)
            {
                retryDifferentSystemHints.set(i);
                retryDifferentSystem++;
            }
            else throw new IllegalStateException("Unhandled outcome: " + outcome);
        }

        updateMetrics(success, failures, timeouts, retryDifferentSystem);

        // If the only errors were retryDifferentSystem and we aren't already filtering the hints then retry
        // immediately otherwise we will repeat the page later including any successful hints we may have already delivered
        // Hints for the batch log can hit RETRY_DIFFERENT_SYSTEM but don't need to be retried here and it could result
        // in the same hint ending up in hintsNeedingRehinting twice
        boolean failedRetryDifferentSystem = false;
        if (retryDifferentSystem > 0 && failures < 1 && timeouts < 1 && hintsFilter == null && !isBatchLogHints)
        {
            reader.seek(currentPagePosition);
            Action retryResult = doSendHintsAndAwait(page, retryDifferentSystemHints);
            if (retryResult != Action.CONTINUE)
                failedRetryDifferentSystem = true;
        }

        // The batchlog Accord hints need to return abort if any hint needs to be retried and retry the whole page
        // since we don't want hints to ping pong back and forth via hintsNeedingRehinting
        if (failures > 0 || timeouts > 0 || failedRetryDifferentSystem || (isBatchLogHints && retryDifferentSystem > 0))
        {
            HintDiagnostics.pageFailureResult(this, success, failures, timeouts, retryDifferentSystem);
            return Action.ABORT;
        }
        else
        {
            HintDiagnostics.pageSuccessResult(this, success, failures, timeouts, retryDifferentSystem);
            rehintHintsNeedingRehinting();
            return Action.CONTINUE;
        }
    }

    private void rehintHintsNeedingRehinting()
    {
        ClusterMetadata cm = ClusterMetadata.current();
        Hint hint;
        while ((hint = hintsNeedingRehinting.poll()) != null)
        {
            HintsService.instance.writeForAllReplicas(hint);
            Mutation mutation = hint.mutation;
            // Also may need to apply locally because it's possible this is from the batchlog
            // and we never applied it locally
            // TODO (review): Additional error handling necessary? Hints are lossy
            DataPlacement dataPlacement = cm.placements.get(cm.schema.getKeyspace(mutation.getKeyspaceName()).getMetadata().params.replication);
            VersionedEndpoints.ForToken forToken = dataPlacement.writes.forToken(mutation.key().getToken());
            Replica self = forToken.get().selfIfPresent();
            if (self != null)
            {
                Stage.MUTATION.maybeExecuteImmediately(new RunnableDebuggableTask()
                {
                    private final long approxCreationTimeNanos = MonotonicClock.Global.approxTime.now();
                    private volatile long approxStartTimeNanos;

                    @Override
                    public void run()
                    {
                        approxStartTimeNanos = MonotonicClock.Global.approxTime.now();
                        mutation.apply();
                    }

                    @Override
                    public long creationTimeNanos()
                    {
                        return approxCreationTimeNanos;
                    }

                    @Override
                    public long startTimeNanos()
                    {
                        return approxStartTimeNanos;
                    }

                    @Override
                    public String description()
                    {
                        return "HintsService rehinting Accord txn";
                    }
                });
            }
        }

    }

    private void updateMetrics(long success, long failures, long timeouts, long retryDifferentSystem)
    {
        HintsServiceMetrics.hintsSucceeded.mark(success);
        HintsServiceMetrics.hintsFailed.mark(failures);
        HintsServiceMetrics.hintsTimedOut.mark(timeouts);
        HintsServiceMetrics.hintsRetryDifferentSystem.mark(retryDifferentSystem);
    }

    /*
     * Sending hints in compatibility mode.
     */
    private <T> Action sendHints(Iterator<T> hints, @Nullable BitSet hintsFilter, Collection<Callback> callbacks, Function<T, Callback> sendFunction)
    {
        int hintIndex = -1;
        while (hints.hasNext())
        {
            if (abortRequested.getAsBoolean())
            {
                HintDiagnostics.abortRequested(this);
                return Action.ABORT;
            }

            T hint = hints.next();
            hintIndex++;
            if (hintsFilter != null && !hintsFilter.get(hintIndex))
                continue;

            callbacks.add(sendFunction.apply(hint));
        }
        return Action.CONTINUE;
    }

    private Callback sendHint(Hint hint)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        SplitHint splitHint = splitHintIntoAccordAndNormal(cm, hint);
        Mutation accordHintMutation = splitHint.accordMutation;
        Dispatcher.RequestTime requestTime;
        IAccordResult<TxnResult> accordTxnResult = null;
        if (accordHintMutation != null)
        {
            requestTime = Dispatcher.RequestTime.forImmediateExecution();
            accordTxnResult = accordHintMutation != null ? ConsensusMigrationMutationHelper.instance().mutateWithAccordAsync(cm, accordHintMutation, null, requestTime, PreserveTimestamp.yes) : null;
        }

        Hint normalHint = splitHint.normalHint;
        Callback callback = new Callback(address, hint.creationTime, accordTxnResult);
        if (normalHint != null)
        {
            // We had a hint that was supposed to be done on Accord for the batch log (otherwise address would be non-null),
            // but Accord no longer manages that table/range and now we don't know which nodes (if any) are missing the Mutation.
            // Convert them to per replica hints *after* all the hints in this page have been applied so we can be reasonably sure
            // this page isn't going to be played again thus avoiding any futher amplification from the same hint being
            // replayed and repeatedly converted to per replica hints
            if (address == null)
            {
                checkState(hostId.equals(RETRY_ON_DIFFERENT_SYSTEM_UUID), "If there is no address to send the hint to then the host ID should be BATCHLOG_ACCORD_HINT_UUID");
                callback.onResponse(null);
                hintsNeedingRehinting.add(normalHint);
            }
            else
            {
                Message<?> message = Message.out(HINT_REQ, new HintMessage(hostId, normalHint));
                MessagingService.instance().sendWithCallback(message, address, callback);
            }
        }
        else
        {
            // Don't wait for a normal response that will never come since no hints were sent
            callback.onResponse(null);
        }

        return callback;
    }

    /**
     * Result of splitting a hint across Accord and non-transactional boundaries
     */
    private class SplitHint
    {
        private final Mutation accordMutation;
        private final Hint normalHint;

        public SplitHint(Mutation accordMutation, Hint normalHint)
        {
            this.accordMutation = accordMutation;
            this.normalHint = normalHint;
        }

        @Override
        public String toString()
        {
            return "SplitHint{" +
                   "accordMutation=" + accordMutation +
                   ", normalHint=" + normalHint +
                   '}';
        }
    }

    private SplitHint splitHintIntoAccordAndNormal(ClusterMetadata cm, Hint hint)
    {
        SplitMutation<Mutation> splitMutation = ConsensusMigrationMutationHelper.instance().splitMutationIntoAccordAndNormal(hint.mutation, cm);
        if (splitMutation.accordMutation == null)
            return new SplitHint(null, hint);
        if (splitMutation.normalMutation == null)
            return new SplitHint(splitMutation.accordMutation, null);
        Hint normalHint = Hint.create(splitMutation.normalMutation, hint.creationTime, splitMutation.normalMutation.smallestGCGS());
        return new SplitHint(splitMutation.accordMutation, normalHint);
    }

    /*
     * Sending hints in raw mode.
     */

    private Callback sendEncodedHint(ByteBuffer hint)
    {
        HintMessage.Encoded message = new HintMessage.Encoded(hostId, hint, messagingVersion);
        Callback callback = new Callback(address, message.getHintCreationTime());
        MessagingService.instance().sendWithCallback(Message.out(HINT_REQ, message), address, callback);
        return callback;
    }

    static final class Callback implements RequestCallback, BiConsumer<TxnResult, Throwable>
    {
        enum Outcome { SUCCESS, TIMEOUT, FAILURE, INTERRUPTED, RETRY_DIFFERENT_SYSTEM }

        private final long start = approxTime.now();
        private final Condition condition = newOneTimeCondition();
        private Outcome normalOutcome;
        private Outcome accordOutcome;
        @Nullable
        private final InetAddressAndPort to;
        private final long hintCreationNanoTime;

        private Callback(@Nonnull InetAddressAndPort to, long hintCreationTimeMillisSinceEpoch)
        {
            this(to, hintCreationTimeMillisSinceEpoch, null);
        }

        private Callback(@Nullable InetAddressAndPort to, long hintCreationTimeMillisSinceEpoch, @Nullable IAccordResult<TxnResult> accordTxnResult)
        {
            this.to = to != null ? to : ACCORD_HINT_ENDPOINT;
            this.hintCreationNanoTime = approxTime.translate().fromMillisSinceEpoch(hintCreationTimeMillisSinceEpoch);
            if (accordTxnResult != null)
                accordTxnResult.addCallback(this);
            else
                accordOutcome = SUCCESS;
        }

        Outcome await()
        {
            boolean timedOut;
            try
            {
                timedOut = !condition.awaitUntil(HINT_REQ.expiresAtNanos(start));
            }
            catch (InterruptedException e)
            {
                logger.warn("Hint dispatch was interrupted", e);
                return INTERRUPTED;
            }
            normalOutcome = timedOut ? TIMEOUT : normalOutcome;

            return outcome();
        }

        private Outcome outcome()
        {
            checkState((normalOutcome != null && accordOutcome != null) || (normalOutcome != SUCCESS || accordOutcome != SUCCESS), "Outcome for both normal and accord hint delivery should be known");
            if (normalOutcome == RETRY_DIFFERENT_SYSTEM || accordOutcome == RETRY_DIFFERENT_SYSTEM)
                return RETRY_DIFFERENT_SYSTEM;
            if (normalOutcome == TIMEOUT || accordOutcome == TIMEOUT)
                return TIMEOUT;
            if (normalOutcome == FAILURE || accordOutcome == FAILURE)
                return FAILURE;
            checkState(normalOutcome == SUCCESS && accordOutcome == SUCCESS, "Hint delivery should have been successful");
            return SUCCESS;
        }

        private synchronized void maybeSignal()
        {
            if ((normalOutcome != null && accordOutcome != null) || normalOutcome == FAILURE || accordOutcome == FAILURE)
            {
                updateDelayMetrics(to, approxTime.now() - this.hintCreationNanoTime);
                condition.signalAll();
            }
        }

        @Override
        public boolean invokeOnFailure()
        {
            return true;
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailure failureMessage)
        {
            if (failureMessage.reason == RequestFailureReason.RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM)
                normalOutcome = RETRY_DIFFERENT_SYSTEM;
            else
                normalOutcome = FAILURE;
            maybeSignal();
        }

        @Override
        public void onResponse(Message msg)
        {
            normalOutcome = SUCCESS;
            maybeSignal();
        }

        @Override
        public void accept(TxnResult success, Throwable fail)
        {
            if (fail != null)
            {
                if (fail instanceof RequestExecutionException || fail instanceof RetryOnDifferentSystemException)
                {
                    if (fail instanceof RetryOnDifferentSystemException)
                        accordOutcome = RETRY_DIFFERENT_SYSTEM;
                    else
                        accordOutcome = TIMEOUT;
                    String msg = "Accord hint delivery transaction failed retriably";
                    if (noSpamLogger.getStatement(msg).shouldLog(Clock.Global.nanoTime()))
                        logger.error(msg, fail);
                }
                else
                {
                    accordOutcome = FAILURE;
                    String msg = "Accord hint delivery transaction failed permanently";
                    if (noSpamLogger.getStatement(msg).shouldLog(Clock.Global.nanoTime()))
                        logger.error(msg, fail);
                }
            }
            else
            {
                if (success.kind() == retry_new_protocol)
                    accordOutcome = RETRY_DIFFERENT_SYSTEM;
                else
                    accordOutcome = SUCCESS;
            }
            maybeSignal();
        }
    }
}
