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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * Dispatches a single hints file to a specified node in a batched manner.
 *
 * Uses either {@link HintMessage.Encoded} - when dispatching hints into a node with the same messaging version as the hints file,
 * or {@link HintMessage}, when conversion is required.
 */
final class HintsDispatcher implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(HintsDispatcher.class);

    private enum Action { CONTINUE, ABORT }

    private final HintsReader reader;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13457
    final UUID hostId;
    final InetAddressAndPort address;
    private final int messagingVersion;
    private final BooleanSupplier abortRequested;

    private InputPosition currentPagePosition;

    private HintsDispatcher(HintsReader reader, UUID hostId, InetAddressAndPort address, int messagingVersion, BooleanSupplier abortRequested)
    {
        currentPagePosition = null;

        this.reader = reader;
        this.hostId = hostId;
        this.address = address;
        this.messagingVersion = messagingVersion;
        this.abortRequested = abortRequested;
    }

    static HintsDispatcher create(File file, RateLimiter rateLimiter, InetAddressAndPort address, UUID hostId, BooleanSupplier abortRequested)
    {
        int messagingVersion = MessagingService.instance().versions.get(address);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13457
        HintsDispatcher dispatcher = new HintsDispatcher(HintsReader.open(file, rateLimiter), hostId, address, messagingVersion, abortRequested);
        HintDiagnostics.dispatcherCreated(dispatcher);
        return dispatcher;
    }

    public void close()
    {
        HintDiagnostics.dispatcherClosed(this);
        reader.close();
    }

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11960
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11960
            currentPagePosition = page.position;
            if (dispatch(page) != Action.CONTINUE)
                return false;
        }

        return true;
    }

    /**
     * @return offset of the first non-delivered page
     */
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11960
    InputPosition dispatchPosition()
    {
        return currentPagePosition;
    }


    // retry in case of a timeout; stop in case of a failure, host going down, or delivery paused
    private Action dispatch(HintsReader.Page page)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13457
        HintDiagnostics.dispatchPage(this);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13124
        return sendHintsAndAwait(page);
    }

    private Action sendHintsAndAwait(HintsReader.Page page)
    {
        Collection<Callback> callbacks = new ArrayList<>();

        /*
         * If hints file messaging version matches the version of the target host, we'll use the optimised path -
         * skipping the redundant decoding/encoding cycle of the already encoded hint.
         *
         * If that is not the case, we'll need to perform conversion to a newer (or an older) format, and decoding the hint
         * is an unavoidable intermediate step.
         */
        Action action = reader.descriptor().messagingVersion() == messagingVersion
                      ? sendHints(page.buffersIterator(), callbacks, this::sendEncodedHint)
                      : sendHints(page.hintsIterator(), callbacks, this::sendHint);

        if (action == Action.ABORT)
            return action;

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13457
        long success = 0, failures = 0, timeouts = 0;
        for (Callback cb : callbacks)
        {
            Callback.Outcome outcome = cb.await();
            if (outcome == Callback.Outcome.SUCCESS) success++;
            else if (outcome == Callback.Outcome.FAILURE) failures++;
            else if (outcome == Callback.Outcome.TIMEOUT) timeouts++;
        }

        updateMetrics(success, failures, timeouts);

        if (failures > 0 || timeouts > 0)
        {
            HintDiagnostics.pageFailureResult(this, success, failures, timeouts);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11960
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13124
            return Action.ABORT;
        }
        else
        {
            HintDiagnostics.pageSuccessResult(this, success, failures, timeouts);
            return Action.CONTINUE;
        }
    }

    private void updateMetrics(long success, long failures, long timeouts)
    {
        HintsServiceMetrics.hintsSucceeded.mark(success);
        HintsServiceMetrics.hintsFailed.mark(failures);
        HintsServiceMetrics.hintsTimedOut.mark(timeouts);
    }

    /*
     * Sending hints in compatibility mode.
     */

    private <T> Action sendHints(Iterator<T> hints, Collection<Callback> callbacks, Function<T, Callback> sendFunction)
    {
        while (hints.hasNext())
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11960
            if (abortRequested.getAsBoolean())
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13457
                HintDiagnostics.abortRequested(this);
                return Action.ABORT;
            }
            callbacks.add(sendFunction.apply(hints.next()));
        }
        return Action.CONTINUE;
    }

    private Callback sendHint(Hint hint)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13234
        Callback callback = new Callback(hint.creationTime);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
        Message<?> message = Message.out(HINT_REQ, new HintMessage(hostId, hint));
        MessagingService.instance().sendWithCallback(message, address, callback);
        return callback;
    }

    /*
     * Sending hints in raw mode.
     */

    private Callback sendEncodedHint(ByteBuffer hint)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
        HintMessage.Encoded message = new HintMessage.Encoded(hostId, hint, messagingVersion);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13234
        Callback callback = new Callback(message.getHintCreationTime());
        MessagingService.instance().sendWithCallback(Message.out(HINT_REQ, message), address, callback);
        return callback;
    }

    private static final class Callback implements RequestCallback
    {
        enum Outcome { SUCCESS, TIMEOUT, FAILURE, INTERRUPTED }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13308

        private final long start = approxTime.now();
        private final SimpleCondition condition = new SimpleCondition();
        private volatile Outcome outcome;
        private final long hintCreationNanoTime;

        private Callback(long hintCreationTimeMillisSinceEpoch)
        {
            this.hintCreationNanoTime = approxTime.translate().fromMillisSinceEpoch(hintCreationTimeMillisSinceEpoch);
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13308
                logger.warn("Hint dispatch was interrupted", e);
                return Outcome.INTERRUPTED;
            }

            return timedOut ? Outcome.TIMEOUT : outcome;
        }

        @Override
        public boolean invokeOnFailure()
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
            return true;
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            outcome = Outcome.FAILURE;
            condition.signalAll();
        }

        @Override
        public void onResponse(Message msg)
        {
            HintsServiceMetrics.updateDelayMetrics(msg.from(), approxTime.now() - this.hintCreationNanoTime);
            outcome = Outcome.SUCCESS;
            condition.signalAll();
        }

        @Override
        public boolean supportsBackPressure()
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9318
            return true;
        }
    }
}
