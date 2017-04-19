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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

/**
 * Dispatches a single hints file to a specified node in a batched manner.
 *
 * Uses either {@link EncodedHintMessage} - when dispatching hints into a node with the same messaging version as the hints file,
 * or {@link HintMessage}, when conversion is required.
 */
final class HintsDispatcher implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(HintsDispatcher.class);

    private enum Action { CONTINUE, ABORT }

    private final HintsReader reader;
    private final UUID hostId;
    private final InetAddress address;
    private final int messagingVersion;
    private final BooleanSupplier abortRequested;

    private InputPosition currentPagePosition;

    private HintsDispatcher(HintsReader reader, UUID hostId, InetAddress address, int messagingVersion, BooleanSupplier abortRequested)
    {
        currentPagePosition = null;

        this.reader = reader;
        this.hostId = hostId;
        this.address = address;
        this.messagingVersion = messagingVersion;
        this.abortRequested = abortRequested;
    }

    static HintsDispatcher create(File file, RateLimiter rateLimiter, InetAddress address, UUID hostId, BooleanSupplier abortRequested)
    {
        int messagingVersion = MessagingService.instance().getVersion(address);
        return new HintsDispatcher(HintsReader.open(file, rateLimiter), hostId, address, messagingVersion, abortRequested);
    }

    public void close()
    {
        reader.close();
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

        boolean hadFailures = false;
        for (Callback cb : callbacks)
        {
            Callback.Outcome outcome = cb.await();
            updateMetrics(outcome);

            if (outcome != Callback.Outcome.SUCCESS)
                hadFailures = true;
        }

        return hadFailures ? Action.ABORT : Action.CONTINUE;
    }

    private void updateMetrics(Callback.Outcome outcome)
    {
        switch (outcome)
        {
            case SUCCESS:
                HintsServiceMetrics.hintsSucceeded.mark();
                break;
            case FAILURE:
                HintsServiceMetrics.hintsFailed.mark();
                break;
            case TIMEOUT:
                HintsServiceMetrics.hintsTimedOut.mark();
                break;
        }
    }

    /*
     * Sending hints in compatibility mode.
     */

    private <T> Action sendHints(Iterator<T> hints, Collection<Callback> callbacks, Function<T, Callback> sendFunction)
    {
        while (hints.hasNext())
        {
            if (abortRequested.getAsBoolean())
                return Action.ABORT;
            callbacks.add(sendFunction.apply(hints.next()));
        }
        return Action.CONTINUE;
    }

    private Callback sendHint(Hint hint)
    {
        Callback callback = new Callback();
        HintMessage message = new HintMessage(hostId, hint);
        MessagingService.instance().sendRRWithFailure(message.createMessageOut(), address, callback);
        return callback;
    }

    /*
     * Sending hints in raw mode.
     */

    private Callback sendEncodedHint(ByteBuffer hint)
    {
        Callback callback = new Callback();
        EncodedHintMessage message = new EncodedHintMessage(hostId, hint, messagingVersion);
        MessagingService.instance().sendRRWithFailure(message.createMessageOut(), address, callback);
        return callback;
    }

    private static final class Callback implements IAsyncCallbackWithFailure
    {
        enum Outcome { SUCCESS, TIMEOUT, FAILURE, INTERRUPTED }

        private final long start = System.nanoTime();
        private final SimpleCondition condition = new SimpleCondition();
        private volatile Outcome outcome;

        Outcome await()
        {
            long timeout = TimeUnit.MILLISECONDS.toNanos(MessagingService.Verb.HINT.getTimeout()) - (System.nanoTime() - start);
            boolean timedOut;

            try
            {
                timedOut = !condition.await(timeout, TimeUnit.NANOSECONDS);
            }
            catch (InterruptedException e)
            {
                logger.warn("Hint dispatch was interrupted", e);
                return Outcome.INTERRUPTED;
            }

            return timedOut ? Outcome.TIMEOUT : outcome;
        }

        public void onFailure(InetAddress from, RequestFailureReason failureReason)
        {
            outcome = Outcome.FAILURE;
            condition.signalAll();
        }

        public void response(MessageIn msg)
        {
            outcome = Outcome.SUCCESS;
            condition.signalAll();
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public boolean supportsBackPressure()
        {
            return true;
        }
    }
}
