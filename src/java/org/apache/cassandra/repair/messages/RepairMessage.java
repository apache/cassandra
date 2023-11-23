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
package org.apache.cassandra.repair.messages;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.RepairRetrySpec;
import org.apache.cassandra.config.RetrySpec;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Backoff;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.net.MessageFlag.CALL_BACK_ON_FAILURE;

/**
 * Base class of all repair related request/response messages.
 *
 * @since 2.0
 */
public abstract class RepairMessage
{
    private enum ErrorHandling { NONE, TIMEOUT, RETRY }
    @VisibleForTesting
    static final CassandraVersion SUPPORTS_RETRY = new CassandraVersion("5.0.0-alpha2.SNAPSHOT");
    private static final Map<Verb, CassandraVersion> VERB_TIMEOUT_VERSIONS;
    public static final Set<Verb> ALLOWS_RETRY;
    private static final Set<Verb> SUPPORTS_RETRY_WITHOUT_VERSION_CHECK = Collections.unmodifiableSet(EnumSet.of(Verb.CLEANUP_MSG));
    public static final RequestCallback<Object> NOOP_CALLBACK = new RequestCallback<>()
    {
        @Override
        public void onResponse(Message<Object> msg)
        {
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
        }
    };

    static
    {
        CassandraVersion timeoutVersion = new CassandraVersion("4.0.7-SNAPSHOT");
        EnumMap<Verb, CassandraVersion> map = new EnumMap<>(Verb.class);
        map.put(Verb.VALIDATION_REQ, timeoutVersion);
        map.put(Verb.SYNC_REQ, timeoutVersion);
        map.put(Verb.VALIDATION_RSP, SUPPORTS_RETRY);
        map.put(Verb.SYNC_RSP, SUPPORTS_RETRY);
        // IR messages
        map.put(Verb.PREPARE_CONSISTENT_REQ, SUPPORTS_RETRY);
        map.put(Verb.PREPARE_CONSISTENT_RSP, SUPPORTS_RETRY);
        map.put(Verb.FINALIZE_PROPOSE_MSG, SUPPORTS_RETRY);
        map.put(Verb.FINALIZE_PROMISE_MSG, SUPPORTS_RETRY);
        map.put(Verb.FINALIZE_COMMIT_MSG, SUPPORTS_RETRY);
        map.put(Verb.FAILED_SESSION_MSG, SUPPORTS_RETRY);
        VERB_TIMEOUT_VERSIONS = Collections.unmodifiableMap(map);

        EnumSet<Verb> allowsRetry = EnumSet.noneOf(Verb.class);
        allowsRetry.add(Verb.PREPARE_MSG);
        allowsRetry.add(Verb.VALIDATION_REQ);
        allowsRetry.add(Verb.VALIDATION_RSP);
        allowsRetry.add(Verb.SYNC_REQ);
        allowsRetry.add(Verb.SYNC_RSP);
        allowsRetry.add(Verb.SNAPSHOT_MSG);
        allowsRetry.add(Verb.CLEANUP_MSG);
        // IR messages
        allowsRetry.add(Verb.PREPARE_CONSISTENT_REQ);
        allowsRetry.add(Verb.PREPARE_CONSISTENT_RSP);
        allowsRetry.add(Verb.FINALIZE_PROPOSE_MSG);
        allowsRetry.add(Verb.FINALIZE_PROMISE_MSG);
        allowsRetry.add(Verb.FINALIZE_COMMIT_MSG);
        allowsRetry.add(Verb.FAILED_SESSION_MSG);
        ALLOWS_RETRY = Collections.unmodifiableSet(allowsRetry);
    }

    private static final Logger logger = LoggerFactory.getLogger(RepairMessage.class);
    private static final NoSpamLogger noSpam = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    @Nullable
    public final RepairJobDesc desc;

    protected RepairMessage(@Nullable RepairJobDesc desc)
    {
        this.desc = desc;
    }

    public TimeUUID parentRepairSession()
    {
        return desc.parentSessionId;
    }

    public interface RepairFailureCallback
    {
        void onFailure(Exception e);
    }

    private static Backoff backoff(SharedContext ctx, Verb verb)
    {
        RepairRetrySpec retrySpec = DatabaseDescriptor.getRepairRetrySpec();
        RetrySpec spec = verb == Verb.VALIDATION_RSP ? retrySpec.getMerkleTreeResponseSpec() : retrySpec;
        if (!spec.isEnabled())
            return Backoff.None.INSTANCE;
        return new Backoff.ExponentialBackoff(spec.maxAttempts.value, spec.baseSleepTime.toMilliseconds(), spec.maxSleepTime.toMilliseconds(), ctx.random().get()::nextDouble);
    }

    public static Supplier<Boolean> notDone(Future<?> f)
    {
        return () -> !f.isDone();
    }

    public static Supplier<Boolean> always()
    {
        return () -> true;
    }

    public static <T> void sendMessageWithRetries(SharedContext ctx, Supplier<Boolean> allowRetry, RepairMessage request, Verb verb, InetAddressAndPort endpoint, RequestCallback<T> finalCallback)
    {
        sendMessageWithRetries(ctx, backoff(ctx, verb), allowRetry, request, verb, endpoint, finalCallback, 0);
    }

    public static <T> void sendMessageWithRetries(SharedContext ctx, RepairMessage request, Verb verb, InetAddressAndPort endpoint, RequestCallback<T> finalCallback)
    {
        sendMessageWithRetries(ctx, backoff(ctx, verb), always(), request, verb, endpoint, finalCallback, 0);
    }

    public static void sendMessageWithRetries(SharedContext ctx, RepairMessage request, Verb verb, InetAddressAndPort endpoint)
    {
        sendMessageWithRetries(ctx, backoff(ctx, verb), always(), request, verb, endpoint, NOOP_CALLBACK, 0);
    }

    public static void sendMessageWithRetries(SharedContext ctx, Supplier<Boolean> allowRetry, RepairMessage request, Verb verb, InetAddressAndPort endpoint)
    {
        sendMessageWithRetries(ctx, backoff(ctx, verb), allowRetry, request, verb, endpoint, NOOP_CALLBACK, 0);
    }

    @VisibleForTesting
    static <T> void sendMessageWithRetries(SharedContext ctx, Backoff backoff, Supplier<Boolean> allowRetry, RepairMessage request, Verb verb, InetAddressAndPort endpoint, RequestCallback<T> finalCallback, int attempt)
    {
        if (!ALLOWS_RETRY.contains(verb))
            throw new AssertionError("Repair verb " + verb + " does not support retry, but a request to send with retry was given!");
        RequestCallback<T> callback = new RequestCallback<>()
        {
            @Override
            public void onResponse(Message<T> msg)
            {
                maybeRecordRetry(null);
                finalCallback.onResponse(msg);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                ErrorHandling allowed = errorHandlingSupported(ctx, endpoint, verb, request.parentRepairSession());
                switch (allowed)
                {
                    case NONE:
                        logger.error("[#{}] {} failed on {}: {}", request.parentRepairSession(), verb, from, failureReason);
                        return;
                    case TIMEOUT:
                        finalCallback.onFailure(from, failureReason);
                        return;
                    case RETRY:
                        int maxAttempts = backoff.maxAttempts();
                        if (failureReason == RequestFailureReason.TIMEOUT && attempt < maxAttempts && allowRetry.get())
                        {
                            ctx.optionalTasks().schedule(() -> sendMessageWithRetries(ctx, backoff, allowRetry, request, verb, endpoint, finalCallback, attempt + 1),
                                                         backoff.computeWaitTime(attempt), backoff.unit());
                            return;
                        }
                        maybeRecordRetry(failureReason);
                        finalCallback.onFailure(from, failureReason);
                        return;
                    default:
                        throw new AssertionError("Unknown error handler: " + allowed);
                }
            }

            private void maybeRecordRetry(@Nullable RequestFailureReason reason)
            {
                if (attempt <= 0)
                    return;
                // we don't know what the prefix kind is... so use NONE... this impacts logPrefix as it will cause us to use "repair" rather than "preview repair" which may not be correct... but close enough...
                String prefix = PreviewKind.NONE.logPrefix(request.parentRepairSession());
                RepairMetrics.retry(verb, attempt);
                if (reason == null)
                {
                    noSpam.info("{} Retry of repair verb " + verb + " was successful after {} attempts", prefix, attempt);
                }
                else if (reason == RequestFailureReason.TIMEOUT)
                {
                    noSpam.warn("{} Timeout for repair verb " + verb + "; could not complete within {} attempts", prefix, attempt);
                    RepairMetrics.retryTimeout(verb);
                }
                else
                {
                    noSpam.warn("{} {} failure for repair verb " + verb + "; could not complete within {} attempts", prefix, reason, attempt);
                    RepairMetrics.retryFailure(verb);
                }
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }
        };
        ctx.messaging().sendWithCallback(Message.outWithFlag(verb, request, CALL_BACK_ON_FAILURE),
                                         endpoint,
                                         callback);
    }

    public static void sendMessageWithFailureCB(SharedContext ctx, Supplier<Boolean> allowRetry, RepairMessage request, Verb verb, InetAddressAndPort endpoint, RepairFailureCallback failureCallback)
    {
        RequestCallback<?> callback = new RequestCallback<>()
        {
            @Override
            public void onResponse(Message<Object> msg)
            {
                logger.info("[#{}] {} received by {}", request.parentRepairSession(), verb, endpoint);
                // todo: at some point we should make repair messages follow the normal path, actually using this
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                failureCallback.onFailure(RepairException.error(request.desc, PreviewKind.NONE, String.format("Got %s failure from %s: %s", verb, from, failureReason)));
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }
        };
        sendMessageWithRetries(ctx, allowRetry, request, verb, endpoint, callback);
    }

    private static ErrorHandling errorHandlingSupported(SharedContext ctx, InetAddressAndPort from, Verb verb, TimeUUID parentSessionId)
    {
        if (SUPPORTS_RETRY_WITHOUT_VERSION_CHECK.contains(verb))
            return ErrorHandling.RETRY;
        // Repair in mixed mode isn't fully supported, but also not activally blocked... so in the common case all participants
        // will be on the same version as this instance, so can avoid the lookup from gossip
        CassandraVersion remoteVersion = ctx.gossiper().getReleaseVersion(from);
        if (remoteVersion == null)
        {
            if (VERB_TIMEOUT_VERSIONS.containsKey(verb))
            {
                logger.warn("[#{}] Not failing repair due to remote host {} not supporting repair message timeouts (version is unknown)", parentSessionId, from);
                return ErrorHandling.NONE;
            }
            return ErrorHandling.TIMEOUT;
        }
        if (remoteVersion.compareTo(SUPPORTS_RETRY) >= 0)
            return ErrorHandling.RETRY;
        CassandraVersion timeoutVersion = VERB_TIMEOUT_VERSIONS.get(verb);
        if (timeoutVersion == null || remoteVersion.compareTo(timeoutVersion) >= 0)
            return ErrorHandling.TIMEOUT;
        return ErrorHandling.NONE;
    }

    public static void sendFailureResponse(SharedContext ctx, Message<?> respondTo)
    {
        Message<?> reply = respondTo.failureResponse(RequestFailureReason.UNKNOWN);
        ctx.messaging().send(reply, respondTo.from());
    }

    public static void sendAck(SharedContext ctx, Message<? extends RepairMessage> message)
    {
        ctx.messaging().send(message.emptyResponse(), message.from());
    }
}
