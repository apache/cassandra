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
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.RepairRetrySpec;
import org.apache.cassandra.config.RetrySpec;
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
    private static final CassandraVersion SUPPORTS_RETRY = new CassandraVersion("5.0.0-alpha2.SNAPSHOT");
    private static final Map<Verb, CassandraVersion> VERB_TIMEOUT_VERSIONS;

    static
    {
        CassandraVersion timeoutVersion = new CassandraVersion("4.0.7-SNAPSHOT");
        EnumMap<Verb, CassandraVersion> map = new EnumMap<>(Verb.class);
        map.put(Verb.VALIDATION_REQ, timeoutVersion);
        map.put(Verb.SYNC_REQ, timeoutVersion);
        map.put(Verb.VALIDATION_RSP, SUPPORTS_RETRY);
        map.put(Verb.SYNC_RSP, SUPPORTS_RETRY);
        VERB_TIMEOUT_VERSIONS = Collections.unmodifiableMap(map);
    }
    private static final Set<Verb> SUPPORTS_RETRY_WITHOUT_VERSION_CHECK = Collections.unmodifiableSet(EnumSet.of(Verb.CLEANUP_MSG));

    private static final Logger logger = LoggerFactory.getLogger(RepairMessage.class);
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

    private static Supplier<Boolean> always()
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
        sendMessageWithRetries(ctx, backoff(ctx, verb), always(), request, verb, endpoint, new RequestCallback<>()
        {
            @Override
            public void onResponse(Message<Object> msg)
            {
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
            }
        }, 0);
    }

    private static <T> void sendMessageWithRetries(SharedContext ctx, Backoff backoff, Supplier<Boolean> allowRetry, RepairMessage request, Verb verb, InetAddressAndPort endpoint, RequestCallback<T> finalCallback, int attempt)
    {
        RequestCallback<T> callback = new RequestCallback<>()
        {
            @Override
            public void onResponse(Message<T> msg)
            {
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
                        finalCallback.onFailure(from, failureReason);
                        return;
                    default:
                        throw new AssertionError("Unknown error handler: " + allowed);
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
}
