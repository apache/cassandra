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

package org.apache.cassandra.simulator;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.net.Verb.ACCORD_FETCH_TOPOLOGY_REQ;
import static org.apache.cassandra.net.Verb.ACCORD_FETCH_TOPOLOGY_RSP;
import static org.apache.cassandra.net.Verb.ACCORD_FETCH_WATERMARKS_REQ;
import static org.apache.cassandra.net.Verb.ACCORD_FETCH_WATERMARKS_RSP;
import static org.apache.cassandra.net.Verb.ACCORD_GET_DURABLE_BEFORE_REQ;
import static org.apache.cassandra.net.Verb.ACCORD_GET_DURABLE_BEFORE_RSP;
import static org.apache.cassandra.net.Verb.ACCORD_SYNC_NOTIFY_REQ;
import static org.apache.cassandra.net.Verb.ACCORD_SYNC_NOTIFY_RSP;

/**
 * Scheduler for Accord verbs to make certain Accord messages more reliable
 */
public class AccordNetworkScheduler implements FutureActionScheduler
{
    public final FutureActionScheduler defaultScheduler;
    public final FutureActionScheduler semiReliableScheduler;

    public static final Verb[] ACCORD_VERBS;

    static
    {
        ACCORD_VERBS = Arrays.asList(Verb.values()).stream().filter(verb -> verb.name().startsWith("ACCORD_")).collect(Collectors.toList()).toArray(new Verb[0]);
    }

    public static final EnumSet<Verb> UNRELIABLE_ACCORD_VERBS = EnumSet.of(
        ACCORD_FETCH_WATERMARKS_REQ,
        ACCORD_FETCH_WATERMARKS_RSP,
        ACCORD_GET_DURABLE_BEFORE_REQ,
        ACCORD_GET_DURABLE_BEFORE_RSP,
        ACCORD_FETCH_TOPOLOGY_REQ,
        ACCORD_FETCH_TOPOLOGY_RSP,
        ACCORD_SYNC_NOTIFY_REQ,
        ACCORD_SYNC_NOTIFY_RSP
    );

    public AccordNetworkScheduler(FutureActionScheduler defaultScheduler, FutureActionScheduler semiReliableScheduler)
    {
        this.defaultScheduler = defaultScheduler;
        this.semiReliableScheduler = semiReliableScheduler;
    }

    @Override
    public DeliverResult shouldDeliver(int from, int to, IInvokableInstance invoker, IMessage message)
    {
        DeliverResult deliverResult;
        boolean protectedMessage = isProtectedMessage(from, to, invoker, message);
        if (protectedMessage)
            deliverResult = semiReliableScheduler.shouldDeliver(from, to, invoker, message);
        else
            deliverResult = defaultScheduler.shouldDeliver(from, to, invoker, message);
        return new DeliverResult(deliverResult.deliver, protectedMessage | deliverResult.protectedMessage);
    }

    @Override
    public long messageDeadlineNanos(int from, int to, boolean protectedMessage)
    {
        return protectedMessage ? semiReliableScheduler.messageDeadlineNanos(from, to, protectedMessage) : defaultScheduler.messageDeadlineNanos(from, to, protectedMessage);
    }

    @Override
    public long messageTimeoutNanos(long expiresAfterNanos, long expirationIntervalNanos, boolean protectedMessage)
    {
        return protectedMessage ? semiReliableScheduler.messageTimeoutNanos(expiresAfterNanos, expirationIntervalNanos, protectedMessage) : defaultScheduler.messageTimeoutNanos(expiresAfterNanos, expirationIntervalNanos, protectedMessage);
    }

    @Override
    public long messageFailureNanos(int from, int to, boolean protectedMessage)
    {
        return protectedMessage ? semiReliableScheduler.messageTimeoutNanos(from, to, protectedMessage) : defaultScheduler.messageFailureNanos(from, to, protectedMessage);
    }

    @Override
    public long schedulerDelayNanos()
    {
        return 1;
    }

    private final SetMultimap<Long, Long> inFlightRequestsByNodes = Multimaps.newSetMultimap(new ConcurrentHashMap<>(), () -> Collections.newSetFromMap(new ConcurrentHashMap<>()));

    private boolean isProtectedMessage(int from, int to, IInvokableInstance invoker, IMessage message)
    {
        return true;
//        Verb verb = Verb.fromId(message.verb());
//        // These ones we let be unreliable and don't bother to deserialize to see if it's something where we can infer
//        // it is related to an exclusive sync point which we want to be more reliable
//        if (UNRELIABLE_ACCORD_VERBS.contains(verb))
//            return false;
//
//        // Check if this a protected response to a protected request
//        if (verb.name().endsWith("_RSP"))
//        {
//            long callbackKey = ((long) from << 32) | (to & 0xFFFFFFFFL);
//            Set<Long> inFlightRequests = inFlightRequestsByNodes.get(callbackKey);
//            if (inFlightRequests != null && inFlightRequests.remove(message.id()))
//                return true;
//        }
//
//        Long[] checkResult = invoker.unsafeCallOnThisThread(() -> {
//            try
//            {
//                Message<?> m = Message.serializer.deserialize(new DataInputBuffer(message.bytes()), (InetAddressAndPort) null, message.version());
//                Object payload = m.payload;
//                boolean shouldProtect = false;
//                boolean noHandler = false;
//                if (payload instanceof TxnRequest)
//                {
//                    TxnRequest<?> txnRequest = (TxnRequest<?>)payload;
//                    shouldProtect = txnRequest.txnId.isSyncPoint();
//                }
//                else if (payload instanceof SetGloballyDurable ||
//                         payload instanceof SetShardDurable ||
//                         payload instanceof GetDurableBefore ||
//                         payload instanceof GetMaxConflict)
//                {
//                    shouldProtect = true;
//                }
//                else
//                {
//                    noHandler = true;
//                }
//
//                if (shouldProtect)
//                    return new Long[]{ 1L, m.id()};
//                else
//                    return new Long[] { 0L, null };
//            }
//            catch (IOException e)
//            {
//                throw new RuntimeException(e);
//            }
//        });
//
//        boolean shouldBeProtected = checkResult[0] != 0;
//        // If this is a protected request expecting a response then store the callback id here so that
//        // the response message is also protected
//        if (shouldBeProtected && checkResult[1] != null)
//        {
//            checkState(verb.name().endsWith("_REQ"));
//            long callbackKey = ((long) to << 32) | (from & 0xFFFFFFFFL);
//            inFlightRequestsByNodes.put(callbackKey, message.idAsLong());
//        }
//        return shouldBeProtected;
    }
}
