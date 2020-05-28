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

package org.apache.cassandra.utils;

import java.net.UnknownHostException;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

public class TokenRangeTestUtil
{
    public static final InetAddressAndPort node1;
    public static final InetAddressAndPort broadcastAddress;

    static
    {
        try
        {
            node1 = InetAddressAndPort.getByName("127.0.1.99");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Error initializing InetAddressAndPort");
        }
        broadcastAddress = FBUtilities.getBroadcastAddressAndPort();
    }

    public static UUID uuid()
    {
        return UUIDGen.getTimeUUID();
    }

    public static void setPendingRanges(String keyspace, int... tokens)
    {
        Multimap<Range<Token>, Replica> pending = HashMultimap.create();
        for (Range<Token> range : generateRanges(tokens))
            pending.put(range, new Replica(broadcastAddress, range, true));

        StorageService.instance.getTokenMetadata().setPendingRangesUnsafe(keyspace, pending);
    }

    public static void setLocalTokens(int... tokens)
    {
        List<Token> tokenList = new ArrayList<>();
        for (int token : tokens)
            tokenList.add(token(token));
        StorageService.instance.getTokenMetadata().updateNormalTokens(tokenList, broadcastAddress);
    }

    private static final Random random = new Random();
    public static int randomInt()
    {
        return randomInt(Integer.MAX_VALUE);
    }

    public static int randomInt(int max)
    {
        return random.nextInt(max);
    }

    public static RangesAtEndpoint generateRangesAtEndpoint(InetAddressAndPort endpoint, int... rangePairs)
    {
        if (rangePairs.length % 2 == 1)
            throw new RuntimeException("generateRangesAtEndpoint argument count should be even");

        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(endpoint);

        for (int i = 0; i < rangePairs.length; i += 2)
        {
            builder.add(Replica.fullReplica(endpoint, generateRange(rangePairs[i], rangePairs[i + 1])));
        }
        return builder.build();
    }

    public static List<Range<Token>> generateRanges(int... rangePairs)
    {
        if (rangePairs.length % 2 == 1)
            throw new RuntimeException("generateRanges argument count should be even");

        List<Range<Token>> ranges = new ArrayList<>();

        for (int i = 0; i < rangePairs.length; i += 2)
        {
            ranges.add(generateRange(rangePairs[i], rangePairs[i + 1]));
        }

        return ranges;
    }

    public static Range<Token> generateRange(int left, int right)
    {
        return new Range<>(token(left), token(right));
    }

    public static Token token(int token)
    {
        return new Murmur3Partitioner.LongToken(token);
    }

    public static Token bytesToken(int token)
    {
        return new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(token));
    }

    public static ListenableFuture<MessageDelivery> registerOutgoingMessageSink(Verb... ignored)
    {
        SettableFuture<MessageDelivery> future = SettableFuture.create();
        Set<Verb> ignore = Sets.newHashSet(ignored);

        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().outboundSink.add((Message<?> message, InetAddressAndPort to) -> {
            if (!ignore.contains(message.verb()))
                future.set(new MessageDelivery(message, to));
            return true;
        });

        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().inboundSink.add((Message<?> message) -> false);

        return future;
    }

    public static class MessageDelivery
    {
        public final Message<?> message;
        public final InetAddressAndPort to;

        MessageDelivery(Message<?> message, InetAddressAndPort to)
        {
            this.message = message;
            this.to = to;
        }
    }
}
