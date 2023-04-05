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

package org.apache.cassandra.dht.tokenallocator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorBase.TokenInfo;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorBase.UnitInfo;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorBase.Weighted;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorEvent.TokenAllocatorEventType;
import org.apache.cassandra.diag.DiagnosticEventService;

/**
 * Utility methods for DiagnosticEvent around {@link TokenAllocator} activities.
 */
final class TokenAllocatorDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private TokenAllocatorDiagnostics()
    {
    }

    static <Unit> void noReplicationTokenAllocatorInstanciated(NoReplicationTokenAllocator<Unit> allocator)
    {
        if (isEnabled(TokenAllocatorEventType.NO_REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.NO_REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED,
                                                      allocator, null, null, null, null, null, null, null));
    }

    static <Unit> void replicationTokenAllocatorInstanciated(ReplicationAwareTokenAllocator<Unit> allocator)
    {
        if (isEnabled(TokenAllocatorEventType.REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED,
                                                      allocator, null, null, null,null, null, null, null));
    }

    static <Unit> void unitedAdded(TokenAllocatorBase<Unit> allocator, int numTokens,
                                   Queue<Weighted<UnitInfo>> sortedUnits, NavigableMap<Token, Unit> sortedTokens,
                                   List<Token> tokens, Unit unit)
    {
        if (isEnabled(TokenAllocatorEventType.UNIT_ADDED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.UNIT_ADDED,
                                                      allocator,
                                                      numTokens,
                                                      ImmutableList.copyOf(sortedUnits),
                                                      null,
                                                      ImmutableMap.copyOf(sortedTokens),
                                                      ImmutableList.copyOf(tokens),
                                                      unit,
                                                      null));
    }

    static <Unit> void unitedAdded(TokenAllocatorBase<Unit> allocator, int numTokens,
                                   Multimap<Unit, Token> unitToTokens, NavigableMap<Token, Unit> sortedTokens,
                                   List<Token> tokens, Unit unit)
    {
        if (isEnabled(TokenAllocatorEventType.UNIT_ADDED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.UNIT_ADDED,
                                                      allocator,
                                                      numTokens,
                                                      null,
                                                      ImmutableMap.copyOf(unitToTokens.asMap()),
                                                      ImmutableMap.copyOf(sortedTokens),
                                                      ImmutableList.copyOf(tokens),
                                                      unit,
                                                      null));
    }


    static <Unit> void unitRemoved(TokenAllocatorBase<Unit> allocator, Unit unit,
                                   Queue<Weighted<UnitInfo>> sortedUnits, Map<Token, Unit> sortedTokens)
    {
        if (isEnabled(TokenAllocatorEventType.UNIT_REMOVED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.UNIT_REMOVED,
                                                      allocator,
                                                      null,
                                                      ImmutableList.copyOf(sortedUnits),
                                                      null,
                                                      ImmutableMap.copyOf(sortedTokens),
                                                      null,
                                                      unit,
                                                      null));
    }

    static <Unit> void unitRemoved(TokenAllocatorBase<Unit> allocator, Unit unit,
                                   Multimap<Unit, Token> unitToTokens, Map<Token, Unit> sortedTokens)
    {
        if (isEnabled(TokenAllocatorEventType.UNIT_REMOVED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.UNIT_REMOVED,
                                                      allocator,
                                                      null,
                                                      null,
                                                      ImmutableMap.copyOf(unitToTokens.asMap()),
                                                      ImmutableMap.copyOf(sortedTokens),
                                                      null,
                                                      unit,
                                                      null));
    }

    static <Unit> void tokenInfosCreated(TokenAllocatorBase<Unit> allocator, Queue<Weighted<UnitInfo>> sortedUnits,
                                         Map<Token, Unit> sortedTokens, TokenInfo<Unit> tokenInfo)
    {
        if (isEnabled(TokenAllocatorEventType.TOKEN_INFOS_CREATED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.TOKEN_INFOS_CREATED,
                                                      allocator,
                                                      null,
                                                      ImmutableList.copyOf(sortedUnits),
                                                      null,
                                                      ImmutableMap.copyOf(sortedTokens),
                                                      null,
                                                      null,
                                                      tokenInfo));
    }

    static <Unit> void tokenInfosCreated(TokenAllocatorBase<Unit> allocator, Multimap<Unit, Token> unitToTokens,
                                         TokenInfo<Unit> tokenInfo)
    {
        if (isEnabled(TokenAllocatorEventType.TOKEN_INFOS_CREATED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.TOKEN_INFOS_CREATED,
                                                      allocator,
                                                      null,
                                                      null,
                                                      ImmutableMap.copyOf(unitToTokens.asMap()),
                                                      null,
                                                      null,
                                                      null,
                                                      tokenInfo));
    }

    static <Unit> void splitsGenerated(TokenAllocatorBase<Unit> allocator,
                                       int numTokens, Queue<Weighted<UnitInfo>> sortedUnits,
                                       NavigableMap<Token, Unit> sortedTokens,
                                       Unit newUnit,
                                       Collection<Token> tokens)
    {
        if (isEnabled(TokenAllocatorEventType.RANDOM_TOKENS_GENERATED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.RANDOM_TOKENS_GENERATED,
                                                      allocator,
                                                      numTokens,
                                                      ImmutableList.copyOf(sortedUnits),
                                                      null,
                                                      ImmutableMap.copyOf(sortedTokens),
                                                      ImmutableList.copyOf(tokens),
                                                      newUnit,
                                                      null));
    }

    static <Unit> void splitsGenerated(TokenAllocatorBase<Unit> allocator,
                                       int numTokens, Multimap<Unit, Token> unitToTokens,
                                       NavigableMap<Token, Unit> sortedTokens, Unit newUnit,
                                       Collection<Token> tokens)
    {
        if (isEnabled(TokenAllocatorEventType.RANDOM_TOKENS_GENERATED))
            service.publish(new TokenAllocatorEvent<>(TokenAllocatorEventType.RANDOM_TOKENS_GENERATED,
                                                      allocator,
                                                      numTokens,
                                                      null,
                                                      ImmutableMap.copyOf(unitToTokens.asMap()),
                                                      ImmutableMap.copyOf(sortedTokens),
                                                      ImmutableList.copyOf(tokens),
                                                      newUnit,
                                                      null));
    }

    private static boolean isEnabled(TokenAllocatorEventType type)
    {
        return service.isEnabled(TokenAllocatorEvent.class, type);
    }

}
