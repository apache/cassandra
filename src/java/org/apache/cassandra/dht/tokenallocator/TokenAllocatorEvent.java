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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorBase.TokenInfo;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorBase.UnitInfo;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorBase.Weighted;
import org.apache.cassandra.diag.DiagnosticEvent;

/**
 * DiagnosticEvent implementation for {@link TokenAllocator} activities.
 */
final class TokenAllocatorEvent<Unit> extends DiagnosticEvent
{

    private final TokenAllocatorEventType type;
    private final TokenAllocatorBase<Unit> allocator;
    private final int replicas;
    @Nullable
    private final Integer numTokens;
    @Nullable
    private final Collection<Weighted<UnitInfo>> sortedUnits;
    @Nullable
    private final Map<Unit, Collection<Token>> unitToTokens;
    @Nullable
    private final ImmutableMap<Token, Unit> sortedTokens;
    @Nullable
    private final List<Token> tokens;
    @Nullable
    private final Unit unit;
    @Nullable
    private final TokenInfo<Unit> tokenInfo;

    TokenAllocatorEvent(TokenAllocatorEventType type, TokenAllocatorBase<Unit> allocator, @Nullable Integer numTokens,
                        @Nullable ImmutableList<Weighted<UnitInfo>> sortedUnits, @Nullable ImmutableMap<Unit, Collection<Token>> unitToTokens,
                        @Nullable ImmutableMap<Token, Unit> sortedTokens, @Nullable ImmutableList<Token> tokens, Unit unit,
                        @Nullable TokenInfo<Unit> tokenInfo)
    {
        this.type = type;
        this.allocator = allocator;
        this.replicas = allocator.getReplicas();
        this.numTokens = numTokens;
        this.sortedUnits = sortedUnits;
        this.unitToTokens = unitToTokens;
        this.sortedTokens = sortedTokens;
        this.tokens = tokens;
        this.unit = unit;
        this.tokenInfo = tokenInfo;
    }

    enum TokenAllocatorEventType
    {
        REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED,
        NO_REPLICATION_AWARE_TOKEN_ALLOCATOR_INSTANCIATED,
        UNIT_ADDED,
        UNIT_REMOVED,
        TOKEN_INFOS_CREATED,
        RANDOM_TOKENS_GENERATED,
        TOKENS_ALLOCATED
    }

    public TokenAllocatorEventType getType()
    {
        return type;
    }

    public HashMap<String, Serializable> toMap()
    {
        // be extra defensive against nulls and bugs
        HashMap<String, Serializable> ret = new HashMap<>();
        if (allocator != null)
        {
            if (allocator.partitioner != null) ret.put("partitioner", allocator.partitioner.getClass().getSimpleName());
            if (allocator.strategy != null) ret.put("strategy", allocator.strategy.getClass().getSimpleName());
        }
        ret.put("replicas", replicas);
        ret.put("numTokens", this.numTokens);
        ret.put("sortedUnits", String.valueOf(sortedUnits));
        ret.put("sortedTokens", String.valueOf(sortedTokens));
        ret.put("unitToTokens", String.valueOf(unitToTokens));
        ret.put("tokens", String.valueOf(tokens));
        ret.put("unit", String.valueOf(unit));
        ret.put("tokenInfo", String.valueOf(tokenInfo));
        return ret;
    }
}
