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

package org.apache.cassandra.dht;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableCollection;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;

/**
 * DiagnosticEvent implementation for bootstrap related activities.
 */
final class BootstrapEvent extends DiagnosticEvent
{

    private final BootstrapEventType type;
    @Nullable
    private final TokenMetadata tokenMetadata;
    private final InetAddressAndPort address;
    @Nullable
    private final String allocationKeyspace;
    @Nullable
    private final Integer rf;
    private final Integer numTokens;
    private final Collection<Token> tokens;

    BootstrapEvent(BootstrapEventType type, InetAddressAndPort address, @Nullable TokenMetadata tokenMetadata,
                   @Nullable String allocationKeyspace, @Nullable Integer rf, int numTokens, ImmutableCollection<Token> tokens)
    {
        this.type = type;
        this.address = address;
        this.tokenMetadata = tokenMetadata;
        this.allocationKeyspace = allocationKeyspace;
        this.rf = rf;
        this.numTokens = numTokens;
        this.tokens = tokens;
    }

    enum BootstrapEventType
    {
        BOOTSTRAP_USING_SPECIFIED_TOKENS,
        BOOTSTRAP_USING_RANDOM_TOKENS,
        TOKENS_ALLOCATED
    }


    public BootstrapEventType getType()
    {
        return type;
    }

    public Map<String, Serializable> toMap()
    {
        // be extra defensive against nulls and bugs
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("tokenMetadata", String.valueOf(tokenMetadata));
        ret.put("allocationKeyspace", allocationKeyspace);
        ret.put("rf", rf);
        ret.put("numTokens", numTokens);
        ret.put("tokens", String.valueOf(tokens));
        return ret;
    }
}
