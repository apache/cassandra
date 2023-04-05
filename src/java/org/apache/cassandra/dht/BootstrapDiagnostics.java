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

import java.util.Collection;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.dht.BootstrapEvent.BootstrapEventType;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;

/**
 * Utility methods for bootstrap related activities.
 */
final class BootstrapDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private BootstrapDiagnostics()
    {
    }

    static void useSpecifiedTokens(InetAddressAndPort address, String allocationKeyspace, Collection<Token> initialTokens,
                                   int numTokens)
    {
        if (isEnabled(BootstrapEventType.BOOTSTRAP_USING_SPECIFIED_TOKENS))
            service.publish(new BootstrapEvent(BootstrapEventType.BOOTSTRAP_USING_SPECIFIED_TOKENS,
                                               address,
                                               null,
                                               allocationKeyspace,
                                               null,
                                               numTokens,
                                               ImmutableList.copyOf(initialTokens)));
    }

    static void useRandomTokens(InetAddressAndPort address, TokenMetadata metadata, int numTokens, Collection<Token> tokens)
    {
        if (isEnabled(BootstrapEventType.BOOTSTRAP_USING_RANDOM_TOKENS))
            service.publish(new BootstrapEvent(BootstrapEventType.BOOTSTRAP_USING_RANDOM_TOKENS,
                                               address,
                                               metadata.cloneOnlyTokenMap(),
                                               null,
                                               null,
                                               numTokens,
                                               ImmutableList.copyOf(tokens)));
    }

    static void tokensAllocated(InetAddressAndPort address, TokenMetadata metadata,
                                String allocationKeyspace, int numTokens, Collection<Token> tokens)
    {
        if (isEnabled(BootstrapEventType.TOKENS_ALLOCATED))
            service.publish(new BootstrapEvent(BootstrapEventType.TOKENS_ALLOCATED,
                                               address,
                                               metadata.cloneOnlyTokenMap(),
                                               allocationKeyspace,
                                               null,
                                               numTokens,
                                               ImmutableList.copyOf(tokens)));
    }

    static void tokensAllocated(InetAddressAndPort address, TokenMetadata metadata,
                                int rf, int numTokens, Collection<Token> tokens)
    {
        if (isEnabled(BootstrapEventType.TOKENS_ALLOCATED))
            service.publish(new BootstrapEvent(BootstrapEventType.TOKENS_ALLOCATED,
                                               address,
                                               metadata.cloneOnlyTokenMap(),
                                               null,
                                               rf,
                                               numTokens,
                                               ImmutableList.copyOf(tokens)));
    }

    private static boolean isEnabled(BootstrapEventType type)
    {
        return service.isEnabled(BootstrapEvent.class, type);
    }

}
