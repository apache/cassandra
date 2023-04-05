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

package org.apache.cassandra.locator;

import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.TokenMetadataEvent.TokenMetadataEventType;

/**
 * Utility methods for events related to {@link TokenMetadata} changes.
 */
final class TokenMetadataDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private TokenMetadataDiagnostics()
    {
    }

    static void pendingRangeCalculationStarted(TokenMetadata tokenMetadata, String keyspace)
    {
        if (isEnabled(TokenMetadataEventType.PENDING_RANGE_CALCULATION_STARTED))
            service.publish(new TokenMetadataEvent(TokenMetadataEventType.PENDING_RANGE_CALCULATION_STARTED, tokenMetadata, keyspace));
    }

    private static boolean isEnabled(TokenMetadataEventType type)
    {
        return service.isEnabled(TokenMetadataEvent.class, type);
    }

}
