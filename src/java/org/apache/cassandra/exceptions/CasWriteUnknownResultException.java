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

package org.apache.cassandra.exceptions;

import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.locator.InetAddressAndPort;

public class CasWriteUnknownResultException extends RequestExecutionException
{
    public final ConsistencyLevel consistency;
    public final int received;
    public final int blockFor;

    public CasWriteUnknownResultException(ConsistencyLevel consistency, int received, int blockFor, String msg)
    {
        super(ExceptionCode.CAS_WRITE_UNKNOWN, msg);
        this.consistency = consistency;
        this.received = received;
        this.blockFor = blockFor;
    }

    /**
     * Always prefer {@link #withParticipants} when you have that information available in context 
     */
    public static CasWriteUnknownResultException withoutParticipants(ConsistencyLevel consistency, int received, int blockFor)
    {
        return new CasWriteUnknownResultException(consistency, received, blockFor, String.format("CAS operation result is unknown - proposal accepted by %d but not a quorum.", received));
    }

    public static CasWriteUnknownResultException withParticipants(ConsistencyLevel consistency, int received, int blockFor, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        return new CasWriteUnknownResultException(consistency, received, blockFor, RequestFailureReason.buildErrorMessage(
                                                      String.format("CAS operation result is unknown - proposal accepted by %d but not a quorum", received),
                                                      failureReasonByEndpoint));
    }
}