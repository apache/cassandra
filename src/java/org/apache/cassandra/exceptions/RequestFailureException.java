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
import java.util.stream.Collectors;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.locator.InetAddressAndPort;

public class RequestFailureException extends RequestExecutionException
{
    public final ConsistencyLevel consistency;
    public final int received;
    public final int blockFor;
    public final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;

    protected RequestFailureException(ExceptionCode code, ConsistencyLevel consistency, int received, int blockFor, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(code, buildErrorMessage(received, failureReasonByEndpoint));
        this.consistency = consistency;
        this.received = received;
        this.blockFor = blockFor;
        this.failureReasonByEndpoint = failureReasonByEndpoint;
    }

    private static String buildErrorMessage(int received, Map<InetAddressAndPort, RequestFailureReason> failures)
    {
        return String.format("Operation failed - received %d responses and %d failures: %s",
                             received,
                             failures.size(),
                             buildFailureString(failures));
    }

    private static String buildFailureString(Map<InetAddressAndPort, RequestFailureReason> failures)
    {
        return failures.entrySet().stream()
                       .map(e -> String.format("%s from %s", e.getValue(), e.getKey()))
                       .collect(Collectors.joining(", "));
    }
}
