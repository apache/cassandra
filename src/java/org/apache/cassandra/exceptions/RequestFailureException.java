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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;

public class RequestFailureException extends RequestExecutionException
{
    public final ConsistencyLevel consistency;
    public final int received;
    public final int blockFor;
    public final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint;

    protected RequestFailureException(ExceptionCode code, ConsistencyLevel consistency, int received, int blockFor, Map<InetAddress, RequestFailureReason> failureReasonByEndpoint)
    {
        super(code, String.format("Operation failed - received %d responses and %d failures", received, failureReasonByEndpoint.size()));
        this.consistency = consistency;
        this.received = received;
        this.blockFor = blockFor;

        // It is possible for the passed in failureReasonByEndpoint map
        // to have new entries added after this exception is constructed
        // (e.g. a delayed failure response from a replica). So to be safe
        // we make a copy of the map at this point to ensure it will not be
        // modified any further. Otherwise, there could be implications when
        // we encode this map for transport.
        this.failureReasonByEndpoint = new HashMap<>(failureReasonByEndpoint);
    }
}
