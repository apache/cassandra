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

package org.apache.cassandra.service.accord.exceptions;

import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;

public class ReadExhaustedException extends ReadFailureException
{
    public ReadExhaustedException(ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(consistency, received, blockFor, dataPresent, failureReasonByEndpoint);
    }

    protected ReadExhaustedException(String msg, ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(msg, consistency, received, blockFor, dataPresent, failureReasonByEndpoint);
    }
}
