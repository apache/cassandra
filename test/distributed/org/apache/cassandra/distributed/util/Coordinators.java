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

package org.apache.cassandra.distributed.util;

import java.util.UUID;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.utils.TimeUUID;

public class Coordinators
{
    public static class WithTrace
    {
        public final SimpleQueryResult result, trace;

        public WithTrace(SimpleQueryResult result, SimpleQueryResult trace)
        {
            this.result = result;
            this.trace = trace;
        }
    }

    public static WithTrace withTracing(ICoordinator coordinator, String query, ConsistencyLevel consistencyLevel, Object... boundValues)
    throws WithTraceException
    {
        UUID session = TimeUUID.Generator.nextTimeAsUUID();
        try
        {
            SimpleQueryResult result = coordinator.executeWithTracingWithResult(session, query, consistencyLevel, boundValues);
            return new WithTrace(result, getTrace(coordinator, session));
        }
        catch (Throwable t)
        {
            throw new WithTraceException(t, getTrace(coordinator, session));
        }
    }

    public static SimpleQueryResult getTrace(ICoordinator coordinator, UUID session)
    {
        return coordinator.executeWithResult("SELECT * FROM system_traces.events WHERE session_id=?", ConsistencyLevel.LOCAL_QUORUM, session);
    }

    public static class WithTraceException extends RuntimeException
    {
        public final SimpleQueryResult trace;

        public WithTraceException(Throwable cause, SimpleQueryResult trace)
        {
            super(cause);
            this.trace = trace;
        }
    }
}
