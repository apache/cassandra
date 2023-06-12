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

package org.apache.cassandra.distributed.impl;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Utilities for accessing the system_traces table from in-JVM dtests
 */
public class TracingUtil
{
    /**
     * Represents an entry from system_traces
     */
    public static class TraceEntry
    {
        public final UUID sessionId;
        public final UUID eventId;
        public final String activity;
        public final InetAddress source;
        public final int sourceElapsed;
        public final String thread;

        private TraceEntry(UUID sessionId, UUID eventId, String activity, InetAddress sourceIP, int sourceElapsed, String thread)
        {
            this.sessionId = sessionId;
            this.eventId = eventId;
            this.activity = activity;
            this.source = sourceIP;
            this.sourceElapsed = sourceElapsed;
            this.thread = thread;
        }

        static TraceEntry fromRowResultObjects(Object[] objects)
        {
            UUID eventId = objects[1] instanceof UUID ? (UUID)objects[1]: ((TimeUUID)objects[1]).asUUID();
            return new TraceEntry((UUID)objects[0],
                                  eventId,
                                  (String) objects[2],
                                  (InetAddress) objects[3],
                                  (Integer) objects[4],
                                  (String) objects[5]);
        }
    }

    public static List<TraceEntry> getTrace(AbstractCluster cluster, UUID sessionId)
    {
        return getTrace(cluster, sessionId, ConsistencyLevel.ALL);
    }

    public static List<TraceEntry> getTrace(AbstractCluster cluster, UUID sessionId, ConsistencyLevel cl)
    {
        Object[][] result = cluster.coordinator(1).execute(
        "SELECT session_id, event_id, activity, source, source_elapsed, thread " +
        "FROM system_traces.events WHERE session_id = ?", cl, sessionId);

        List<TraceEntry> traces = new LinkedList<>();
        for (Object[] r : result)
        {
            traces.add(TraceEntry.fromRowResultObjects(r));
        }
        return traces;
    }

    public static List<TraceEntry> getTraces(AbstractCluster cluster)
    {
        return getTraces(cluster, ConsistencyLevel.ALL);
    }

    public static List<TraceEntry> getTraces(AbstractCluster cluster, ConsistencyLevel cl)
    {
        Object[][] result = cluster.coordinator(1).execute(
        "SELECT session_id, event_id, activity, source, source_elapsed, thread " +
        "FROM system_traces.events", cl);

        List<TraceEntry> traces = new ArrayList<>();
        for (Object[] r : result)
        {
            traces.add(TraceEntry.fromRowResultObjects(r));
        }
        return traces;
    }
}
