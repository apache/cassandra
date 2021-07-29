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
package org.apache.cassandra.tools.nodetool;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "setlogginglevel", description = "Set the log level threshold for a given component or class. Will reset to the initial configuration if called with no parameters.")
public class SetLoggingLevel extends NodeToolCmd
{
    @Arguments(usage = "<component|class> <level>", description = "The component or class to change the level for and the log level threshold to set. Will reset to initial level if omitted. "
        + "Available components:  bootstrap, compaction, repair, streaming, cql, ring")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        String target = args.size() >= 1 ? args.get(0) : EMPTY;
        String level = args.size() == 2 ? args.get(1) : EMPTY;

        List<String> classQualifiers = Collections.singletonList(target);
        if (target.equals("bootstrap"))
        {
            classQualifiers = Lists.newArrayList(
                    "org.apache.cassandra.gms",
                    "org.apache.cassandra.hints",
                    "org.apache.cassandra.schema",
                    "org.apache.cassandra.service.StorageService",
                    "org.apache.cassandra.db.SystemKeyspace",
                    "org.apache.cassandra.batchlog.BatchlogManager",
                    "org.apache.cassandra.net.MessagingService");
        }
        else if (target.equals("repair"))
        {
            classQualifiers = Lists.newArrayList(
                    "org.apache.cassandra.repair",
                    "org.apache.cassandra.db.compaction.CompactionManager",
                    "org.apache.cassandra.service.SnapshotVerbHandler");
        }
        else if (target.equals("streaming"))
        {
            classQualifiers = Lists.newArrayList(
                    "org.apache.cassandra.streaming",
                    "org.apache.cassandra.dht.RangeStreamer");
        }
        else if (target.equals("compaction"))
        {
            classQualifiers = Lists.newArrayList(
                    "org.apache.cassandra.db.compaction",
                    "org.apache.cassandra.db.ColumnFamilyStore",
                    "org.apache.cassandra.io.sstable.IndexSummaryRedistribution");
        }
        else if (target.equals("cql"))
        {
            classQualifiers = Lists.newArrayList(
                    "org.apache.cassandra.cql3",
                    "org.apache.cassandra.auth",
                    "org.apache.cassandra.batchlog",
                    "org.apache.cassandra.net.ResponseVerbHandler",
                    "org.apache.cassandra.service.AbstractReadExecutor",
                    "org.apache.cassandra.service.AbstractWriteResponseHandler",
                    "org.apache.cassandra.service.paxos",
                    "org.apache.cassandra.service.ReadCallback",
                    "org.apache.cassandra.service.ResponseResolver");
        }
        else if (target.equals("ring"))
        {
            classQualifiers = Lists.newArrayList(
                    "org.apache.cassandra.gms",
                    "org.apache.cassandra.service.PendingRangeCalculatorService",
                    "org.apache.cassandra.service.LoadBroadcaster",
                    "org.apache.cassandra.transport.Server");
        }

        for (String classQualifier : classQualifiers)
            probe.setLoggingLevel(classQualifier, level);
    }
}
