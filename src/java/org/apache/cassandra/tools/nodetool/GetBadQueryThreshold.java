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

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getbadquerythreshold", description = "Print badquery threshold")
public class GetBadQueryThreshold extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        if (probe.isBadQueryTracingEnabled())
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\ntracing fraction: " + probe.getBadQueryTracingFraction());
            sb.append("\nmax samples count per badquery report in syslog: " + probe.getBadQueryMaxSamplesInSyslog());
            sb.append("\nmax read partition size in bytes: " + probe.getBadQueryReadMaxPartitionSizeInbytes());
            sb.append("\nmax write partition size in bytes: " + probe.getBadQueryWriteMaxPartitionSizeInbytes());
            sb.append("\nread slow local latency in ms: " + probe.getBadQueryReadSlowLocalLatencyInms());
            sb.append("\nwrite slow local latency in ms: " + probe.getBadQueryWriteSlowLocalLatencyInms());
            sb.append("\nread slow coordinator latency in ms: " + probe.getBadQueryReadSlowCoordLatencyInms());
            sb.append("\nwrite slow coordinator latency in ms: " + probe.getBadQueryWriteSlowCoordLatencyInms());
            sb.append("\ntombstone limit: " + probe.getBadQueryTombstoneLimit());
            sb.append("\nignore keyspace list: " + probe.getBadQueryIgnoreKeyspaces());
            System.out.println("Current badquery threshold: " + sb.toString());
        }
        else
        {
            System.out.println("badquery tracing is disabled");
        }
    }
}
