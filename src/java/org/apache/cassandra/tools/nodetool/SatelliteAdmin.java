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

import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Nodetool command for managing satellite datacenter failover.
 */
@CommandLine.Command(name = "satellite_admin",
                     description = "Manage satellite datacenter failover",
                     subcommands = { SatelliteAdmin.Status.class,
                                     SatelliteAdmin.Advance.class })
public class SatelliteAdmin extends AbstractCommand
{
    @Override
    protected void execute(NodeProbe probe)
    {
        probe.output().out.println("Usage: nodetool satellite_admin <status|advance> <keyspace> [options]");
        probe.output().out.println("Run 'nodetool help satellite_admin' for details.");
    }

    @Command(name = "status", description = "Show satellite failover status for a keyspace")
    public static class Status extends AbstractCommand
    {
        @Parameters(index = "0", description = "The keyspace name")
        private String keyspace;

        @Override
        protected void execute(NodeProbe probe)
        {
            String status = probe.getStorageService().getSatelliteFailoverStatus(keyspace);
            probe.output().out.println(status);
        }
    }

    @Command(name = "advance", description = "Advance satellite failover for a keyspace")
    public static class Advance extends AbstractCommand
    {
        @Parameters(index = "0", description = "The keyspace name")
        private String keyspace;

        @Option(names = { "-r", "--ranges" },
                description = "Token ranges to process (format: start:end,start:end). Defaults to local ranges if not specified.")
        private String rangesStr = null;

        @Option(names = { "--ack" },
                description = "Only advance through epoch ack and paxos repair (TRANSITION_ACK -> TRANSITION)")
        private boolean ackOnly;

        @Option(names = { "--barrier" },
                description = "Only process MT barrier for TRANSITION ranges (TRANSITION -> NORMAL)")
        private boolean barrierOnly;

        @Option(names = { "--force" },
                description = "Skip gate evaluation and commit state transitions directly")
        private boolean force;

        @Override
        protected void execute(NodeProbe probe)
        {
            String result = probe.getStorageService().advanceSatelliteFailover(
                keyspace, rangesStr, ackOnly, barrierOnly, force);
            probe.output().out.println(result);
        }
    }
}
