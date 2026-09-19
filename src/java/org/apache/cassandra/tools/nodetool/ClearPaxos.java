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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "clearpaxos", description = "Clean up paxos state for one or more tables without running repair")
public class ClearPaxos extends AbstractCommand
{
    @CassandraUsage(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by optional tables to clean up paxos state")
    @Parameters(description = "The keyspace followed by optional tables to clean up paxos state", arity = "0..*")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        String keyspace = args.isEmpty() ? null : args.get(0);
        String[] tables = args.size() <= 1 ? new String[0] : args.subList(1, args.size()).toArray(new String[0]);

        try
        {
            probe.output().out.println("Starting paxos cleanup...");
            probe.paxosCleanup(keyspace, tables);
            probe.output().out.println("Paxos cleanup completed.");
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error during paxos cleanup", e);
        }
    }
}
