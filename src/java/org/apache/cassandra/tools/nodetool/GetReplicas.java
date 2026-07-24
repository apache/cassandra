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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;

@Command(name = "getreplicas", description = "Print the replicas that own the key")
public class GetReplicas extends AbstractCommand
{
    @CassandraUsage(usage = "<keyspace> <table> <key>", description = "The keyspace, the table, and the partition key for which we need to find the replica")
    private List<String> args = new ArrayList<>();

    @Parameters(index = "0", arity = "0..1", description = "The keyspace for which we need to find the replica")
    private String keyspace;

    @Parameters(index = "1", arity = "0..1", description = "The table for which we need to find the replica")
    private String table;

    @Parameters(index = "2", arity = "0..1", description = "The partition key for which we need to find the replica (e.g., pk1:pk2:pk3 for compound keys)")
    private String key;

    @Mixin
    private PrintPortMixin printPortMixin = new PrintPortMixin();

    @Override
    public void execute(NodeProbe probe)
    {
        args = concatArgs(keyspace, table, key);

        checkArgument(args.size() == 3, "requires keyspace, table and partition key arguments");
        String ks = args.get(0);
        String table = args.get(1);
        String key = args.get(2);

        if (printPortMixin.printPort)
        {
            for (String replica : probe.getReplicasWithPort(ks, table, key))
            {
                probe.output().out.println(replica);
            }
        }
        else
        {
            List<InetAddress> replicas = probe.getReplicas(ks, table, key);
            for (InetAddress replica : replicas)
            {
                probe.output().out.println(replica.getHostAddress());
            }
        }
    }
}
