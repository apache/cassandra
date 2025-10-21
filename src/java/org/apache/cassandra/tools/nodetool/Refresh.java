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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;

/** @deprecated See CASSANDRA-6719 */
@Deprecated(since = "4.0")
@Command(name = "refresh", description = "Load newly placed SSTables to the system without restart")
public class Refresh extends AbstractCommand
{
    @CassandraUsage(usage = "<keyspace> <table>", description = "The keyspace and table name")
    private List<String> args = new ArrayList<>();

    @Parameters(index = "0", description = "The keyspace name", arity = "0..1")
    private String keyspace;

    @Parameters(index = "1", description = "The table name", arity = "0..1")
    private String table;

    @Override
    public void execute(NodeProbe probe)
    {
        args = concatArgs(keyspace, table);

        probe.output().out.println("nodetool refresh is deprecated, use nodetool import instead");
        checkArgument(args.size() == 2, "refresh requires ks and cf args");
        probe.loadNewSSTables(args.get(0), args.get(1));
    }
}
