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
import java.util.Map;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "statusautocompaction", description = "status of autocompaction of the given keyspace and table")
public class StatusAutoCompaction extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Option(title = "show_all", name = { "-a", "--all" }, description = "Show auto compaction status for each keyspace/table")
    private boolean showAll = false;

    @Override
    public void execute(NodeProbe probe)
    {
        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] tableNames = parseOptionalTables(args);

        boolean allDisabled = true;
        boolean allEnabled = true;
        TableBuilder table = new TableBuilder();
        table.add("Keyspace", "Table", "Status");
        try
        {
            for (String keyspace : keyspaces)
            {
                Map<String, Boolean> statuses = probe.getAutoCompactionDisabled(keyspace, tableNames);
                for (Map.Entry<String, Boolean> status : statuses.entrySet())
                {
                    String tableName = status.getKey();
                    boolean disabled = status.getValue();
                    allDisabled &= disabled;
                    allEnabled &= !disabled;
                    table.add(keyspace, tableName, !disabled ? "running" : "not running");
                }
            }
            if (showAll)
                table.printTo(probe.output().out);
            else
                probe.output().out.println(allEnabled ? "running" :
                                   allDisabled ? "not running" : "partially running");
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error occurred during status-auto-compaction", e);
        }
    }
}
