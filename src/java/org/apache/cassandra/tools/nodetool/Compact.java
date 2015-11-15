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

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "compact", description = "Force a (major) compaction on one or more tables or user-defined compaction on given SSTables")
public class Compact extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace> <tables>...] or <SSTable file>...", description = "The keyspace followed by one or many tables or list of SSTable data files when using --user-defined")
    private List<String> args = new ArrayList<>();

    @Option(title = "split_output", name = {"-s", "--split-output"}, description = "Use -s to not create a single big file")
    private boolean splitOutput = false;

    @Option(title = "user-defined", name = {"--user-defined"}, description = "Use --user-defined to submit listed files for user-defined compaction")
    private boolean userDefined = false;

    @Override
    public void execute(NodeProbe probe)
    {
        if (splitOutput && userDefined)
        {
            throw new RuntimeException("Invalid option combination: User defined compaction can not be split");
        }
        else if (userDefined)
        {
            try
            {
                String userDefinedFiles = String.join(",", args);
                probe.forceUserDefinedCompaction(userDefinedFiles);
            } catch (Exception e) {
                throw new RuntimeException("Error occurred during user defined compaction", e);
            }
            return;
        }

        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] tableNames = parseOptionalTables(args);

        for (String keyspace : keyspaces)
        {
            try
            {
                probe.forceKeyspaceCompaction(splitOutput, keyspace, tableNames);
            } catch (Exception e)
            {
                throw new RuntimeException("Error occurred during compaction", e);
            }
        }
    }
}