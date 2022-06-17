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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "forcecompact", description = "Force a (major) compaction on a table")
public class ForceCompact extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace> <table> <keys>]", description = "The keyspace, table, and a list of partition keys ignoring the gc_grace_seconds")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        // Check if the input has valid size
        checkArgument(args.size() >= 3, "forcecompact requires keyspace, table and keys args");

        // We rely on lower-level APIs to check and throw exceptions if the input keyspace or table name are invalid
        String keyspaceName = args.get(0);
        String tableName = args.get(1);
        String[] partitionKeysIgnoreGcGrace = parsePartitionKeys(args);

        try
        {
            probe.forceCompactionKeysIgnoringGcGrace(keyspaceName, tableName, partitionKeysIgnoreGcGrace);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error occurred during compaction keys", e);
        }
    }
}