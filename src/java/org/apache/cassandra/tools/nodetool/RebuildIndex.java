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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.toArray;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "rebuild_index", description = "A full rebuild of native secondary indexes for a given table")
public class RebuildIndex extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table> <indexName...>", description = "The keyspace and table name followed by a list of index names")
    List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() >= 3, "rebuild_index requires ks, cf and idx args");
        probe.rebuildIndex(args.get(0), args.get(1), toArray(args.subList(2, args.size()), String.class));
    }
}