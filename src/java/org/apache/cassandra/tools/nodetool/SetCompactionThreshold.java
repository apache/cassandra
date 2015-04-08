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
import static java.lang.Integer.parseInt;
import io.airlift.command.Arguments;
import io.airlift.command.Command;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "setcompactionthreshold", description = "Set min and max compaction thresholds for a given table")
public class SetCompactionThreshold extends NodeToolCmd
{
    @Arguments(title = "<keyspace> <table> <minthreshold> <maxthreshold>", usage = "<keyspace> <table> <minthreshold> <maxthreshold>", description = "The keyspace, the table, min and max threshold", required = true)
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 4, "setcompactionthreshold requires ks, cf, min, and max threshold args.");

        int minthreshold = parseInt(args.get(2));
        int maxthreshold = parseInt(args.get(3));
        checkArgument(minthreshold >= 0 && maxthreshold >= 0, "Thresholds must be positive integers");
        checkArgument(minthreshold <= maxthreshold, "Min threshold cannot be greater than max.");
        checkArgument(minthreshold >= 2 || maxthreshold == 0, "Min threshold must be at least 2");

        probe.setCompactionThreshold(args.get(0), args.get(1), minthreshold, maxthreshold);
    }
}