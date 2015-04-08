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
import io.airlift.command.Arguments;
import io.airlift.command.Command;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getcompactionthreshold", description = "Print min and max compaction thresholds for a given table")
public class GetCompactionThreshold extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table>", description = "The keyspace with a table")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "getcompactionthreshold requires ks and cf args");
        String ks = args.get(0);
        String cf = args.get(1);

        ColumnFamilyStoreMBean cfsProxy = probe.getCfsProxy(ks, cf);
        System.out.println("Current compaction thresholds for " + ks + "/" + cf + ": \n" +
                " min = " + cfsProxy.getMinimumCompactionThreshold() + ", " +
                " max = " + cfsProxy.getMaximumCompactionThreshold());
    }
}