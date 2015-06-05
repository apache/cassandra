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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.airlift.command.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

/**
* Created by beggleston on 6/2/15.
*/
@Command(name="epaxosstates", description = "prints the current state of epaxos token states", hidden = true)
public class PrintEpaxosStates extends NodeTool.NodeToolCmd
{
    @Override
    protected void execute(NodeProbe probe)
    {
        Map<String, List<Map<String, String>>> tables = probe.getPaxosStates();
        List<String> keys = new ArrayList<>(tables.keySet());
        Collections.sort(keys);

        int numTables = 0;
        int numTokens = 0;

        for (String key: keys)
        {
            numTables++;
            System.out.println(key);
            for (Map<String, String> attrs: tables.get(key))
            {
                numTokens++;
                System.out.println(String.format("range: [%s,%s), epoch: %s, executed: %s, scope: %s, state: %s",
                                                 attrs.get("tokenHi"), attrs.get("tokenLo"), attrs.get("epoch"),
                                                 attrs.get("executions"), attrs.get("scope"), attrs.get("state")));
            }
        }

        System.out.println(String.format("\n %s tables with %s token ranges", numTables, numTokens));
    }
}
