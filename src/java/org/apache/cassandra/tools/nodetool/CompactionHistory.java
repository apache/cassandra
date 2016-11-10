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

import static com.google.common.collect.Iterables.toArray;
import io.airlift.command.Command;

import java.util.List;
import java.util.Set;

import javax.management.openmbean.TabularData;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "compactionhistory", description = "Print history of compaction")
public class CompactionHistory extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        System.out.println("Compaction History: ");

        TabularData tabularData = probe.getCompactionHistory();
        if (tabularData.isEmpty())
        {
            System.out.printf("There is no compaction history");
            return;
        }

        String format = "%-41s%-19s%-29s%-26s%-15s%-15s%s%n";
        List<String> indexNames = tabularData.getTabularType().getIndexNames();
        System.out.printf(format, toArray(indexNames, Object.class));

        Set<?> values = tabularData.keySet();
        for (Object eachValue : values)
        {
            List<?> value = (List<?>) eachValue;
            System.out.printf(format, toArray(value, Object.class));
        }
    }
}