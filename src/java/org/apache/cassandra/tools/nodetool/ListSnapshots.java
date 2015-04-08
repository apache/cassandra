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

import io.airlift.command.Command;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.openmbean.TabularData;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "listsnapshots", description = "Lists all the snapshots along with the size on disk and true size.")
public class ListSnapshots extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            System.out.println("Snapshot Details: ");

            final Map<String,TabularData> snapshotDetails = probe.getSnapshotDetails();
            if (snapshotDetails.isEmpty())
            {
                System.out.printf("There are no snapshots");
                return;
            }

            final long trueSnapshotsSize = probe.trueSnapshotsSize();
            final String format = "%-20s%-29s%-29s%-19s%-19s%n";
            // display column names only once
            final List<String> indexNames = snapshotDetails.entrySet().iterator().next().getValue().getTabularType().getIndexNames();
            System.out.printf(format, (Object[]) indexNames.toArray(new String[indexNames.size()]));

            for (final Map.Entry<String, TabularData> snapshotDetail : snapshotDetails.entrySet())
            {
                Set<?> values = snapshotDetail.getValue().keySet();
                for (Object eachValue : values)
                {
                    final List<?> value = (List<?>) eachValue;
                    System.out.printf(format, value.toArray(new Object[value.size()]));
                }
            }

            System.out.println("\nTotal TrueDiskSpaceUsed: " + FileUtils.stringifyFileSize(trueSnapshotsSize) + "\n");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error during list snapshot", e);
        }
    }
}