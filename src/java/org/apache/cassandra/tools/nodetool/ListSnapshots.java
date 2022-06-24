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

import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.openmbean.TabularData;

import io.airlift.airline.Command;

import io.airlift.airline.Option;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "listsnapshots", description = "Lists all the snapshots along with the size on disk and true size. True size is the total size of all SSTables which are not backed up to disk. Size on disk is total size of the snapshot on disk. Total TrueDiskSpaceUsed does not make any SSTable deduplication.")
public class ListSnapshots extends NodeToolCmd
{
    @Option(title = "no_ttl",
    name = { "-nt", "--no-ttl" },
    description = "Skip snapshots with TTL")
    private boolean noTTL = false;

    @Option(title = "ephemeral",
    name = { "-e", "--ephemeral" },
    description = "Include ephememeral snapshots")
    private boolean includeEphemeral = false;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        try
        {
            out.println("Snapshot Details: ");

            Map<String, String> options = new HashMap<>();
            options.put("no_ttl", Boolean.toString(noTTL));
            options.put("include_ephemeral", Boolean.toString(includeEphemeral));

            final Map<String, TabularData> snapshotDetails = probe.getSnapshotDetails(options);
            if (snapshotDetails.isEmpty())
            {
                out.println("There are no snapshots");
                return;
            }

            final long trueSnapshotsSize = probe.trueSnapshotsSize();
            TableBuilder table = new TableBuilder();
            // display column names only once
            final List<String> indexNames = snapshotDetails.entrySet().iterator().next().getValue().getTabularType().getIndexNames();

            if (includeEphemeral)
                table.add(indexNames.toArray(new String[indexNames.size()]));
            else
                table.add(indexNames.subList(0, indexNames.size() - 1).toArray(new String[indexNames.size() - 1]));

            for (final Map.Entry<String, TabularData> snapshotDetail : snapshotDetails.entrySet())
            {
                Set<?> values = snapshotDetail.getValue().keySet();
                for (Object eachValue : values)
                {
                    final List<?> value = (List<?>) eachValue;
                    if (includeEphemeral)
                        table.add(value.toArray(new String[value.size()]));
                    else
                        table.add(value.subList(0, value.size() - 1).toArray(new String[value.size() - 1]));
                }
            }
            table.printTo(out);

            out.println("\nTotal TrueDiskSpaceUsed: " + FileUtils.stringifyFileSize(trueSnapshotsSize) + '\n');
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error during list snapshot", e);
        }
    }
}
