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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "clearsnapshotfiles", description = "Remove the snapshot files with the given name from the given keyspace and given table dir")
public class ClearSnapshotFiles extends NodeTool.NodeToolCmd
{
    @Arguments(usage = "<snapshot_name> <keyspace> <table_dir_name> [file_names] ...", description = "The keyspace, table_dir name and file_names to be deleted")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() >= 3, "clearsnapshotfiles requires snapshotname, keyspace name and table dir name");
        String snapshotName = args.get(0);
        String keyspaceName = args.get(1);
        String tableDirName = args.get(2);
        String[] filesToDelete = args.stream().skip(3).toArray(String[]::new);

        try
        {
            probe.clearSnapshotFiles(snapshotName, keyspaceName, tableDirName, filesToDelete);

        }
        catch (IOException e)
        {
            throw new RuntimeException("Error during clearing snapshots", e);
        }
    }
}
