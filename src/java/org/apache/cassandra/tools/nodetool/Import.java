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
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "import", description = "Import new SSTables to the system")
public class Import extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table> <directory>", description = "The keyspace, table name and directory to import sstables from")
    private List<String> args = new ArrayList<>();

    @Option(title = "keep_level",
            name = {"-l", "--keep-level"},
            description = "Keep the level on the new sstables")
    private boolean keepLevel = false;

    @Option(title = "keep_repaired",
            name = {"-r", "--keep-repaired"},
            description = "Keep any repaired information from the sstables")
    private boolean keepRepaired = false;

    @Option(title = "no_verify_sstables",
            name = {"-v", "--no-verify"},
            description = "Don't verify new sstables")
    private boolean noVerify = false;

    @Option(title = "no_verify_tokens",
            name = {"-t", "--no-tokens"},
            description = "Don't verify that all tokens in the new sstable are owned by the current node")
    private boolean noVerifyTokens = false;

    @Option(title = "no_invalidate_caches",
            name = {"-c", "--no-invalidate-caches"},
            description = "Don't invalidate the row cache when importing")
    private boolean noInvalidateCaches = false;

    @Option(title = "no_jbod_check",
            name = {"-j", "--no-jbod-check"},
            description = "Don't iterate over keys to check which data directory is best suited")
    private boolean noJBODCheck = false;

    @Option(title = "quick",
            name = {"-q", "--quick"},
            description = "Do a quick import without verifying sstables, clearing row cache or checking in which data directory to put the file")
    private boolean quick = false;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 3, "import requires keyspace, table name and directory");

        if (!noVerifyTokens && noVerify)
        {
            noVerifyTokens = true;
            System.out.println("Not verifying tokens since --no-verify or -v is set");
        }

        if (quick)
        {
            System.out.println("Doing a quick import - skipping sstable verification, row cache invalidation and JBOD checking");
            noVerifyTokens = true;
            noInvalidateCaches = true;
            noVerify = true;
            noJBODCheck = true;
        }
        probe.importNewSSTables(args.get(0), args.get(1), args.get(2), !keepLevel, !keepRepaired, !noVerify, !noVerifyTokens, !noInvalidateCaches, !noJBODCheck);
    }
}