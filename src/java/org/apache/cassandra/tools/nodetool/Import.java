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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "import", description = "Import new SSTables to the system")
public class Import extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table> <directory> ...", description = "The keyspace, table name and directories to import sstables from")
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

    @Option(title = "quick",
            name = {"-q", "--quick"},
            description = "Do a quick import without verifying sstables, clearing row cache or checking in which data directory to put the file")
    private boolean quick = false;

    @Option(title = "extended_verify",
            name = {"-e", "--extended-verify"},
            description = "Run an extended verify, verifying all values in the new sstables")
    private boolean extendedVerify = false;

    @Option(title = "copy_data",
            name = {"-p", "--copy-data"},
            description = "Copy data from source directories instead of moving them")
    private boolean copyData = false;

    @Option(title = "require_index_components",
            name = {"-ri", "--require-index-components"},
            description = "Require existing index components for SSTables with attached indexes")
    private boolean failOnMissingIndex = false;

    @Option(title = "no_index_validation",
            name = {"-niv", "--no-index-validation"},
            description = "Skip SSTable-attached index checksum validation")
    private boolean noIndexValidation = false;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() >= 3, "import requires keyspace, table name and directories");

        if (quick)
        {
            probe.output().out.println("Doing a quick import - skipping sstable verification and row cache invalidation");
            noVerifyTokens = true;
            noInvalidateCaches = true;
            noVerify = true;
            extendedVerify = false;
            noIndexValidation = true;
        }
        List<String> srcPaths = Lists.newArrayList(args.subList(2, args.size()));
        List<String> failedDirs = probe.importNewSSTables(args.get(0), args.get(1), new HashSet<>(srcPaths), !keepLevel,
                                                          !keepRepaired, !noVerify, !noVerifyTokens, !noInvalidateCaches,
                                                          extendedVerify, copyData, failOnMissingIndex, !noIndexValidation);
        if (!failedDirs.isEmpty())
        {
            PrintStream err = probe.output().err;
            err.println("Some directories failed to import, check server logs for details:");
            for (String directory : failedDirs)
                err.println(directory);
            System.exit(1);
        }
    }
}
