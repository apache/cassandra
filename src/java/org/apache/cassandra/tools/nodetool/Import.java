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
import java.util.HashSet;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;

@Command(name = "import", description = "Import new SSTables to the system")
public class Import extends AbstractCommand
{
    public static final String IMPORT_FAIL_MESSAGE = "Some directories failed to import, check server logs for details";

    @CassandraUsage(usage = "<keyspace> <table> <directory> ...", description = "The keyspace, table name and directories to import sstables from")
    private List<String> args = new ArrayList<>();

    @Parameters(index = "0", paramLabel = "keyspace", description = "The keyspace name")
    private String keyspace;

    @Parameters(index = "1", paramLabel = "table", description = "The table name")
    private String table;

    @Parameters(index = "2..*", paramLabel = "directories", description = "The directories to import sstables from")
    private String[] directories;

    @Option(paramLabel = "keep_level",
            names = { "-l", "--keep-level" },
            description = "Keep the level on the new sstables")
    private boolean keepLevel = false;

    @Option(paramLabel = "keep_repaired",
            names = { "-r", "--keep-repaired" },
            description = "Keep any repaired information from the sstables")
    private boolean keepRepaired = false;

    @Option(paramLabel = "no_verify_sstables",
            names = { "-v", "--no-verify" },
            description = "Don't verify new sstables")
    private boolean noVerify = false;

    @Option(paramLabel = "no_verify_tokens",
            names = { "-t", "--no-tokens" },
            description = "Don't verify that all tokens in the new sstable are owned by the current node")
    private boolean noVerifyTokens = false;

    @Option(paramLabel = "no_invalidate_caches",
            names = { "-c", "--no-invalidate-caches" },
            description = "Don't invalidate the row cache when importing")
    private boolean noInvalidateCaches = false;

    @Option(paramLabel = "quick",
            names = { "-q", "--quick" },
            description = "Do a quick import without verifying sstables, clearing row cache or checking in which data directory to put the file")
    private boolean quick = false;

    @Option(paramLabel = "extended_verify",
            names = { "-e", "--extended-verify" },
            description = "Run an extended verify, verifying all values in the new sstables")
    private boolean extendedVerify = false;

    // The previous option -p collides with the --port in the JMX, so we need to alter it to -cd.
    // It is safe to alter the name since the option is not used by users as it doesn't work.
    @Option(paramLabel = "copy_data",
            names = { "-cd", "--copy-data" },
            description = "Copy data from source directories instead of moving them")
    private boolean copyData = false;

    @Option(paramLabel = "require_index_components",
            names = { "-ri", "--require-index-components" },
            description = "Require existing index components for SSTables with attached indexes")
    private boolean failOnMissingIndex = false;

    @Option(paramLabel = "no_index_validation",
            names = { "-niv", "--no-index-validation" },
            description = "Skip SSTable-attached index checksum validation")
    private boolean noIndexValidation = false;

    @Override
    public void execute(NodeProbe probe)
    {
        args = concatArgs(keyspace, table, directories);
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
            for (String directory : failedDirs)
                output.printError(directory);
            throw new RuntimeException(IMPORT_FAIL_MESSAGE);
        }
    }
}
