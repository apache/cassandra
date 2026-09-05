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
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;
import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalKeyspace;
import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalTables;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * @deprecated See CASSANDRA-20793. Use {@code compact keyspace}, {@code compact sstables},
 * or {@code compact range} instead.
 */
@Deprecated(since = "7.0")
@Command(name = "compact",
         description = "Force a (major) compaction on one or more tables or user-defined compaction on given SSTables",
         subcommands = { Compact.Keyspace.class, Compact.SSTables.class, Compact.Range.class })
public class Compact extends AbstractCommand
{
    @CassandraUsage(usage = "[<keyspace> <tables>...] or <SSTable file>...",
                    description = "The keyspace followed by one or many tables or list of SSTable data files when using --user-defined")
    @Parameters(index = "0..*", description = "The keyspace followed by one or many tables or " +
                                              "list of SSTable data files when using --user-defined")
    private List<String> args = new ArrayList<>();

    @Option(paramLabel = "split_output", names = { "-s", "--split-output" }, description = "Use -s to not create a single big file")
    private boolean splitOutput = false;

    @Option(paramLabel = "user_defined", names = { "--user-defined" }, description = "Use --user-defined to submit listed files for user-defined compaction")
    private boolean userDefined = false;

    @Option(paramLabel = "start_token", names = { "-st", "--start-token" }, description = "Use -st to specify a token at which the compaction range starts (inclusive)")
    private String startToken = EMPTY;

    @Option(paramLabel = "end_token", names = { "-et", "--end-token" }, description = "Use -et to specify a token at which compaction range ends (inclusive)")
    private String endToken = EMPTY;

    @Option(paramLabel = "partition_key", names = { "--partition" }, description = "String representation of the partition key")
    private String partitionKey = EMPTY;

    @Option(paramLabel = "jobs",
            names = {"-j", "--jobs"},
            description = "Use -j to specify the maximum number of threads to use for parallel compaction. " +
                          "If not set, up to half the compaction threads will be used. " +
                          "If set to 0, the major compaction will use all threads and will not permit other compactions to run until it completes (use with caution).")
    private Integer parallelism = null;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.output().out.println("WARNING: nodetool compact is deprecated, use 'compact keyspace', 'compact sstables', or 'compact range' instead.");

        final boolean startEndTokenProvided = !(startToken.isEmpty() && endToken.isEmpty());
        final boolean partitionKeyProvided = !partitionKey.isEmpty();
        final boolean tokenProvided = startEndTokenProvided || partitionKeyProvided;
        if (splitOutput && (userDefined || tokenProvided))
        {
            throw new RuntimeException("Invalid option combination: Can not use split-output here");
        }
        if (userDefined && tokenProvided)
        {
            throw new RuntimeException("Invalid option combination: Can not provide tokens when using user-defined");
        }

        if (userDefined)
        {
            try
            {
                String userDefinedFiles = String.join(",", args);
                probe.forceUserDefinedCompaction(userDefinedFiles);
            } catch (Exception e) {
                throw new RuntimeException("Error occurred during user defined compaction", e);
            }
            return;
        }

        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] tableNames = parseOptionalTables(args);

        for (String keyspace : keyspaces)
        {
            try
            {
                if (startEndTokenProvided)
                {
                    probe.forceKeyspaceCompactionForTokenRange(keyspace, startToken, endToken, tableNames);
                }
                else if (partitionKeyProvided)
                {
                    probe.forceKeyspaceCompactionForPartitionKey(keyspace, partitionKey, tableNames);
                }
                else
                {
                    if (parallelism != null)
                        probe.forceKeyspaceCompaction(splitOutput, parallelism, keyspace, tableNames);
                    else // avoid referring to the new method to work with older versions
                        probe.forceKeyspaceCompaction(splitOutput, keyspace, tableNames);
                }
            } catch (Exception e)
            {
                throw new RuntimeException("Error occurred during compaction", e);
            }
        }
    }

    /**
     * Subcommand for compacting specific keyspace tables or a specific partition within a keyspace.
     */
    @Command(name = "keyspace", description = "Force a (major) compaction on one or more tables in a keyspace")
    public static class Keyspace extends AbstractCommand
    {
        @CassandraUsage(usage = "[<keyspace> <tables>...]",
                        description = "The keyspace followed by one or many tables")
        @Parameters(index = "0", description = "The keyspace name", arity = "0..1")
        private String keyspaceName;

        @Parameters(index = "1..*", description = "The table names", arity = "0..*")
        private List<String> tableNames = new ArrayList<>();

        @Option(paramLabel = "split_output", names = { "-s", "--split-output" }, description = "Use -s to not create a single big file")
        private boolean splitOutput = false;

        @Option(paramLabel = "partition_key", names = { "--partition" }, description = "String representation of the partition key")
        private String partitionKey = EMPTY;

        @Option(paramLabel = "jobs",
                names = {"-j", "--jobs"},
                description = "Use -j to specify the maximum number of threads to use for parallel compaction. " +
                              "If not set, up to half the compaction threads will be used. " +
                              "If set to 0, the major compaction will use all threads and will not permit other compactions to run until it completes (use with caution).")
        private Integer parallelism = null;

        @Override
        public void execute(NodeProbe probe)
        {
            if (splitOutput && !partitionKey.isEmpty())
                throw new RuntimeException("Invalid option combination: Can not use split-output with --partition");

            List<String> args = concatArgs(keyspaceName, tableNames);
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] tables = parseOptionalTables(args);

            for (String ks : keyspaces)
            {
                try
                {
                    if (!partitionKey.isEmpty())
                    {
                        probe.forceKeyspaceCompactionForPartitionKey(ks, partitionKey, tables);
                    }
                    else
                    {
                        if (parallelism != null)
                            probe.forceKeyspaceCompaction(splitOutput, parallelism, ks, tables);
                        else
                            probe.forceKeyspaceCompaction(splitOutput, ks, tables);
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during compaction", e);
                }
            }
        }
    }

    /**
     * Subcommand for user-defined compaction on specific SSTable files.
     */
    @Command(name = "sstables", description = "Force user-defined compaction on given SSTable files")
    public static class SSTables extends AbstractCommand
    {
        @CassandraUsage(usage = "<SSTable file>...",
                        description = "List of SSTable data files for user-defined compaction")
        @Parameters(index = "0..*", description = "List of SSTable data files for user-defined compaction", arity = "1..*")
        private List<String> sstableFiles = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            try
            {
                String userDefinedFiles = String.join(",", sstableFiles);
                probe.forceUserDefinedCompaction(userDefinedFiles);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error occurred during user defined compaction", e);
            }
        }
    }

    /**
     * Subcommand for token range compaction on one or more tables in a keyspace.
     * At least one of --start-token or --end-token must be provided.
     */
    @Command(name = "range", description = "Force compaction on a token range for one or more tables in a keyspace")
    public static class Range extends AbstractCommand
    {
        @CassandraUsage(usage = "[<keyspace> <tables>...]",
                        description = "The keyspace followed by one or many tables")
        @Parameters(index = "0", description = "The keyspace name", arity = "0..1")
        private String keyspaceName;

        @Parameters(index = "1..*", description = "The table names", arity = "0..*")
        private List<String> tableNames = new ArrayList<>();

        @ArgGroup(exclusive = false, multiplicity = "1..1")
        private TokenRange tokenRange;

        /**
         * Token range options for compaction. At least one of --start-token or --end-token must be provided.
         */
        static class TokenRange
        {
            @Option(paramLabel = "start_token",
                    names = { "-st", "--start-token" },
                    description = "Use -st to specify a token at which the compaction range starts (inclusive)")
            private String startToken = EMPTY;

            @Option(paramLabel = "end_token",
                    names = { "-et", "--end-token" },
                    description = "Use -et to specify a token at which compaction range ends (inclusive)")
            private String endToken = EMPTY;
        }

        @Override
        public void execute(NodeProbe probe)
        {
            List<String> args = concatArgs(keyspaceName, tableNames);
            List<String> keyspaces = parseOptionalKeyspace(args, probe);
            String[] tables = parseOptionalTables(args);

            String startToken = tokenRange != null ? tokenRange.startToken : EMPTY;
            String endToken = tokenRange != null ? tokenRange.endToken : EMPTY;

            for (String ks : keyspaces)
            {
                try
                {
                    probe.forceKeyspaceCompactionForTokenRange(ks, startToken, endToken, tables);
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Error occurred during compaction", e);
                }
            }
        }
    }
}
