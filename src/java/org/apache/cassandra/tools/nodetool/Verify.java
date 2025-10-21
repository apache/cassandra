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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalKeyspace;
import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalTables;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;

@Command(name = "verify", description = "Verify (check data checksum for) one or more tables")
public class Verify extends AbstractCommand
{
    @CassandraUsage(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Parameters(index = "0", description = "The keyspace followed by one or many tables", arity = "0..1")
    private String keyspace;

    @Parameters(index = "1..*", description = "The tables to upgrade", arity = "0..*")
    private List<String> tables;

    @Option(paramLabel = "extended_verify",
            names = { "-e", "--extended-verify" },
            description = "Verify each cell data, beyond simply checking sstable checksums")
    private boolean extendedVerify = false;

    @Option(paramLabel = "check_version",
            names = { "-c", "--check-version" },
            description = "Also check that all sstables are the latest version")
    private boolean checkVersion = false;

    @Option(paramLabel = "override-disable",
            names = { "-f", "--force" },
            description = "Override disabling of verify tool - see CASSANDRA-9947 for caveats")
    private boolean overrideDisable = false;

    @Option(paramLabel = "dfp",
            names = { "-d", "--dfp" },
            description = "Invoke the disk failure policy if a corrupt sstable is found")
    private boolean diskFailurePolicy = false;

    @Option(paramLabel = "repair_status_change",
            names = { "-r", "--rsc" },
            description = "Mutate the repair status on corrupt sstables")
    private boolean mutateRepairStatus = false;

    @Option(paramLabel = "check_owns_tokens",
            names = { "-t", "--check-tokens" },
            description = "Verify that all tokens in sstables are owned by this node")
    private boolean checkOwnsTokens = false;

    @Option(paramLabel = "quick",
            names = { "-q", "--quick" },
            description = "Do a quick check - avoid reading all data to verify checksums")
    private boolean quick = false;

    @Override
    public void execute(NodeProbe probe)
    {
        args = concatArgs(keyspace, tables);
        PrintStream out = probe.output().out;
        if (!overrideDisable)
        {
            out.println("verify is disabled unless a [-f|--force] override flag is provided. See CASSANDRA-9947 and CASSANDRA-17017 for details.");
            System.exit(1);
        }

        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] tableNames = parseOptionalTables(args);

        if (checkOwnsTokens && !extendedVerify)
        {
            out.println("Token verification requires --extended-verify");
            // if System.exit gets removed, make sure to update org.apache.cassandra.distributed.test.NodeToolTest.testNodetoolSystemExit
            System.exit(1);
        }

        for (String keyspace : keyspaces)
        {
            try
            {
                probe.verify(out, extendedVerify, checkVersion, diskFailurePolicy, mutateRepairStatus, checkOwnsTokens, quick, keyspace, tableNames);
            } catch (Exception e)
            {
                throw new RuntimeException("Error occurred during verifying", e);
            }
        }
    }
}
