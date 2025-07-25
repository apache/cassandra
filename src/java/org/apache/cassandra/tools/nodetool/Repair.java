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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;
import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalKeyspaceNonLocal;
import static org.apache.cassandra.tools.nodetool.CommandUtils.parseOptionalTables;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@Command(name = "repair", description = "Repair one or more tables")
public class Repair extends AbstractCommand
{
    public final static Set<String> ONLY_EXPLICITLY_REPAIRED = Sets.newHashSet(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);

    @CassandraUsage(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Parameters(index = "0", description = "The keyspace followed by one or many tables", arity = "0..1")
    private String keyspace;

    @Parameters(index = "1..*", description = "Tables to repair", arity = "0..*")
    public String[] tables;

    @Option(paramLabel = "seqential", names = { "-seq", "--sequential" }, description = "Use -seq to carry out a sequential repair")
    private boolean sequential = false;

    @Option(paramLabel = "dc_parallel", names = { "-dcpar", "--dc-parallel" }, description = "Use -dcpar to repair data centers in parallel.")
    private boolean dcParallel = false;

    @Option(paramLabel = "local_dc", names = { "-local", "--in-local-dc" }, description = "Use -local to only repair against nodes in the same datacenter")
    private boolean localDC = false;

    @Option(paramLabel = "specific_dc", names = { "-dc", "--in-dc" }, description = "Use -dc to repair specific datacenters")
    private List<String> specificDataCenters = new ArrayList<>();

    @Option(paramLabel = "specific_host", names = { "-hosts", "--in-hosts" }, description = "Use -hosts to repair specific hosts")
    private List<String> specificHosts = new ArrayList<>();

    @Option(paramLabel = "start_token", names = { "-st", "--start-token" }, description = "Use -st to specify a token at which the repair range starts (exclusive)")
    private String startToken = EMPTY;

    @Option(paramLabel = "end_token", names = { "-et", "--end-token" }, description = "Use -et to specify a token at which repair range ends (inclusive)")
    private String endToken = EMPTY;

    @Option(paramLabel = "primary_range", names = { "-pr", "--partitioner-range" }, description = "Use -pr to repair only the first range returned by the partitioner")
    private boolean primaryRange = false;

    @Option(paramLabel = "full", names = { "-full", "--full" }, description = "Use -full to issue a full repair.")
    private boolean fullRepair = false;

    @Option(paramLabel = "force", names = { "-force", "--force" }, description = "Use -force to filter out down endpoints")
    private boolean force = false;

    @Option(paramLabel = "preview", names = { "-prv", "--preview" }, description = "Determine ranges and amount of data to be streamed, but don't actually perform repair")
    private boolean preview = false;

    @Option(paramLabel = "validate", names = { "-vd", "--validate" }, description = "Checks that repaired data is in sync between nodes. Out of sync repaired data indicates a full repair should be run.")
    private boolean validate = false;

    @Option(paramLabel = "job_threads", names = { "-j", "--job-threads" }, description = "Number of threads to run repair jobs. " +
                                                                                         "Usually this means number of CFs to repair concurrently. " +
                                                                                         "WARNING: increasing this puts more load on repairing nodes, so be careful. (default: 1, max: 4)")
    private int numJobThreads = 1;

    @Option(paramLabel = "trace_repair", names = { "-tr", "--trace" }, description = "Use -tr to trace the repair. Traces are logged to system_traces.events.")
    private boolean trace = false;

    @Option(paramLabel = "pull_repair", names = { "-pl", "--pull" }, description = "Use --pull to perform a one way repair where data is only streamed from a remote node to this node.")
    private boolean pullRepair = false;

    @Option(paramLabel = "optimise_streams", names = { "-os", "--optimise-streams" }, description = "Use --optimise-streams to try to reduce the number of streams we do (EXPERIMENTAL, see CASSANDRA-3200).")
    private boolean optimiseStreams = false;

    @Option(paramLabel = "skip-paxos", names = { "-skip-paxos", "--skip-paxos" }, description = "If the --skip-paxos flag is included, the paxos repair step is skipped. Paxos repair is also skipped for preview repairs.")
    private boolean skipPaxos = false;

    @Option(paramLabel = "paxos-only", names = { "-paxos-only", "--paxos-only" }, description = "If the --paxos-only flag is included, no table data is repaired, only paxos operations..")
    private boolean paxosOnly = false;

    @Option(paramLabel = "accord-only", names = { "-accord-only", "--accord-only" }, description = "If the --accord-only flag is included, no table data is repaired, only accord operations..")
    private boolean accordOnly = false;

    @Option(paramLabel = "skip-accord", names = { "-skip-accord", "--skip-accord" }, description = "If the --skip-accord flag is included, the Accord repair step is skipped. Accord repair is also skipped for preview repairs.")
    private boolean skipAccord = false;


    @Option(paramLabel = "ignore_unreplicated_keyspaces", names = { "-iuk", "--ignore-unreplicated-keyspaces" }, description = "Use --ignore-unreplicated-keyspaces to ignore keyspaces which are not replicated, otherwise the repair will fail")
    private boolean ignoreUnreplicatedKeyspaces = false;

    @Option(paramLabel = "no_purge", names = { "--include-gcgs-expired-tombstones" }, description = "Do not apply gc grace seconds to purge any tombstones. Only useful in rare recovery scenarios, never regular operations.")
    private boolean dontPurgeTombstones = false;

    private PreviewKind getPreviewKind()
    {
        if (validate)
        {
            return PreviewKind.REPAIRED;
        }
        else if (preview && fullRepair)
        {
            return PreviewKind.ALL;
        }
        else if (preview)
        {
            return PreviewKind.UNREPAIRED;
        }
        else
        {
            return PreviewKind.NONE;
        }
    }

    @Override
    public void execute(NodeProbe probe)
    {
        args = concatArgs(keyspace, tables);

        List<String> keyspaces = parseOptionalKeyspaceNonLocal(args, probe);
        String[] cfnames = parseOptionalTables(args);

        if (primaryRange && (!specificDataCenters.isEmpty() || !specificHosts.isEmpty()))
            throw new RuntimeException("Primary range repair should be performed on all nodes in the cluster.");

        for (String keyspace : keyspaces)
        {
            // avoid repairing system_distributed by default (CASSANDRA-9621)
            if ((args == null || args.isEmpty()) && ONLY_EXPLICITLY_REPAIRED.contains(keyspace))
                continue;

            Map<String, String> options = createOptions(probe::getDataCenter, cfnames);
            try
            {
                probe.repairAsync(probe.output().out, keyspace, options);
            } catch (Exception e)
            {
                throw new RuntimeException("Error occurred during repair", e);
            }
        }
    }

    public Map<String, String> createOptions(Supplier<String> localDCOption, String[] cfnames)
    {
        Map<String, String> options = new HashMap<>();
        RepairParallelism parallelismDegree = RepairParallelism.PARALLEL;
        if (sequential)
            parallelismDegree = RepairParallelism.SEQUENTIAL;
        else if (dcParallel)
            parallelismDegree = RepairParallelism.DATACENTER_AWARE;
        options.put(RepairOption.PARALLELISM_KEY, parallelismDegree.getName());
        options.put(RepairOption.PRIMARY_RANGE_KEY, Boolean.toString(primaryRange));
        options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(!fullRepair && !(paxosOnly && getPreviewKind() == PreviewKind.NONE)));
        options.put(RepairOption.JOB_THREADS_KEY, Integer.toString(numJobThreads));
        options.put(RepairOption.TRACE_KEY, Boolean.toString(trace));
        options.put(RepairOption.COLUMNFAMILIES_KEY, StringUtils.join(cfnames, ","));
        options.put(RepairOption.PULL_REPAIR_KEY, Boolean.toString(pullRepair));
        options.put(RepairOption.FORCE_REPAIR_KEY, Boolean.toString(force));
        options.put(RepairOption.PREVIEW, getPreviewKind().toString());
        options.put(RepairOption.OPTIMISE_STREAMS_KEY, Boolean.toString(optimiseStreams));
        options.put(RepairOption.IGNORE_UNREPLICATED_KS, Boolean.toString(ignoreUnreplicatedKeyspaces));
        options.put(RepairOption.NO_TOMBSTONE_PURGING, Boolean.toString(dontPurgeTombstones));
        checkArgument(!(paxosOnly && accordOnly), "Can't specify both paxos-only and accord-only");
        checkArgument(!(skipPaxos && paxosOnly), "Can't specify both skip-paxos and paxos-only");
        boolean repairPaxos = !skipPaxos && !accordOnly && getPreviewKind() == PreviewKind.NONE;
        options.put(RepairOption.REPAIR_PAXOS_KEY, Boolean.toString(repairPaxos));
        checkArgument(!(skipAccord && accordOnly), "Can't specify both skip-accord and accord-only");
        boolean repairAccord = !skipAccord && !paxosOnly && getPreviewKind() == PreviewKind.NONE;
        options.put(RepairOption.REPAIR_ACCORD_KEY, Boolean.toString(repairAccord));
        boolean repairData = false;
        if (getPreviewKind() == PreviewKind.NONE)
        {
            // Paxos only historically doesn't do a repair, but Accord sticks to repairing at ALL
            // unless --force is specified.
            // If repair is incremental we need to do the repair to get the sstables created in the repaired set
            if (accordOnly)
                repairData = !fullRepair;
            // Default if not Paxos/Accord only is to repair data
            else if (!paxosOnly)
                repairData = true;
        }
        else
        {
            // Preview also "repairs" data
            repairData = true;
        }
        // Incremental repair always needs a data repair to actually do the incremental repair and move the sstables
        options.put(RepairOption.REPAIR_DATA_KEY, Boolean.toString(repairData));

        if (!startToken.isEmpty() || !endToken.isEmpty())
        {
            options.put(RepairOption.RANGES_KEY, startToken + ":" + endToken);
        }
        if (localDC)
        {
            options.put(RepairOption.DATACENTERS_KEY, StringUtils.join(newArrayList(localDCOption.get()), ","));
        }
        else
        {
            options.put(RepairOption.DATACENTERS_KEY, StringUtils.join(specificDataCenters, ","));
        }
        options.put(RepairOption.HOSTS_KEY, StringUtils.join(specificHosts, ","));
        return options;
    }
}
