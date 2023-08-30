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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.Sets;

import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.commons.lang3.StringUtils;

@Command(name = "repair", description = "Repair one or more tables")
public class Repair extends NodeToolCmd
{
    public final static Set<String> ONLY_EXPLICITLY_REPAIRED = Sets.newHashSet(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);

    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Option(title = "seqential", name = {"-seq", "--sequential"}, description = "Use -seq to carry out a sequential repair")
    private boolean sequential = false;

    @Option(title = "dc parallel", name = {"-dcpar", "--dc-parallel"}, description = "Use -dcpar to repair data centers in parallel.")
    private boolean dcParallel = false;

    @Option(title = "local_dc", name = {"-local", "--in-local-dc"}, description = "Use -local to only repair against nodes in the same datacenter")
    private boolean localDC = false;

    @Option(title = "specific_dc", name = {"-dc", "--in-dc"}, description = "Use -dc to repair specific datacenters")
    private List<String> specificDataCenters = new ArrayList<>();;

    @Option(title = "specific_host", name = {"-hosts", "--in-hosts"}, description = "Use -hosts to repair specific hosts")
    private List<String> specificHosts = new ArrayList<>();

    @Option(title = "start_token", name = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts (exclusive)")
    private String startToken = EMPTY;

    @Option(title = "end_token", name = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends (inclusive)")
    private String endToken = EMPTY;

    @Option(title = "primary_range", name = {"-pr", "--partitioner-range"}, description = "Use -pr to repair only the first range returned by the partitioner")
    private boolean primaryRange = false;

    @Option(title = "full", name = {"-full", "--full"}, description = "Use -full to issue a full repair.")
    private boolean fullRepair = false;

    @Option(title = "force", name = {"-force", "--force"}, description = "Use -force to filter out down endpoints")
    private boolean force = false;

    @Option(title = "preview", name = {"-prv", "--preview"}, description = "Determine ranges and amount of data to be streamed, but don't actually perform repair")
    private boolean preview = false;

    @Option(title = "validate", name = {"-vd", "--validate"}, description = "Checks that repaired data is in sync between nodes. Out of sync repaired data indicates a full repair should be run.")
    private boolean validate = false;

    @Option(title = "job_threads", name = {"-j", "--job-threads"}, description = "Number of threads to run repair jobs. " +
                                                                                 "Usually this means number of CFs to repair concurrently. " +
                                                                                 "WARNING: increasing this puts more load on repairing nodes, so be careful. (default: 1, max: 4)")
    private int numJobThreads = 1;

    @Option(title = "trace_repair", name = {"-tr", "--trace"}, description = "Use -tr to trace the repair. Traces are logged to system_traces.events.")
    private boolean trace = false;

    @Option(title = "pull_repair", name = {"-pl", "--pull"}, description = "Use --pull to perform a one way repair where data is only streamed from a remote node to this node.")
    private boolean pullRepair = false;

    @Option(title = "optimise_streams", name = {"-os", "--optimise-streams"}, description = "Use --optimise-streams to try to reduce the number of streams we do (EXPERIMENTAL, see CASSANDRA-3200).")
    private boolean optimiseStreams = false;

    @Option(title = "skip-paxos", name = {"-skip-paxos", "--skip-paxos"}, description = "If the --skip-paxos flag is included, the paxos repair step is skipped. Paxos repair is also skipped for preview repairs.")
    private boolean skipPaxos = false;

    @Option(title = "paxos-only", name = {"-paxos-only", "--paxos-only"}, description = "If the --paxos-only flag is included, no table data is repaired, only paxos operations..")
    private boolean paxosOnly = false;

    @Option(title = "ignore_unreplicated_keyspaces", name = {"-iuk","--ignore-unreplicated-keyspaces"}, description = "Use --ignore-unreplicated-keyspaces to ignore keyspaces which are not replicated, otherwise the repair will fail")
    private boolean ignoreUnreplicatedKeyspaces = false;

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
        List<String> keyspaces = parseOptionalKeyspace(args, probe, KeyspaceSet.NON_LOCAL_STRATEGY);
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

    public static Map<String, String> parseOptionMap(Supplier<String> localDCOption, List<String> args)
    {
        List<String> realArgs = new ArrayList<>(args.size() + 1);
        realArgs.add("repair");
        realArgs.addAll(args);
        Cli<Object> parser = Cli.builder("fortesting").withCommand(Repair.class).build();
        Repair repair = (Repair) parser.parse(realArgs);
        String[] cfnames = repair.parseOptionalTables(repair.args);
        return repair.createOptions(localDCOption, cfnames);
    }

    private Map<String, String> createOptions(Supplier<String> localDCOption, String[] cfnames)
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
        options.put(RepairOption.REPAIR_PAXOS_KEY, Boolean.toString(!skipPaxos && getPreviewKind() == PreviewKind.NONE));
        options.put(RepairOption.PAXOS_ONLY_KEY, Boolean.toString(paxosOnly && getPreviewKind() == PreviewKind.NONE));

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
