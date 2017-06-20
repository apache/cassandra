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
package org.apache.cassandra.repair.messages;

import java.util.*;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Repair options.
 */
public class RepairOption
{
    public static final String PARALLELISM_KEY = "parallelism";
    public static final String PRIMARY_RANGE_KEY = "primaryRange";
    public static final String INCREMENTAL_KEY = "incremental";
    public static final String JOB_THREADS_KEY = "jobThreads";
    public static final String RANGES_KEY = "ranges";
    public static final String COLUMNFAMILIES_KEY = "columnFamilies";
    public static final String DATACENTERS_KEY = "dataCenters";
    public static final String HOSTS_KEY = "hosts";
    public static final String TRACE_KEY = "trace";
    public static final String SUB_RANGE_REPAIR_KEY = "sub_range_repair";
    public static final String PULL_REPAIR_KEY = "pullRepair";
    public static final String PREVIEW = "previewKind";

    // we don't want to push nodes too much for repair
    public static final int MAX_JOB_THREADS = 4;

    private static final Logger logger = LoggerFactory.getLogger(RepairOption.class);

    /**
     * Construct RepairOptions object from given map of Strings.
     * <p>
     * Available options are:
     *
     * <table>
     *     <caption>Repair Options</caption>
     *     <thead>
     *         <tr>
     *             <th>key</th>
     *             <th>value</th>
     *             <th>default (when key not given)</th>
     *         </tr>
     *     </thead>
     *     <tbody>
     *         <tr>
     *             <td>parallelism</td>
     *             <td>"sequential", "parallel" or "dc_parallel"</td>
     *             <td>"sequential"</td>
     *         </tr>
     *         <tr>
     *             <td>primaryRange</td>
     *             <td>"true" if perform repair only on primary range.</td>
     *             <td>false</td>
     *         </tr>
     *         <tr>
     *             <td>incremental</td>
     *             <td>"true" if perform incremental repair.</td>
     *             <td>false</td>
     *         </tr>
     *         <tr>
     *             <td>trace</td>
     *             <td>"true" if repair is traced.</td>
     *             <td>false</td>
     *         </tr>
     *         <tr>
     *             <td>jobThreads</td>
     *             <td>Number of threads to use to run repair job.</td>
     *             <td>1</td>
     *         </tr>
     *         <tr>
     *             <td>ranges</td>
     *             <td>Ranges to repair. A range is expressed as &lt;start token&gt;:&lt;end token&gt;
     *             and multiple ranges can be given as comma separated ranges(e.g. aaa:bbb,ccc:ddd).</td>
     *             <td></td>
     *         </tr>
     *         <tr>
     *             <td>columnFamilies</td>
     *             <td>Specify names of ColumnFamilies to repair.
     *             Multiple ColumnFamilies can be given as comma separated values(e.g. cf1,cf2,cf3).</td>
     *             <td></td>
     *         </tr>
     *         <tr>
     *             <td>dataCenters</td>
     *             <td>Specify names of data centers who participate in this repair.
     *             Multiple data centers can be given as comma separated values(e.g. dc1,dc2,dc3).</td>
     *             <td></td>
     *         </tr>
     *         <tr>
     *             <td>hosts</td>
     *             <td>Specify names of hosts who participate in this repair.
     *             Multiple hosts can be given as comma separated values(e.g. cass1,cass2).</td>
     *             <td></td>
     *         </tr>
     *         <tr>
     *             <td>pullRepair</td>
     *             <td>"true" if the repair should only stream data one way from a remote host to this host.
     *             This is only allowed if exactly 2 hosts are specified along with a token range that they share.</td>
     *             <td>false</td>
     *         </tr>
     *     </tbody>
     * </table>
     *
     * @param options options to parse
     * @param partitioner partitioner is used to construct token ranges
     * @return RepairOptions object
     */
    public static RepairOption parse(Map<String, String> options, IPartitioner partitioner)
    {
        // if no parallel option is given, then this will be "sequential" by default.
        RepairParallelism parallelism = RepairParallelism.fromName(options.get(PARALLELISM_KEY));
        boolean primaryRange = Boolean.parseBoolean(options.get(PRIMARY_RANGE_KEY));
        boolean incremental = Boolean.parseBoolean(options.get(INCREMENTAL_KEY));
        PreviewKind previewKind = PreviewKind.valueOf(options.getOrDefault(PREVIEW, PreviewKind.NONE.toString()));
        boolean trace = Boolean.parseBoolean(options.get(TRACE_KEY));
        boolean pullRepair = Boolean.parseBoolean(options.get(PULL_REPAIR_KEY));

        int jobThreads = 1;
        if (options.containsKey(JOB_THREADS_KEY))
        {
            try
            {
                jobThreads = Integer.parseInt(options.get(JOB_THREADS_KEY));
            }
            catch (NumberFormatException ignore) {}
        }
        // ranges
        String rangesStr = options.get(RANGES_KEY);
        Set<Range<Token>> ranges = new HashSet<>();
        if (rangesStr != null)
        {
            if (incremental)
                logger.warn("Incremental repair can't be requested with subrange repair " +
                            "because each subrange repair would generate an anti-compacted table. " +
                            "The repair will occur but without anti-compaction.");
            StringTokenizer tokenizer = new StringTokenizer(rangesStr, ",");
            while (tokenizer.hasMoreTokens())
            {
                String[] rangeStr = tokenizer.nextToken().split(":", 2);
                if (rangeStr.length < 2)
                {
                    continue;
                }
                Token parsedBeginToken = partitioner.getTokenFactory().fromString(rangeStr[0].trim());
                Token parsedEndToken = partitioner.getTokenFactory().fromString(rangeStr[1].trim());
                if (parsedBeginToken.equals(parsedEndToken))
                {
                    throw new IllegalArgumentException("Start and end tokens must be different.");
                }
                ranges.add(new Range<>(parsedBeginToken, parsedEndToken));
            }
        }

        RepairOption option = new RepairOption(parallelism, primaryRange, incremental, trace, jobThreads, ranges, !ranges.isEmpty(), pullRepair, previewKind);

        // data centers
        String dataCentersStr = options.get(DATACENTERS_KEY);
        Collection<String> dataCenters = new HashSet<>();
        if (dataCentersStr != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(dataCentersStr, ",");
            while (tokenizer.hasMoreTokens())
            {
                dataCenters.add(tokenizer.nextToken().trim());
            }
            option.getDataCenters().addAll(dataCenters);
        }

        // hosts
        String hostsStr = options.get(HOSTS_KEY);
        Collection<String> hosts = new HashSet<>();
        if (hostsStr != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(hostsStr, ",");
            while (tokenizer.hasMoreTokens())
            {
                hosts.add(tokenizer.nextToken().trim());
            }
            option.getHosts().addAll(hosts);
        }

        // columnfamilies
        String cfStr = options.get(COLUMNFAMILIES_KEY);
        if (cfStr != null)
        {
            Collection<String> columnFamilies = new HashSet<>();
            StringTokenizer tokenizer = new StringTokenizer(cfStr, ",");
            while (tokenizer.hasMoreTokens())
            {
                columnFamilies.add(tokenizer.nextToken().trim());
            }
            option.getColumnFamilies().addAll(columnFamilies);
        }

        // validate options
        if (jobThreads > MAX_JOB_THREADS)
        {
            throw new IllegalArgumentException("Too many job threads. Max is " + MAX_JOB_THREADS);
        }
        if (!dataCenters.isEmpty() && !hosts.isEmpty())
        {
            throw new IllegalArgumentException("Cannot combine -dc and -hosts options.");
        }
        if (primaryRange && ((!dataCenters.isEmpty() && !option.isInLocalDCOnly()) || !hosts.isEmpty()))
        {
            throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
        }
        if (pullRepair)
        {
            if (hosts.size() != 2)
            {
                throw new IllegalArgumentException("Pull repair can only be performed between two hosts. Please specify two hosts, one of which must be this host.");
            }
            else if (ranges.isEmpty())
            {
                throw new IllegalArgumentException("Token ranges must be specified when performing pull repair. Please specify at least one token range which both hosts have in common.");
            }
        }

        if (option.isIncremental() && !option.isPreview() && !option.isGlobal())
        {
            throw new IllegalArgumentException("Incremental repairs cannot be run against a subset of tokens or ranges");
        }

        return option;
    }

    private final RepairParallelism parallelism;
    private final boolean primaryRange;
    private final boolean incremental;
    private final boolean trace;
    private final int jobThreads;
    private final boolean isSubrangeRepair;
    private final boolean pullRepair;
    private final PreviewKind previewKind;

    private final Collection<String> columnFamilies = new HashSet<>();
    private final Collection<String> dataCenters = new HashSet<>();
    private final Collection<String> hosts = new HashSet<>();
    private final Collection<Range<Token>> ranges = new HashSet<>();

    public RepairOption(RepairParallelism parallelism, boolean primaryRange, boolean incremental, boolean trace, int jobThreads, Collection<Range<Token>> ranges, boolean isSubrangeRepair, boolean pullRepair, PreviewKind previewKind)
    {
        if (FBUtilities.isWindows &&
            (DatabaseDescriptor.getDiskAccessMode() != Config.DiskAccessMode.standard || DatabaseDescriptor.getIndexAccessMode() != Config.DiskAccessMode.standard) &&
            parallelism == RepairParallelism.SEQUENTIAL)
        {
            logger.warn("Sequential repair disabled when memory-mapped I/O is configured on Windows. Reverting to parallel.");
            this.parallelism = RepairParallelism.PARALLEL;
        }
        else
            this.parallelism = parallelism;

        this.primaryRange = primaryRange;
        this.incremental = incremental;
        this.trace = trace;
        this.jobThreads = jobThreads;
        this.ranges.addAll(ranges);
        this.isSubrangeRepair = isSubrangeRepair;
        this.pullRepair = pullRepair;
        this.previewKind = previewKind;
    }

    public RepairParallelism getParallelism()
    {
        return parallelism;
    }

    public boolean isPrimaryRange()
    {
        return primaryRange;
    }

    public boolean isIncremental()
    {
        return incremental;
    }

    public boolean isTraced()
    {
        return trace;
    }

    public boolean isPullRepair()
    {
        return pullRepair;
    }

    public int getJobThreads()
    {
        return jobThreads;
    }

    public Collection<String> getColumnFamilies()
    {
        return columnFamilies;
    }

    public Collection<Range<Token>> getRanges()
    {
        return ranges;
    }

    public Collection<String> getDataCenters()
    {
        return dataCenters;
    }

    public Collection<String> getHosts()
    {
        return hosts;
    }

    public boolean isGlobal()
    {
        return dataCenters.isEmpty() && hosts.isEmpty() && !isSubrangeRepair();
    }

    public boolean isSubrangeRepair()
    {
        return isSubrangeRepair;
    }

    public PreviewKind getPreviewKind()
    {
        return previewKind;
    }

    public boolean isPreview()
    {
        return previewKind.isPreview();
    }

    public boolean isInLocalDCOnly() {
        return dataCenters.size() == 1 && dataCenters.contains(DatabaseDescriptor.getLocalDataCenter());
    }

    @Override
    public String toString()
    {
        return "repair options (" +
               "parallelism: " + parallelism +
               ", primary range: " + primaryRange +
               ", incremental: " + incremental +
               ", job threads: " + jobThreads +
               ", ColumnFamilies: " + columnFamilies +
               ", dataCenters: " + dataCenters +
               ", hosts: " + hosts +
               ", previewKind: " + previewKind +
               ", # of ranges: " + ranges.size() +
               ", pull repair: " + pullRepair +
               ')';
    }

    public Map<String, String> asMap()
    {
        Map<String, String> options = new HashMap<>();
        options.put(PARALLELISM_KEY, parallelism.toString());
        options.put(PRIMARY_RANGE_KEY, Boolean.toString(primaryRange));
        options.put(INCREMENTAL_KEY, Boolean.toString(incremental));
        options.put(JOB_THREADS_KEY, Integer.toString(jobThreads));
        options.put(COLUMNFAMILIES_KEY, Joiner.on(",").join(columnFamilies));
        options.put(DATACENTERS_KEY, Joiner.on(",").join(dataCenters));
        options.put(HOSTS_KEY, Joiner.on(",").join(hosts));
        options.put(SUB_RANGE_REPAIR_KEY, Boolean.toString(isSubrangeRepair));
        options.put(TRACE_KEY, Boolean.toString(trace));
        options.put(RANGES_KEY, Joiner.on(",").join(ranges));
        options.put(PULL_REPAIR_KEY, Boolean.toString(pullRepair));
        options.put(PREVIEW, previewKind.toString());
        return options;
    }
}
