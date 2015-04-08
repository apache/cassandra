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

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.management.InstanceNotFoundException;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "cfstats", description = "Print statistics on tables")
public class CfStats extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace.table>...]", description = "List of tables (or keyspace) names")
    private List<String> cfnames = new ArrayList<>();

    @Option(name = "-i", description = "Ignore the list of tables and display the remaining cfs")
    private boolean ignore = false;

    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KB, MB, GB, TB")
    private boolean humanReadable = false;

    @Override
    public void execute(NodeProbe probe)
    {
        CfStats.OptionFilter filter = new OptionFilter(ignore, cfnames);
        Map<String, List<ColumnFamilyStoreMBean>> cfstoreMap = new HashMap<>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

        while (cfamilies.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
            String keyspaceName = entry.getKey();
            ColumnFamilyStoreMBean cfsProxy = entry.getValue();

            if (!cfstoreMap.containsKey(keyspaceName) && filter.isColumnFamilyIncluded(entry.getKey(), cfsProxy.getColumnFamilyName()))
            {
                List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<>();
                columnFamilies.add(cfsProxy);
                cfstoreMap.put(keyspaceName, columnFamilies);
            } else if (filter.isColumnFamilyIncluded(entry.getKey(), cfsProxy.getColumnFamilyName()))
            {
                cfstoreMap.get(keyspaceName).add(cfsProxy);
            }
        }

        // make sure all specified kss and cfs exist
        filter.verifyKeyspaces(probe.getKeyspaces());
        filter.verifyColumnFamilies();

        // print out the table statistics
        for (Map.Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet())
        {
            String keyspaceName = entry.getKey();
            List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
            long keyspaceReadCount = 0;
            long keyspaceWriteCount = 0;
            int keyspacePendingFlushes = 0;
            double keyspaceTotalReadTime = 0.0f;
            double keyspaceTotalWriteTime = 0.0f;

            System.out.println("Keyspace: " + keyspaceName);
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                String cfName = cfstore.getColumnFamilyName();
                long writeCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteLatency")).getCount();
                long readCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadLatency")).getCount();

                if (readCount > 0)
                {
                    keyspaceReadCount += readCount;
                    keyspaceTotalReadTime += (long) probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadTotalLatency");
                }
                if (writeCount > 0)
                {
                    keyspaceWriteCount += writeCount;
                    keyspaceTotalWriteTime += (long) probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteTotalLatency");
                }
                keyspacePendingFlushes += (long) probe.getColumnFamilyMetric(keyspaceName, cfName, "PendingFlushes");
            }

            double keyspaceReadLatency = keyspaceReadCount > 0
                                         ? keyspaceTotalReadTime / keyspaceReadCount / 1000
                                         : Double.NaN;
            double keyspaceWriteLatency = keyspaceWriteCount > 0
                                          ? keyspaceTotalWriteTime / keyspaceWriteCount / 1000
                                          : Double.NaN;

            System.out.println("\tRead Count: " + keyspaceReadCount);
            System.out.println("\tRead Latency: " + String.format("%s", keyspaceReadLatency) + " ms.");
            System.out.println("\tWrite Count: " + keyspaceWriteCount);
            System.out.println("\tWrite Latency: " + String.format("%s", keyspaceWriteLatency) + " ms.");
            System.out.println("\tPending Flushes: " + keyspacePendingFlushes);

            // print out column family statistics for this keyspace
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                String cfName = cfstore.getColumnFamilyName();
                if (cfName.contains("."))
                    System.out.println("\t\tTable (index): " + cfName);
                else
                    System.out.println("\t\tTable: " + cfName);

                System.out.println("\t\tSSTable count: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "LiveSSTableCount"));

                int[] leveledSStables = cfstore.getSSTableCountPerLevel();
                if (leveledSStables != null)
                {
                    System.out.print("\t\tSSTables in each level: [");
                    for (int level = 0; level < leveledSStables.length; level++)
                    {
                        int count = leveledSStables[level];
                        System.out.print(count);
                        long maxCount = 4L; // for L0
                        if (level > 0)
                            maxCount = (long) Math.pow(10, level);
                        //  show max threshold for level when exceeded
                        if (count > maxCount)
                            System.out.print("/" + maxCount);

                        if (level < leveledSStables.length - 1)
                            System.out.print(", ");
                        else
                            System.out.println("]");
                    }
                }

                Long memtableOffHeapSize = null;
                Long bloomFilterOffHeapSize = null;
                Long indexSummaryOffHeapSize = null;
                Long compressionMetadataOffHeapSize = null;

                Long offHeapSize = null;

                try
                {
                    memtableOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableOffHeapSize");
                    bloomFilterOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterOffHeapMemoryUsed");
                    indexSummaryOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "IndexSummaryOffHeapMemoryUsed");
                    compressionMetadataOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "CompressionMetadataOffHeapMemoryUsed");

                    offHeapSize = memtableOffHeapSize + bloomFilterOffHeapSize + indexSummaryOffHeapSize + compressionMetadataOffHeapSize;
                }
                catch (RuntimeException e)
                {
                    // offheap-metrics introduced in 2.1.3 - older versions do not have the appropriate mbeans
                    if (!(e.getCause() instanceof InstanceNotFoundException))
                        throw e;
                }

                System.out.println("\t\tSpace used (live): " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "LiveDiskSpaceUsed"), humanReadable));
                System.out.println("\t\tSpace used (total): " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "TotalDiskSpaceUsed"), humanReadable));
                System.out.println("\t\tSpace used by snapshots (total): " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "SnapshotsSize"), humanReadable));
                if (offHeapSize != null)
                    System.out.println("\t\tOff heap memory used (total): " + format(offHeapSize, humanReadable));
                System.out.println("\t\tSSTable Compression Ratio: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "CompressionRatio"));
                long numberOfKeys = 0;
                for (long keys : (long[]) probe.getColumnFamilyMetric(keyspaceName, cfName, "EstimatedColumnCountHistogram"))
                    numberOfKeys += keys;
                System.out.println("\t\tNumber of keys (estimate): " + numberOfKeys);
                System.out.println("\t\tMemtable cell count: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableColumnsCount"));
                System.out.println("\t\tMemtable data size: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableLiveDataSize"), humanReadable));
                if (memtableOffHeapSize != null)
                    System.out.println("\t\tMemtable off heap memory used: " + format(memtableOffHeapSize, humanReadable));
                System.out.println("\t\tMemtable switch count: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableSwitchCount"));
                System.out.println("\t\tLocal read count: " + ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadLatency")).getCount());
                double localReadLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "ReadLatency")).getMean() / 1000;
                double localRLatency = localReadLatency > 0 ? localReadLatency : Double.NaN;
                System.out.printf("\t\tLocal read latency: %01.3f ms%n", localRLatency);
                System.out.println("\t\tLocal write count: " + ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteLatency")).getCount());
                double localWriteLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "WriteLatency")).getMean() / 1000;
                double localWLatency = localWriteLatency > 0 ? localWriteLatency : Double.NaN;
                System.out.printf("\t\tLocal write latency: %01.3f ms%n", localWLatency);
                System.out.println("\t\tPending flushes: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "PendingFlushes"));
                System.out.println("\t\tBloom filter false positives: " + probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterFalsePositives"));
                System.out.printf("\t\tBloom filter false ratio: %s%n", String.format("%01.5f", probe.getColumnFamilyMetric(keyspaceName, cfName, "RecentBloomFilterFalseRatio")));
                System.out.println("\t\tBloom filter space used: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterDiskSpaceUsed"), humanReadable));
                if (bloomFilterOffHeapSize != null)
                    System.out.println("\t\tBloom filter off heap memory used: " + format(bloomFilterOffHeapSize, humanReadable));
                if (indexSummaryOffHeapSize != null)
                    System.out.println("\t\tIndex summary off heap memory used: " + format(indexSummaryOffHeapSize, humanReadable));
                if (compressionMetadataOffHeapSize != null)
                    System.out.println("\t\tCompression metadata off heap memory used: " + format(compressionMetadataOffHeapSize, humanReadable));

                System.out.println("\t\tCompacted partition minimum bytes: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MinRowSize"), humanReadable));
                System.out.println("\t\tCompacted partition maximum bytes: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MaxRowSize"), humanReadable));
                System.out.println("\t\tCompacted partition mean bytes: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MeanRowSize"), humanReadable));
                CassandraMetricsRegistry.JmxHistogramMBean histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "LiveScannedHistogram");
                System.out.println("\t\tAverage live cells per slice (last five minutes): " + histogram.getMean());
                System.out.println("\t\tMaximum live cells per slice (last five minutes): " + histogram.getMax());
                histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspaceName, cfName, "TombstoneScannedHistogram");
                System.out.println("\t\tAverage tombstones per slice (last five minutes): " + histogram.getMean());
                System.out.println("\t\tMaximum tombstones per slice (last five minutes): " + histogram.getMax());

                System.out.println("");
            }
            System.out.println("----------------");
        }
    }

    private String format(long bytes, boolean humanReadable) {
        return humanReadable ? FileUtils.stringifyFileSize(bytes) : Long.toString(bytes);
    }

    /**
     * Used for filtering keyspaces and columnfamilies to be displayed using the cfstats command.
     */
    private static class OptionFilter
    {
        private Map<String, List<String>> filter = new HashMap<>();
        private Map<String, List<String>> verifier = new HashMap<>();
        private List<String> filterList = new ArrayList<>();
        private boolean ignoreMode;

        public OptionFilter(boolean ignoreMode, List<String> filterList)
        {
            this.filterList.addAll(filterList);
            this.ignoreMode = ignoreMode;

            for (String s : filterList)
            {
                String[] keyValues = s.split("\\.", 2);

                // build the map that stores the ks' and cfs to use
                if (!filter.containsKey(keyValues[0]))
                {
                    filter.put(keyValues[0], new ArrayList<String>());
                    verifier.put(keyValues[0], new ArrayList<String>());

                    if (keyValues.length == 2)
                    {
                        filter.get(keyValues[0]).add(keyValues[1]);
                        verifier.get(keyValues[0]).add(keyValues[1]);
                    }
                } else
                {
                    if (keyValues.length == 2)
                    {
                        filter.get(keyValues[0]).add(keyValues[1]);
                        verifier.get(keyValues[0]).add(keyValues[1]);
                    }
                }
            }
        }

        public boolean isColumnFamilyIncluded(String keyspace, String columnFamily)
        {
            // supplying empty params list is treated as wanting to display all kss & cfs
            if (filterList.isEmpty())
                return !ignoreMode;

            List<String> cfs = filter.get(keyspace);

            // no such keyspace is in the map
            if (cfs == null)
                return ignoreMode;
                // only a keyspace with no cfs was supplied
                // so ignore or include (based on the flag) every column family in specified keyspace
            else if (cfs.size() == 0)
                return !ignoreMode;

            // keyspace exists, and it contains specific cfs
            verifier.get(keyspace).remove(columnFamily);
            return ignoreMode ^ cfs.contains(columnFamily);
        }

        public void verifyKeyspaces(List<String> keyspaces)
        {
            for (String ks : verifier.keySet())
                if (!keyspaces.contains(ks))
                    throw new IllegalArgumentException("Unknown keyspace: " + ks);
        }

        public void verifyColumnFamilies()
        {
            for (String ks : filter.keySet())
                if (verifier.get(ks).size() > 0)
                    throw new IllegalArgumentException("Unknown tables: " + verifier.get(ks) + " in keyspace: " + ks);
        }
    }
}