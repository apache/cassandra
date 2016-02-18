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

import java.util.*;
import javax.management.InstanceNotFoundException;

import com.google.common.collect.ArrayListMultimap;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "tablestats", description = "Print statistics on tables")
public class TableStats extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace.table>...]", description = "List of tables (or keyspace) names")
    private List<String> tableNames = new ArrayList<>();

    @Option(name = "-i", description = "Ignore the list of tables and display the remaining tables")
    private boolean ignore = false;

    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB")
    private boolean humanReadable = false;

    @Override
    public void execute(NodeProbe probe)
    {
        TableStats.OptionFilter filter = new OptionFilter(ignore, tableNames);
        ArrayListMultimap<String, ColumnFamilyStoreMBean> selectedTableMbeans = ArrayListMultimap.create();
        Map<String, KeyspaceStats> keyspaceStats = new HashMap<>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> tableMBeans = probe.getColumnFamilyStoreMBeanProxies();

        while (tableMBeans.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = tableMBeans.next();
            String keyspaceName = entry.getKey();
            ColumnFamilyStoreMBean tableProxy = entry.getValue();

            if (filter.isKeyspaceIncluded(keyspaceName))
            {
                KeyspaceStats stats = keyspaceStats.get(keyspaceName);
                if (stats == null)
                {
                    stats = new KeyspaceStats(probe, keyspaceName);
                    keyspaceStats.put(keyspaceName, stats);
                }
                stats.add(tableProxy);

                if (filter.isTableIncluded(keyspaceName, tableProxy.getTableName()))
                    selectedTableMbeans.put(keyspaceName, tableProxy);
            }
        }

        // make sure all specified keyspace and tables exist
        filter.verifyKeyspaces(probe.getKeyspaces());
        filter.verifyTables();

        // print out the table statistics
        for (Map.Entry<String, Collection<ColumnFamilyStoreMBean>> entry : selectedTableMbeans.asMap().entrySet())
        {
            String keyspaceName = entry.getKey();
            Collection<ColumnFamilyStoreMBean> tables = entry.getValue();

            System.out.println("Keyspace: " + keyspaceName);
            KeyspaceStats stats = keyspaceStats.get(keyspaceName);

            System.out.println("\tRead Count: " + stats.readCount);
            System.out.println("\tRead Latency: " + String.format("%s", stats.readLatency()) + " ms.");
            System.out.println("\tWrite Count: " + stats.writeCount);
            System.out.println("\tWrite Latency: " + String.format("%s", stats.writeLatency()) + " ms.");
            System.out.println("\tPending Flushes: " + stats.pendingFlushes);

            // print out column family statistics for this keyspace
            for (ColumnFamilyStoreMBean table : tables)
            {
                String tableName = table.getTableName();
                if (tableName.contains("."))
                    System.out.println("\t\tTable (index): " + tableName);
                else
                    System.out.println("\t\tTable: " + tableName);

                System.out.println("\t\tSSTable count: " + probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveSSTableCount"));

                int[] leveledSStables = table.getSSTableCountPerLevel();
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
                    memtableOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableOffHeapSize");
                    bloomFilterOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterOffHeapMemoryUsed");
                    indexSummaryOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "IndexSummaryOffHeapMemoryUsed");
                    compressionMetadataOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "CompressionMetadataOffHeapMemoryUsed");

                    offHeapSize = memtableOffHeapSize + bloomFilterOffHeapSize + indexSummaryOffHeapSize + compressionMetadataOffHeapSize;
                }
                catch (RuntimeException e)
                {
                    // offheap-metrics introduced in 2.1.3 - older versions do not have the appropriate mbeans
                    if (!(e.getCause() instanceof InstanceNotFoundException))
                        throw e;
                }

                System.out.println("\t\tSpace used (live): " + format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveDiskSpaceUsed"), humanReadable));
                System.out.println("\t\tSpace used (total): " + format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "TotalDiskSpaceUsed"), humanReadable));
                System.out.println("\t\tSpace used by snapshots (total): " + format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "SnapshotsSize"), humanReadable));
                if (offHeapSize != null)
                    System.out.println("\t\tOff heap memory used (total): " + format(offHeapSize, humanReadable));
                System.out.println("\t\tSSTable Compression Ratio: " + probe.getColumnFamilyMetric(keyspaceName, tableName, "CompressionRatio"));
                System.out.println("\t\tNumber of keys (estimate): " + probe.getColumnFamilyMetric(keyspaceName, tableName, "EstimatedPartitionCount"));
                System.out.println("\t\tMemtable cell count: " + probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableColumnsCount"));
                System.out.println("\t\tMemtable data size: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableLiveDataSize"), humanReadable));
                if (memtableOffHeapSize != null)
                    System.out.println("\t\tMemtable off heap memory used: " + format(memtableOffHeapSize, humanReadable));
                System.out.println("\t\tMemtable switch count: " + probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableSwitchCount"));
                System.out.println("\t\tLocal read count: " + ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getCount());
                double localReadLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getMean() / 1000;
                double localRLatency = localReadLatency > 0 ? localReadLatency : Double.NaN;
                System.out.printf("\t\tLocal read latency: %01.3f ms%n", localRLatency);
                System.out.println("\t\tLocal write count: " + ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getCount());
                double localWriteLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getMean() / 1000;
                double localWLatency = localWriteLatency > 0 ? localWriteLatency : Double.NaN;
                System.out.printf("\t\tLocal write latency: %01.3f ms%n", localWLatency);
                System.out.println("\t\tPending flushes: " + probe.getColumnFamilyMetric(keyspaceName, tableName, "PendingFlushes"));
                System.out.println("\t\tBloom filter false positives: " + probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterFalsePositives"));
                System.out.printf("\t\tBloom filter false ratio: %s%n", String.format("%01.5f", probe.getColumnFamilyMetric(keyspaceName, tableName, "RecentBloomFilterFalseRatio")));
                System.out.println("\t\tBloom filter space used: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterDiskSpaceUsed"), humanReadable));
                if (bloomFilterOffHeapSize != null)
                    System.out.println("\t\tBloom filter off heap memory used: " + format(bloomFilterOffHeapSize, humanReadable));
                if (indexSummaryOffHeapSize != null)
                    System.out.println("\t\tIndex summary off heap memory used: " + format(indexSummaryOffHeapSize, humanReadable));
                if (compressionMetadataOffHeapSize != null)
                    System.out.println("\t\tCompression metadata off heap memory used: " + format(compressionMetadataOffHeapSize, humanReadable));

                System.out.println("\t\tCompacted partition minimum bytes: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MinPartitionSize"), humanReadable));
                System.out.println("\t\tCompacted partition maximum bytes: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MaxPartitionSize"), humanReadable));
                System.out.println("\t\tCompacted partition mean bytes: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MeanPartitionSize"), humanReadable));
                CassandraMetricsRegistry.JmxHistogramMBean histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveScannedHistogram");
                System.out.println("\t\tAverage live cells per slice (last five minutes): " + histogram.getMean());
                System.out.println("\t\tMaximum live cells per slice (last five minutes): " + histogram.getMax());
                histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "TombstoneScannedHistogram");
                System.out.println("\t\tAverage tombstones per slice (last five minutes): " + histogram.getMean());
                System.out.println("\t\tMaximum tombstones per slice (last five minutes): " + histogram.getMax());
                System.out.println("\t\tDropped Mutations: " + format((Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "DroppedMutations"), humanReadable));

                System.out.println("");
            }
            System.out.println("----------------");
        }
    }

    private String format(long bytes, boolean humanReadable)
    {
        return humanReadable ? FileUtils.stringifyFileSize(bytes) : Long.toString(bytes);
    }

    /**
     * Used for filtering keyspaces and tables to be displayed using the tablestats command.
     */
    private static class OptionFilter
    {
        private Map<String, List<String>> filter = new HashMap<>();
        private Map<String, List<String>> verifier = new HashMap<>(); // Same as filter initially, but we remove tables every time we've checked them for inclusion
                                                                      // in isTableIncluded() so that we detect if those table requested don't exist (verifyTables())
        private List<String> filterList = new ArrayList<>();
        private boolean ignoreMode;

        public OptionFilter(boolean ignoreMode, List<String> filterList)
        {
            this.filterList.addAll(filterList);
            this.ignoreMode = ignoreMode;

            for (String s : filterList)
            {
                String[] keyValues = s.split("\\.", 2);

                // build the map that stores the keyspaces and tables to use
                if (!filter.containsKey(keyValues[0]))
                {
                    filter.put(keyValues[0], new ArrayList<String>());
                    verifier.put(keyValues[0], new ArrayList<String>());
                }

                if (keyValues.length == 2)
                {
                    filter.get(keyValues[0]).add(keyValues[1]);
                    verifier.get(keyValues[0]).add(keyValues[1]);
                }
            }
        }

        public boolean isTableIncluded(String keyspace, String table)
        {
            // supplying empty params list is treated as wanting to display all keyspaces and tables
            if (filterList.isEmpty())
                return !ignoreMode;

            List<String> tables = filter.get(keyspace);

            // no such keyspace is in the map
            if (tables == null)
                return ignoreMode;
                // only a keyspace with no tables was supplied
                // so ignore or include (based on the flag) every column family in specified keyspace
            else if (tables.size() == 0)
                return !ignoreMode;

            // keyspace exists, and it contains specific table
            verifier.get(keyspace).remove(table);
            return ignoreMode ^ tables.contains(table);
        }

        public boolean isKeyspaceIncluded(String keyspace)
        {
            // supplying empty params list is treated as wanting to display all keyspaces and tables
            if (filterList.isEmpty())
                return !ignoreMode;

            // Note that if there is any table for the keyspace, we want to include the keyspace irregarding
            // of the ignoreMode, since the ignoreMode then apply to the table inside the keyspace but the
            // keyspace itself is not ignored
            return filter.get(keyspace) != null || ignoreMode;
        }

        public void verifyKeyspaces(List<String> keyspaces)
        {
            for (String ks : verifier.keySet())
                if (!keyspaces.contains(ks))
                    throw new IllegalArgumentException("Unknown keyspace: " + ks);
        }

        public void verifyTables()
        {
            for (String ks : filter.keySet())
                if (verifier.get(ks).size() > 0)
                    throw new IllegalArgumentException("Unknown tables: " + verifier.get(ks) + " in keyspace: " + ks);
        }
    }

    private static class KeyspaceStats
    {
        private final NodeProbe probe;
        private final String keyspaceName;

        public long readCount;
        public long writeCount;
        public int pendingFlushes;
        private double totalReadTime;
        private double totalWriteTime;

        public KeyspaceStats(NodeProbe probe, String keyspaceName)
        {
            this.probe = probe;
            this.keyspaceName = keyspaceName;
        }

        public void add(ColumnFamilyStoreMBean table)
        {
            String tableName = table.getTableName();
            long tableWriteCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getCount();
            long tableReadCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getCount();

            if (tableReadCount > 0)
            {
                readCount += tableReadCount;
                totalReadTime += (long) probe.getColumnFamilyMetric(keyspaceName, tableName, "ReadTotalLatency");
            }
            if (tableWriteCount > 0)
            {
                writeCount += tableWriteCount;
                totalWriteTime += (long) probe.getColumnFamilyMetric(keyspaceName, tableName, "WriteTotalLatency");
            }
            pendingFlushes += (long) probe.getColumnFamilyMetric(keyspaceName, tableName, "PendingFlushes");
        }

        public double readLatency()
        {
            return readCount > 0
                 ? totalReadTime / readCount / 1000
                 : Double.NaN;
        }

        public double writeLatency()
        {
            return writeCount > 0
                 ? totalWriteTime / writeCount / 1000
                 : Double.NaN;
        }
    }
}
