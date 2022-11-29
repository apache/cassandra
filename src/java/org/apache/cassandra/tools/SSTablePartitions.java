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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class SSTablePartitions
{
    private static final String KEY_OPTION = "k";
    private static final String EXCLUDE_KEY_OPTION = "x";
    private static final String RECURSIVE_OPTION = "r";
    private static final String SNAPSHOTS_OPTION = "s";
    private static final String BACKUPS_OPTION = "b";
    private static final String PARTITIONS_ONLY_OPTION = "y";
    private static final String SIZE_THRESHOLD_OPTION = "t";
    private static final String TOMBSTONE_THRESHOLD_OPTION = "o";
    private static final String CELL_THRESHOLD_OPTION = "c";
    private static final String ROW_THRESHOLD_OPTION = "w";
    private static final String CSV_OPTION = "m";
    private static final String CURRENT_TIMESTAMP_OPTION = "u";

    private static final Options options = new Options();

    private static final TableId EMPTY_TABLE_ID = TableId.fromUUID(new UUID(0L, 0L));

    static
    {
        DatabaseDescriptor.clientInitialization();

        Option optKey = new Option(KEY_OPTION, "key", true, "Partition keys to include");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, "exclude-key", true,
                                       "Excluded partition key(s) from partition detailed row/cell/tombstone " +
                                       "information (irrelevant, if --partitions-only is given)");
        excludeKey.setArgs(Option.UNLIMITED_VALUES); // Number of times -x <key> can be passed on the command line.
        options.addOption(excludeKey);

        Option thresholdKey = new Option(SIZE_THRESHOLD_OPTION, "min-size", true,
                                         "partition size threshold, expressed as either the number of bytes or a " +
                                         "size with unit of the form 10KiB, 20MiB, 30GiB, etc.");
        options.addOption(thresholdKey);

        Option tombstoneKey = new Option(TOMBSTONE_THRESHOLD_OPTION, "min-tombstones", true,
                                         "partition tombstone count threshold");
        options.addOption(tombstoneKey);

        Option cellKey = new Option(CELL_THRESHOLD_OPTION, "min-cells", true, "partition cell count threshold");
        options.addOption(cellKey);

        Option rowKey = new Option(ROW_THRESHOLD_OPTION, "min-rows", true, "partition row count threshold");
        options.addOption(rowKey);

        Option currentTimestampKey = new Option(CURRENT_TIMESTAMP_OPTION, "current-timestamp", true,
                                                "timestamp (seconds since epoch, unit time) for TTL expired calculation");
        options.addOption(currentTimestampKey);

        Option recursiveKey = new Option(RECURSIVE_OPTION, "recursive", false, "scan for sstables recursively");
        options.addOption(recursiveKey);

        Option snapshotsKey = new Option(SNAPSHOTS_OPTION, "snapshots", false,
                                         "include snapshots present in data directories (recursive scans)");
        options.addOption(snapshotsKey);

        Option backupsKey = new Option(BACKUPS_OPTION, "backups", false,
                                       "include backups present in data directories (recursive scans)");
        options.addOption(backupsKey);

        Option partitionsOnlyKey = new Option(PARTITIONS_ONLY_OPTION, "partitions-only", false,
                                              "Do not process per-partition detailed row/cell/tombstone information, " +
                                              "only brief information");
        options.addOption(partitionsOnlyKey);

        Option csvKey = new Option(CSV_OPTION, "csv", false, "CSV output (machine readable)");
        options.addOption(csvKey);
    }

    /**
     * Given arguments specifying a list of SSTables or directories, print information about SSTable partitions.
     *
     * @param args command lines arguments
     * @throws ConfigurationException on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws ConfigurationException, IOException
    {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e)
        {
            System.err.println(e.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        if (cmd.getArgs().length == 0)
        {
            System.err.println("You must supply at least one sstable or directory");
            printUsage();
            System.exit(1);
        }

        int ec = processArguments(cmd);

        System.exit(ec);
    }

    private static void printUsage()
    {
        String usage = String.format("sstablepartitions <options> <sstable files or directories>%n");
        String header = "Print partition statistics of one or more sstables.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }

    private static int processArguments(CommandLine cmd) throws IOException
    {
        String[] keys = cmd.getOptionValues(KEY_OPTION);
        Set<String> excludes = cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null
                               ? Collections.emptySet()
                               : ImmutableSet.copyOf(cmd.getOptionValues(EXCLUDE_KEY_OPTION));

        boolean scanRecursive = cmd.hasOption(RECURSIVE_OPTION);
        boolean withSnapshots = cmd.hasOption(SNAPSHOTS_OPTION);
        boolean withBackups = cmd.hasOption(BACKUPS_OPTION);
        boolean csv = cmd.hasOption(CSV_OPTION);
        boolean partitionsOnly = cmd.hasOption(PARTITIONS_ONLY_OPTION);

        long sizeThreshold = Long.MAX_VALUE;
        int cellCountThreshold = Integer.MAX_VALUE;
        int rowCountThreshold = Integer.MAX_VALUE;
        int tombstoneCountThreshold = Integer.MAX_VALUE;
        long currentTime = Clock.Global.currentTimeMillis() / 1000L;

        try
        {
            if (cmd.hasOption(SIZE_THRESHOLD_OPTION))
            {
                String threshold = cmd.getOptionValue(SIZE_THRESHOLD_OPTION);
                sizeThreshold = NumberUtils.isParsable(threshold)
                                ? Long.parseLong(threshold)
                                : new DataStorageSpec.LongBytesBound(threshold).toBytes();
            }
            if (cmd.hasOption(CELL_THRESHOLD_OPTION))
                cellCountThreshold = Integer.parseInt(cmd.getOptionValue(CELL_THRESHOLD_OPTION));
            if (cmd.hasOption(ROW_THRESHOLD_OPTION))
                rowCountThreshold = Integer.parseInt(cmd.getOptionValue(ROW_THRESHOLD_OPTION));
            if (cmd.hasOption(TOMBSTONE_THRESHOLD_OPTION))
                tombstoneCountThreshold = Integer.parseInt(cmd.getOptionValue(TOMBSTONE_THRESHOLD_OPTION));
            if (cmd.hasOption(CURRENT_TIMESTAMP_OPTION))
                currentTime = Integer.parseInt(cmd.getOptionValue(CURRENT_TIMESTAMP_OPTION));
        }
        catch (NumberFormatException e)
        {
            System.err.printf("Invalid threshold argument: %s%n", e.getMessage());
            return 1;
        }

        if (sizeThreshold < 0 || cellCountThreshold < 0 || tombstoneCountThreshold < 0 || currentTime < 0)
        {
            System.err.println("Negative values are not allowed");
            return 1;
        }

        List<File> directories = new ArrayList<>();
        List<ExtendedDescriptor> descriptors = new ArrayList<>();

        if (!argumentsToFiles(cmd.getArgs(), descriptors, directories))
            return 1;

        for (File directory : directories)
        {
            processDirectory(scanRecursive, withSnapshots, withBackups, directory, descriptors);
        }

        if (csv)
            System.out.println("key,keyBinary,live,offset,size,rowCount,cellCount," +
                               "tombstoneCount,rowTombstoneCount,rangeTombstoneCount,complexTombstoneCount," +
                               "cellTombstoneCount,rowTtlExpired,cellTtlExpired," +
                               "directory,keyspace,table,index," +
                               "snapshot,backup,generation,format,version");

        Collections.sort(descriptors);

        for (ExtendedDescriptor desc : descriptors)
        {
            processSSTable(keys, excludes, desc,
                           sizeThreshold, cellCountThreshold, rowCountThreshold, tombstoneCountThreshold, partitionsOnly,
                           csv, currentTime);
        }

        return 0;
    }

    private static void processDirectory(boolean scanRecursive,
                                         boolean withSnapshots,
                                         boolean withBackups,
                                         File dir,
                                         List<ExtendedDescriptor> descriptors)
    {
        File[] files = dir.tryList();
        if (files == null)
            return;

        for (File file : files)
        {
            if (file.isFile())
            {
                try
                {
                    if (Descriptor.componentFromFile(file) != BigFormat.Components.DATA)
                        continue;

                    ExtendedDescriptor desc = ExtendedDescriptor.guessFromFile(file);
                    if (desc.snapshot != null && !withSnapshots)
                        continue;
                    if (desc.backup != null && !withBackups)
                        continue;

                    descriptors.add(desc);
                }
                catch (IllegalArgumentException e)
                {
                    // ignore that error when scanning directories
                }
            }
            if (scanRecursive && file.isDirectory())
            {
                processDirectory(true,
                                 withSnapshots, withBackups,
                                 file,
                                 descriptors);
            }
        }
    }

    private static boolean argumentsToFiles(String[] args, List<ExtendedDescriptor> descriptors, List<File> directories)
    {
        boolean err = false;
        for (String arg : args)
        {
            File file = new File(arg);
            if (!file.exists())
            {
                System.err.printf("Argument '%s' does not resolve to a file or directory%n", arg);
                err = true;
            }

            if (!file.isReadable())
            {
                System.err.printf("Argument '%s' is not a readable file or directory (check permissions)%n", arg);
                err = true;
                continue;
            }

            if (file.isFile())
            {
                try
                {
                    descriptors.add(ExtendedDescriptor.guessFromFile(file));
                }
                catch (IllegalArgumentException e)
                {
                    System.err.printf("Argument '%s' is not an sstable%n", arg);
                    err = true;
                }
            }
            if (file.isDirectory())
                directories.add(file);
        }
        return !err;
    }

    private static void processSSTable(String[] keys,
                                       Set<String> excludedKeys,
                                       ExtendedDescriptor desc,
                                       long sizeThreshold,
                                       int cellCountThreshold,
                                       int rowCountThreshold,
                                       int tombstoneCountThreshold,
                                       boolean partitionsOnly,
                                       boolean csv,
                                       long currentTime) throws IOException
    {
        TableMetadata metadata = Util.metadataFromSSTable(desc.descriptor);
        SSTableReader sstable = SSTableReader.openNoValidation(null, desc.descriptor, TableMetadataRef.forOfflineTools(metadata));

        if (!csv)
            System.out.printf("%nProcessing %s (%s uncompressed, %s on disk)%n",
                              desc,
                              prettyPrintMemory(sstable.uncompressedLength()),
                              prettyPrintMemory(sstable.onDiskLength()));

        List<PartitionStats> matches = new ArrayList<>();
        SSTableStats sstableStats = new SSTableStats();

        try (ISSTableScanner scanner = buildScanner(sstable, metadata, keys, excludedKeys))
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    ByteBuffer key = partition.partitionKey().getKey();
                    boolean isExcluded = excludedKeys.contains(metadata.partitionKeyType.getString(key));

                    PartitionStats partitionStats = new PartitionStats(key,
                                                                       scanner.getCurrentPosition(),
                                                                       partition.partitionLevelDeletion().isLive());

                    // Consume the partition to populate the stats.
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();

                        // We don't need any details if we are only interested on its size or if it's excluded.
                        if (!partitionsOnly && !isExcluded)
                            partitionStats.addUnfiltered(desc, currentTime, unfiltered);
                    }

                    // record the partiton size
                    partitionStats.endOfPartition(scanner.getCurrentPosition());

                    if (isExcluded)
                        continue;

                    sstableStats.addPartition(partitionStats);

                    if (partitionStats.size < sizeThreshold &&
                        partitionStats.rowCount < rowCountThreshold &&
                        partitionStats.cellCount < cellCountThreshold &&
                        partitionStats.tombstoneCount() < tombstoneCountThreshold)
                        continue;

                    matches.add(partitionStats);
                    if (csv)
                        partitionStats.printPartitionInfoCSV(metadata, desc);
                    else
                        partitionStats.printPartitionInfo(metadata, partitionsOnly);
                }
            }
        }
        catch (RuntimeException e)
        {
            System.err.printf("Failure processing sstable %s: %s%n", desc.descriptor, e);
        }
        finally
        {
            sstable.selfRef().release();
        }

        if (!csv)
        {
            printSummary(metadata, desc, sstableStats, matches, partitionsOnly);
        }
    }

    private static String prettyPrintMemory(long bytes)
    {
        return FBUtilities.prettyPrintMemory(bytes, " ");
    }

    private static ISSTableScanner buildScanner(SSTableReader sstable,
                                                TableMetadata metadata,
                                                String[] keys,
                                                Set<String> excludedKeys)
    {
        if (keys != null && keys.length > 0)
        {
            try
            {
                return sstable.getScanner(Arrays.stream(keys)
                                                .filter(key -> !excludedKeys.contains(key))
                                                .map(metadata.partitionKeyType::fromString)
                                                .map(k -> sstable.getPartitioner().decorateKey(k))
                                                .sorted()
                                                .map(DecoratedKey::getToken)
                                                .map(token -> new Bounds<>(token.minKeyBound(), token.maxKeyBound()))
                                                .collect(Collectors.<AbstractBounds<PartitionPosition>>toList())
                                                .iterator());
            }
            catch (RuntimeException e)
            {
                System.err.printf("Cannot use one or more partition keys in %s for the partition key type ('%s') " +
                                  "of the underlying table: %s%n",
                                  Arrays.toString(keys),
                                  metadata.partitionKeyType.asCQL3Type(), e);
            }
        }
        return sstable.getScanner();
    }

    private static void printSummary(TableMetadata metadata,
                                     ExtendedDescriptor desc,
                                     SSTableStats stats,
                                     List<PartitionStats> matches,
                                     boolean partitionsOnly)
    {
        // Print header
        if (!matches.isEmpty())
        {
            System.out.printf("Summary of %s:%n" +
                              "  File: %s%n" +
                              "  %d partitions match%n" +
                              "  Keys:", desc, desc.descriptor.fileFor(BigFormat.Components.DATA), matches.size());

            for (PartitionStats match : matches)
                System.out.print(" " + maybeEscapeKeyForSummary(metadata, match.key));

            System.out.println();
        }

        // Print stats table columns
        String format;
        if (partitionsOnly)
        {
            System.out.printf("         %20s%n", "Partition size");
            format = "  %-5s  %20s%n";
        }
        else
        {
            System.out.printf("         %20s %20s %20s %20s%n", "Partition size", "Row count", "Cell count", "Tombstone count");
            format = "  %-5s  %20s %20d %20d %20d%n";
        }

        // Print approximate percentiles from the histograms
        printPercentile(partitionsOnly, stats, format, "~p50", h -> h.percentile(.5d));
        printPercentile(partitionsOnly, stats, format, "~p75", h -> h.percentile(.75d));
        printPercentile(partitionsOnly, stats, format, "~p90", h -> h.percentile(.90d));
        printPercentile(partitionsOnly, stats, format, "~p95", h -> h.percentile(.95d));
        printPercentile(partitionsOnly, stats, format, "~p99", h -> h.percentile(.99d));
        printPercentile(partitionsOnly, stats, format, "~p999", h -> h.percentile(.999d));

        // Print accurate metrics (min/max/count)
        if (partitionsOnly)
        {
            System.out.printf(format, "min", prettyPrintMemory(stats.minSize));
            System.out.printf(format, "max", prettyPrintMemory(stats.maxSize));
        }
        else
        {
            System.out.printf(format,
                              "min",
                              prettyPrintMemory(stats.minSize),
                              stats.minRowCount,
                              stats.minCellCount,
                              stats.minTombstoneCount);
            System.out.printf(format,
                              "max",
                              prettyPrintMemory(stats.maxSize),
                              stats.maxRowCount,
                              stats.maxCellCount,
                              stats.maxTombstoneCount);
        }
        System.out.printf("  count  %20d%n", stats.partitionSizeHistogram.count());
    }

    private static void printPercentile(boolean partitionsOnly,
                                        SSTableStats stats,
                                        String format,
                                        String header,
                                        ToLongFunction<EstimatedHistogram> value)
    {
        if (partitionsOnly)
        {
            System.out.printf(format,
                              header,
                              prettyPrintMemory(value.applyAsLong(stats.partitionSizeHistogram)));
        }
        else
        {
            System.out.printf(format,
                              header,
                              prettyPrintMemory(value.applyAsLong(stats.partitionSizeHistogram)),
                              value.applyAsLong(stats.rowCountHistogram),
                              value.applyAsLong(stats.cellCountHistogram),
                              value.applyAsLong(stats.tombstoneCountHistogram));
        }
    }

    private static String maybeEscapeKeyForSummary(TableMetadata metadata, ByteBuffer key)
    {
        String s = metadata.partitionKeyType.getString(key);
        if (s.indexOf(' ') == -1)
            return s;
        return "\"" + StringUtils.replace(s, "\"", "\"\"") + "\"";
    }

    static final class SSTableStats
    {
        // EH of 155 can track a max value of 3520571548412 i.e. 3.5TB
        EstimatedHistogram partitionSizeHistogram = new EstimatedHistogram(155, true);

        // EH of 118 can track a max value of 4139110981, i.e., > 4B rows, cells or tombstones
        EstimatedHistogram rowCountHistogram = new EstimatedHistogram(118, true);
        EstimatedHistogram cellCountHistogram = new EstimatedHistogram(118, true);
        EstimatedHistogram tombstoneCountHistogram = new EstimatedHistogram(118, true);

        long minSize = 0;
        long maxSize = 0;

        int minRowCount = 0;
        int maxRowCount = 0;

        int minCellCount = 0;
        int maxCellCount = 0;

        int minTombstoneCount = 0;
        int maxTombstoneCount = 0;

        void addPartition(PartitionStats stats)
        {
            partitionSizeHistogram.add(stats.size);
            rowCountHistogram.add(stats.rowCount);
            cellCountHistogram.add(stats.cellCount);
            tombstoneCountHistogram.add(stats.tombstoneCount());

            if (minSize == 0 || stats.size < minSize)
                minSize = stats.size;
            if (stats.size > maxSize)
                maxSize = stats.size;

            if (minRowCount == 0 || stats.rowCount < minRowCount)
                minRowCount = stats.rowCount;
            if (stats.rowCount > maxRowCount)
                maxRowCount = stats.rowCount;

            if (minCellCount == 0 || stats.cellCount < minCellCount)
                minCellCount = stats.cellCount;
            if (stats.cellCount > maxCellCount)
                maxCellCount = stats.cellCount;

            if (minTombstoneCount == 0 || stats.tombstoneCount() < minTombstoneCount)
                minTombstoneCount = stats.tombstoneCount();
            if (stats.tombstoneCount() > maxTombstoneCount)
                maxTombstoneCount = stats.tombstoneCount();
        }
    }

    static final class PartitionStats
    {
        final ByteBuffer key;
        final long offset;
        final boolean live;

        long size = -1;
        int rowCount = 0;
        int cellCount = 0;
        int rowTombstoneCount = 0;
        int rangeTombstoneCount = 0;
        int complexTombstoneCount = 0;
        int cellTombstoneCount = 0;
        int rowTtlExpired = 0;
        int cellTtlExpired = 0;

        PartitionStats(ByteBuffer key, long offset, boolean live)
        {
            this.key = key;
            this.offset = offset;
            this.live = live;
        }

        void endOfPartition(long position)
        {
            size = position - offset;
        }

        int tombstoneCount()
        {
            return rowTombstoneCount + rangeTombstoneCount + complexTombstoneCount + cellTombstoneCount + rowTtlExpired + cellTtlExpired;
        }

        void addUnfiltered(ExtendedDescriptor desc, long currentTime, Unfiltered unfiltered)
        {
            if (unfiltered instanceof Row)
            {
                Row row = (Row) unfiltered;
                rowCount++;

                if (!row.deletion().isLive())
                    rowTombstoneCount++;

                LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
                if (!liveInfo.isEmpty() && liveInfo.isExpiring() && liveInfo.localExpirationTime() < currentTime)
                    rowTtlExpired++;

                for (ColumnData cd : row)
                {

                    if (cd.column().isSimple())
                    {
                        addCell((int) currentTime, liveInfo, (Cell<?>) cd);
                    }
                    else
                    {
                        ComplexColumnData complexData = (ComplexColumnData) cd;
                        if (!complexData.complexDeletion().isLive())
                            complexTombstoneCount++;

                        for (Cell<?> cell : complexData)
                            addCell((int) currentTime, liveInfo, cell);
                    }
                }
            }
            else if (unfiltered instanceof RangeTombstoneMarker)
            {
                rangeTombstoneCount++;
            }
            else
            {
                throw new UnsupportedOperationException("Unknown kind " + unfiltered.kind() + " in sstable " + desc.descriptor);
            }
        }

        private void addCell(int currentTime, LivenessInfo liveInfo, Cell<?> cell)
        {
            cellCount++;
            if (cell.isTombstone())
                cellTombstoneCount++;
            if (cell.isExpiring() && (liveInfo.isEmpty() || cell.ttl() != liveInfo.ttl()) && !cell.isLive(currentTime))
                cellTtlExpired++;
        }

        void printPartitionInfo(TableMetadata metadata, boolean partitionsOnly)
        {
            String key = metadata.partitionKeyType.getString(this.key);
            if (partitionsOnly)
                System.out.printf("  Partition: '%s' (%s) %s, size: %s%n",
                                  key,
                                  ByteBufferUtil.bytesToHex(this.key), live ? "live" : "not live",
                                  prettyPrintMemory(size));
            else
                System.out.printf("  Partition: '%s' (%s) %s, size: %s, rows: %d, cells: %d, " +
                                  "tombstones: %d (row:%d, range:%d, complex:%d, cell:%d, row-TTLd:%d, cell-TTLd:%d)%n",
                                  key,
                                  ByteBufferUtil.bytesToHex(this.key),
                                  live ? "live" : "not live",
                                  prettyPrintMemory(size),
                                  rowCount,
                                  cellCount,
                                  tombstoneCount(),
                                  rowTombstoneCount,
                                  rangeTombstoneCount,
                                  complexTombstoneCount,
                                  cellTombstoneCount,
                                  rowTtlExpired,
                                  cellTtlExpired);
        }

        void printPartitionInfoCSV(TableMetadata metadata, ExtendedDescriptor desc)
        {
            System.out.printf("\"%s\",%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s%n",
                              maybeEscapeKeyForSummary(metadata, key),
                              ByteBufferUtil.bytesToHex(key),
                              live ? "true" : "false",
                              offset, size,
                              rowCount, cellCount, tombstoneCount(),
                              rowTombstoneCount, rangeTombstoneCount, complexTombstoneCount, cellTombstoneCount,
                              rowTtlExpired, cellTtlExpired,
                              desc.descriptor.fileFor(BigFormat.Components.DATA),
                              notNull(desc.keyspace),
                              notNull(desc.table),
                              notNull(desc.index),
                              notNull(desc.snapshot),
                              notNull(desc.backup),
                              desc.descriptor.id,
                              desc.descriptor.version.format.name(),
                              desc.descriptor.version.version);
        }
    }

    static final class ExtendedDescriptor implements Comparable<ExtendedDescriptor>
    {
        final String keyspace;
        final String table;
        final String index;
        final String snapshot;
        final String backup;
        final TableId tableId;
        final Descriptor descriptor;

        ExtendedDescriptor(String keyspace, String table, TableId tableId, String index, String snapshot, String backup, Descriptor descriptor)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.tableId = tableId;
            this.index = index;
            this.snapshot = snapshot;
            this.backup = backup;
            this.descriptor = descriptor;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            if (backup != null)
                sb.append("Backup:").append(backup).append(' ');
            if (snapshot != null)
                sb.append("Snapshot:").append(snapshot).append(' ');
            if (keyspace != null)
                sb.append(keyspace).append('.');
            if (table != null)
                sb.append(table);
            if (index != null)
                sb.append('.').append(index);
            if (tableId != null)
                sb.append('-').append(tableId.toHexString());
            return sb.append(" #")
                     .append(descriptor.id)
                     .append(" (")
                     .append(descriptor.version.format.name())
                     .append('-')
                     .append(descriptor.version.version)
                     .append(')')
                     .toString();
        }

        static ExtendedDescriptor guessFromFile(File fArg)
        {
            Descriptor desc = Descriptor.fromFile(fArg);

            String snapshot = null;
            String backup = null;
            String index = null;

            File parent = fArg.parent();
            File grandparent = parent.parent();

            if (parent.name().length() > 1 && parent.name().startsWith(".") && parent.name().charAt(1) != '.')
            {
                index = parent.name().substring(1);
                parent = parent.parent();
                grandparent = parent.parent();
            }

            if (parent.name().equals(Directories.BACKUPS_SUBDIR))
            {
                backup = parent.name();
                parent = parent.parent();
                grandparent = parent.parent();
            }

            if (grandparent.name().equals(Directories.SNAPSHOT_SUBDIR))
            {
                snapshot = parent.name();
                parent = grandparent.parent();
                grandparent = parent.parent();
            }

            try
            {
                Pair<String, TableId> tableNameAndId = TableId.tableNameAndIdFromFilename(parent.name());
                if (tableNameAndId != null)
                {
                    return new ExtendedDescriptor(grandparent.name(),
                                                  tableNameAndId.left,
                                                  tableNameAndId.right,
                                                  index,
                                                  snapshot,
                                                  backup,
                                                  desc);
                }
            }
            catch (NumberFormatException e)
            {
                // ignore non-parseable table-IDs
            }

            return new ExtendedDescriptor(null,
                                          null,
                                          null,
                                          index,
                                          snapshot,
                                          backup,
                                          desc);
        }

        @Override
        public int compareTo(ExtendedDescriptor o)
        {
            int c = descriptor.directory.toString().compareTo(o.descriptor.directory.toString());
            if (c != 0)
                return c;
            c = notNull(keyspace).compareTo(notNull(o.keyspace));
            if (c != 0)
                return c;
            c = notNull(table).compareTo(notNull(o.table));
            if (c != 0)
                return c;
            c = notNull(tableId).toString().compareTo(notNull(o.tableId).toString());
            if (c != 0)
                return c;
            c = notNull(index).compareTo(notNull(o.index));
            if (c != 0)
                return c;
            c = notNull(snapshot).compareTo(notNull(o.snapshot));
            if (c != 0)
                return c;
            c = notNull(backup).compareTo(notNull(o.backup));
            if (c != 0)
                return c;
            c = notNull(descriptor.id.toString()).compareTo(notNull(o.descriptor.id.toString()));
            if (c != 0)
                return c;
            return Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
        }
    }

    private static String notNull(String s)
    {
        return s != null ? s : "";
    }

    private static TableId notNull(TableId s)
    {
        return s != null ? s : EMPTY_TABLE_ID;
    }
}
