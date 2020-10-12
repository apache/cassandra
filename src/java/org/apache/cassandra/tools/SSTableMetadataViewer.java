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

import static org.apache.cassandra.tools.Util.BLUE;
import static org.apache.cassandra.tools.Util.CYAN;
import static org.apache.cassandra.tools.Util.RESET;
import static org.apache.cassandra.tools.Util.WHITE;
import static org.apache.commons.lang3.time.DurationFormatUtils.formatDurationWords;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tools.Util.TermHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.google.common.collect.MinMaxPriorityQueue;

/**
 * Shows the contents of sstable metadata
 */
public class SSTableMetadataViewer
{
    private static final Options options = new Options();
    private static CommandLine cmd;
    private static final String COLORS = "c";
    private static final String UNICODE = "u";
    private static final String GCGS_KEY = "g";
    private static final String TIMESTAMP_UNIT = "t";
    private static final String SCAN = "s";
    private static Comparator<ValuedByteBuffer> VCOMP = Comparator.comparingLong(ValuedByteBuffer::getValue).reversed();

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    boolean color;
    boolean unicode;
    int gc;
    PrintStream out;
    String[] files;
    TimeUnit tsUnit;

    public SSTableMetadataViewer()
    {
        this(true, true, 0, TimeUnit.MICROSECONDS, System.out);
    }

    public SSTableMetadataViewer(boolean color, boolean unicode, int gc, TimeUnit tsUnit, PrintStream out)
    {
        this.color = color;
        this.tsUnit = tsUnit;
        this.unicode = unicode;
        this.out = out;
        this.gc = gc;
    }

    public static String deletion(long time)
    {
        if (time == 0 || time == Integer.MAX_VALUE)
        {
            return "no tombstones";
        }
        return toDateString(time, TimeUnit.SECONDS);
    }

    public static String toDateString(long time, TimeUnit unit)
    {
        if (time == 0)
        {
            return null;
        }
        return new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(unit.toMillis(time)));
    }

    public static String toDurationString(long duration, TimeUnit unit)
    {
        if (duration == 0)
        {
            return null;
        }
        else if (duration == Integer.MAX_VALUE)
        {
            return "never";
        }
        return formatDurationWords(unit.toMillis(duration), true, true);
    }

    public static String toByteString(long bytes)
    {
        if (bytes == 0)
            return null;
        else if (bytes < 1024)
            return bytes + " B";

        int exp = (int) (Math.log(bytes) / Math.log(1024));
        char pre = "kMGTP".charAt(exp - 1);
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }

    public String scannedOverviewOutput(String key, long value)
    {
        StringBuilder sb = new StringBuilder();
        if (color) sb.append(CYAN);
        sb.append('[');
        if (color) sb.append(RESET);
        sb.append(key);
        if (color) sb.append(CYAN);
        sb.append("] ");
        if (color) sb.append(RESET);
        sb.append(value);
        return sb.toString();
    }

    private void printScannedOverview(Descriptor descriptor, StatsMetadata stats) throws IOException
    {
        TableMetadata cfm = Util.metadataFromSSTable(descriptor);
        SSTableReader reader = SSTableReader.openNoValidation(descriptor, TableMetadataRef.forOfflineTools(cfm));
        try (ISSTableScanner scanner = reader.getScanner())
        {
            long bytes = scanner.getLengthInBytes();
            MinMaxPriorityQueue<ValuedByteBuffer> widestPartitions = MinMaxPriorityQueue
                                                                     .orderedBy(VCOMP)
                                                                     .maximumSize(5)
                                                                     .create();
            MinMaxPriorityQueue<ValuedByteBuffer> largestPartitions = MinMaxPriorityQueue
                                                                      .orderedBy(VCOMP)
                                                                      .maximumSize(5)
                                                                      .create();
            MinMaxPriorityQueue<ValuedByteBuffer> mostTombstones = MinMaxPriorityQueue
                                                                   .orderedBy(VCOMP)
                                                                   .maximumSize(5)
                                                                   .create();
            long partitionCount = 0;
            long rowCount = 0;
            long tombstoneCount = 0;
            long cellCount = 0;
            double totalCells = stats.totalColumnsSet;
            int lastPercent = 0;
            long lastPercentTime = 0;
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {

                    long psize = 0;
                    long pcount = 0;
                    int ptombcount = 0;
                    partitionCount++;
                    if (!partition.staticRow().isEmpty())
                    {
                        rowCount++;
                        pcount++;
                        psize += partition.staticRow().dataSize();
                    }
                    if (!partition.partitionLevelDeletion().isLive())
                    {
                        tombstoneCount++;
                        ptombcount++;
                    }
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        switch (unfiltered.kind())
                        {
                            case ROW:
                                rowCount++;
                                Row row = (Row) unfiltered;
                                psize += row.dataSize();
                                pcount++;
                                for (org.apache.cassandra.db.rows.Cell<?> cell : row.cells())
                                {
                                    cellCount++;
                                    double percentComplete = Math.min(1.0, cellCount / totalCells);
                                    if (lastPercent != (int) (percentComplete * 100) &&
                                        (System.currentTimeMillis() - lastPercentTime) > 1000)
                                    {
                                        lastPercentTime = System.currentTimeMillis();
                                        lastPercent = (int) (percentComplete * 100);
                                        if (color)
                                            out.printf("\r%sAnalyzing SSTable...  %s%s %s(%%%s)", BLUE, CYAN,
                                                       Util.progress(percentComplete, 30, unicode),
                                                       RESET,
                                                       (int) (percentComplete * 100));
                                        else
                                            out.printf("\rAnalyzing SSTable...  %s (%%%s)",
                                                       Util.progress(percentComplete, 30, unicode),
                                                       (int) (percentComplete * 100));
                                        out.flush();
                                    }
                                    if (cell.isTombstone())
                                    {
                                        tombstoneCount++;
                                        ptombcount++;
                                    }
                                }
                                break;
                            case RANGE_TOMBSTONE_MARKER:
                                tombstoneCount++;
                                ptombcount++;
                                break;
                        }
                    }

                    widestPartitions.add(new ValuedByteBuffer(partition.partitionKey().getKey(), pcount));
                    largestPartitions.add(new ValuedByteBuffer(partition.partitionKey().getKey(), psize));
                    mostTombstones.add(new ValuedByteBuffer(partition.partitionKey().getKey(), ptombcount));
                }
            }

            out.printf("\r%80s\r", " ");
            field("Size", bytes);
            field("Partitions", partitionCount);
            field("Rows", rowCount);
            field("Tombstones", tombstoneCount);
            field("Cells", cellCount);
            field("Widest Partitions", "");
            Util.iterToStream(widestPartitions.iterator()).sorted(VCOMP).forEach(p ->
                                                                                 {
                                                                                     out.println("  " + scannedOverviewOutput(cfm.partitionKeyType.getString(p.buffer), p.value));
                                                                                 });
            field("Largest Partitions", "");
            Util.iterToStream(largestPartitions.iterator()).sorted(VCOMP).forEach(p ->
                                                                                  {
                                                                                      out.print("  ");
                                                                                      out.print(scannedOverviewOutput(cfm.partitionKeyType.getString(p.buffer), p.value));
                                                                                      if (color)
                                                                                          out.print(WHITE);
                                                                                      out.print(" (");
                                                                                      out.print(toByteString(p.value));
                                                                                      out.print(")");
                                                                                      if (color)
                                                                                          out.print(RESET);
                                                                                      out.println();
                                                                                  });
            StringBuilder tleaders = new StringBuilder();
            Util.iterToStream(mostTombstones.iterator()).sorted(VCOMP).forEach(p ->
                                                                               {
                                                                                   if (p.value > 0)
                                                                                   {
                                                                                       tleaders.append("  ");
                                                                                       tleaders.append(scannedOverviewOutput(cfm.partitionKeyType.getString(p.buffer), p.value));
                                                                                       tleaders.append(System.lineSeparator());
                                                                                   }
                                                                               });
            String tombstoneLeaders = tleaders.toString();
            if (tombstoneLeaders.length() > 10)
            {
                field("Tombstone Leaders", "");
                out.print(tombstoneLeaders);
            }
        }
        finally
        {
            reader.selfRef().ensureReleased();
        }
    }

    private void printSStableMetadata(String fname, boolean scan) throws IOException
    {
        Descriptor descriptor = Descriptor.fromFilename(fname);
        Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer()
                .deserialize(descriptor, EnumSet.allOf(MetadataType.class));
        ValidationMetadata validation = (ValidationMetadata) metadata.get(MetadataType.VALIDATION);
        StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
        CompactionMetadata compaction = (CompactionMetadata) metadata.get(MetadataType.COMPACTION);
        CompressionMetadata compression = null;
        File compressionFile = new File(descriptor.filenameFor(Component.COMPRESSION_INFO));
        if (compressionFile.exists())
            compression = CompressionMetadata.create(fname);
        SerializationHeader.Component header = (SerializationHeader.Component) metadata
                .get(MetadataType.HEADER);

        field("SSTable", descriptor);
        if (scan && descriptor.version.getVersion().compareTo("ma") >= 0)
        {
            printScannedOverview(descriptor, stats);
        }
        if (validation != null)
        {
            field("Partitioner", validation.partitioner);
            field("Bloom Filter FP chance", validation.bloomFilterFPChance);
        }
        if (stats != null)
        {
            field("Minimum timestamp", stats.minTimestamp, toDateString(stats.minTimestamp, tsUnit));
            field("Maximum timestamp", stats.maxTimestamp, toDateString(stats.maxTimestamp, tsUnit));
            field("SSTable min local deletion time", stats.minLocalDeletionTime, deletion(stats.minLocalDeletionTime));
            field("SSTable max local deletion time", stats.maxLocalDeletionTime, deletion(stats.maxLocalDeletionTime));
            field("Compressor", compression != null ? compression.compressor().getClass().getName() : "-");
            if (compression != null)
                field("Compression ratio", stats.compressionRatio);
            field("TTL min", stats.minTTL, toDurationString(stats.minTTL, TimeUnit.SECONDS));
            field("TTL max", stats.maxTTL, toDurationString(stats.maxTTL, TimeUnit.SECONDS));

            if (validation != null && header != null)
                printMinMaxToken(descriptor, FBUtilities.newPartitioner(descriptor), header.getKeyType());

            if (header != null && header.getClusteringTypes().size() == stats.minClusteringValues.size())
            {
                List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
                List<ByteBuffer> minClusteringValues = stats.minClusteringValues;
                List<ByteBuffer> maxClusteringValues = stats.maxClusteringValues;
                String[] minValues = new String[clusteringTypes.size()];
                String[] maxValues = new String[clusteringTypes.size()];
                for (int i = 0; i < clusteringTypes.size(); i++)
                {
                    minValues[i] = clusteringTypes.get(i).getString(minClusteringValues.get(i));
                    maxValues[i] = clusteringTypes.get(i).getString(maxClusteringValues.get(i));
                }
                field("minClusteringValues", Arrays.toString(minValues));
                field("maxClusteringValues", Arrays.toString(maxValues));
            }
            field("Estimated droppable tombstones",
                  stats.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000) - this.gc));
            field("SSTable Level", stats.sstableLevel);
            field("Repaired at", stats.repairedAt, toDateString(stats.repairedAt, TimeUnit.MILLISECONDS));
            field("Pending repair", stats.pendingRepair);
            field("Replay positions covered", stats.commitLogIntervals);
            field("totalColumnsSet", stats.totalColumnsSet);
            field("totalRows", stats.totalRows);
            field("Estimated tombstone drop times", "");

            TermHistogram estDropped = new TermHistogram(stats.estimatedTombstoneDropTime,
                                                         "Drop Time",
                                                         offset -> String.format("%d %s",
                                                                offset,
                                                                Util.wrapQuiet(toDateString(offset, TimeUnit.SECONDS),
                                                                                        color)),
                                                         String::valueOf);
            estDropped.printHistogram(out, color, unicode);
            field("Partition Size", "");
            TermHistogram rowSize = new TermHistogram(stats.estimatedPartitionSize,
                                                      "Size (bytes)",
                                                      offset -> String.format("%d %s",
                                                                              offset,
                                                                              Util.wrapQuiet(toByteString(offset), color)),
                                                      String::valueOf);
            rowSize.printHistogram(out, color, unicode);
            field("Column Count", "");
            TermHistogram cellCount = new TermHistogram(stats.estimatedCellPerPartitionCount,
                                                        "Columns",
                                                        String::valueOf,
                                                        String::valueOf);
            cellCount.printHistogram(out, color, unicode);
        }
        if (compaction != null)
        {
            field("Estimated cardinality", compaction.cardinalityEstimator.cardinality());
        }
        if (header != null)
        {
            EncodingStats encodingStats = header.getEncodingStats();
            AbstractType<?> keyType = header.getKeyType();
            List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
            Map<ByteBuffer, AbstractType<?>> staticColumns = header.getStaticColumns();
            Map<String, String> statics = staticColumns.entrySet().stream()
                    .collect(Collectors.toMap(e -> UTF8Type.instance.getString(e.getKey()),
                                              e -> e.getValue().toString()));
            Map<ByteBuffer, AbstractType<?>> regularColumns = header.getRegularColumns();
            Map<String, String> regulars = regularColumns.entrySet().stream()
                    .collect(Collectors.toMap(e -> UTF8Type.instance.getString(e.getKey()),
                                              e -> e.getValue().toString()));

            field("EncodingStats minTTL", encodingStats.minTTL,
                    toDurationString(encodingStats.minTTL, TimeUnit.SECONDS));
            field("EncodingStats minLocalDeletionTime", encodingStats.minLocalDeletionTime,
                    toDateString(encodingStats.minLocalDeletionTime, TimeUnit.SECONDS));
            field("EncodingStats minTimestamp", encodingStats.minTimestamp,
                    toDateString(encodingStats.minTimestamp, tsUnit));
            field("KeyType", keyType.toString());
            field("ClusteringTypes", clusteringTypes.toString());
            field("StaticColumns", FBUtilities.toString(statics));
            field("RegularColumns", FBUtilities.toString(regulars));
            field("IsTransient", stats.isTransient);
        }
    }

    private void field(String field, Object value)
    {
        field(field, value, null);
    }

    private void field(String field, Object value, String comment)
    {
        StringBuilder sb = new StringBuilder();
        if (color) sb.append(BLUE);
        sb.append(field);
        if (color) sb.append(CYAN);
        sb.append(": ");
        if (color) sb.append(RESET);
        sb.append(value == null? "--" : value.toString());

        if (comment != null)
        {
            if (color) sb.append(WHITE);
            sb.append(" (");
            sb.append(comment);
            sb.append(")");
            if (color) sb.append(RESET);
        }
        this.out.println(sb.toString());
    }

    private static void printUsage()
    {
        try (PrintWriter errWriter = new PrintWriter(System.err, true))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(errWriter, 120, "sstablemetadata <options> <sstable...>",
                                String.format("%nDump information about SSTable[s] for Apache Cassandra 3.x%nOptions:"),
                                options, 2, 1, "", true);
            errWriter.println();
        }
    }

    private void printMinMaxToken(Descriptor descriptor, IPartitioner partitioner, AbstractType<?> keyType)
            throws IOException
    {
        File summariesFile = new File(descriptor.filenameFor(Component.SUMMARY));
        if (!summariesFile.exists())
            return;

        try (DataInputStream iStream = new DataInputStream(Files.newInputStream(summariesFile.toPath())))
        {
            Pair<DecoratedKey, DecoratedKey> firstLast = new IndexSummary.IndexSummarySerializer()
                    .deserializeFirstLastKey(iStream, partitioner);
            field("First token", firstLast.left.getToken(), keyType.getString(firstLast.left.getKey()));
            field("Last token", firstLast.right.getToken(), keyType.getString(firstLast.right.getKey()));
        }
    }

    /**
     * @param args
     *            a list of sstables whose metadata we're interested in
     */
    public static void main(String[] args) throws IOException
    {
        CommandLineParser parser = new PosixParser();

        Option disableColors = new Option(COLORS, "colors", false, "Use ANSI color sequences");
        disableColors.setOptionalArg(true);
        options.addOption(disableColors);
        Option unicode = new Option(UNICODE, "unicode", false, "Use unicode to draw histograms and progress bars");
        unicode.setOptionalArg(true);

        options.addOption(unicode);
        Option gcgs = new Option(GCGS_KEY, "gc_grace_seconds", true, "Time to use when calculating droppable tombstones");
        gcgs.setOptionalArg(true);
        options.addOption(gcgs);
        Option tsUnit = new Option(TIMESTAMP_UNIT, "timestamp_unit", true, "Time unit that cell timestamps are written with");
        tsUnit.setOptionalArg(true);
        options.addOption(tsUnit);

        Option scanEnabled = new Option(SCAN, "scan", false,
                "Full sstable scan for additional details. Only available in 3.0+ sstables. Defaults: false");
        scanEnabled.setOptionalArg(true);
        options.addOption(scanEnabled);
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            printUsage();
            System.exit(1);
        }

        if (cmd.getArgs().length < 1)
        {
            System.err.println("You must supply at least one sstable");
            printUsage();
            System.exit(1);
        }
        boolean enabledColors = cmd.hasOption(COLORS);
        boolean enabledUnicode = cmd.hasOption(UNICODE);
        boolean fullScan = cmd.hasOption(SCAN);
        int gc = Integer.parseInt(cmd.getOptionValue(GCGS_KEY, "0"));
        TimeUnit ts = TimeUnit.valueOf(cmd.getOptionValue(TIMESTAMP_UNIT, "MICROSECONDS"));
        SSTableMetadataViewer metawriter = new SSTableMetadataViewer(enabledColors, enabledUnicode, gc, ts, System.out);
        for (String fname : cmd.getArgs())
        {
            File sstable = new File(fname);
            if (sstable.exists())
            {
                metawriter.printSStableMetadata(sstable.getAbsolutePath(), fullScan);
            }
            else
            {
                System.out.println("No such file: " + fname);
            }
        }
    }

    private static class ValuedByteBuffer
    {
        public long value;
        public ByteBuffer buffer;

        public ValuedByteBuffer(ByteBuffer buffer, long value)
        {
            this.value = value;
            this.buffer = buffer;
        }

        public long getValue()
        {
            return value;
        }
    }
}
