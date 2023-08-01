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
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.LongFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

import static java.lang.String.format;

@SuppressWarnings("serial")
public final class Util
{
    static final String RESET = "\u001B[0m";
    static final String BLUE = "\u001B[34m";
    static final String CYAN = "\u001B[36m";
    static final String WHITE = "\u001B[37m";
    private static final List<String> ANSI_COLORS = Lists.newArrayList(RESET, BLUE, CYAN, WHITE);

    private static final String FULL_BAR_UNICODE = Strings.repeat("\u2593", 30);
    private static final String EMPTY_BAR_UNICODE = Strings.repeat("\u2591", 30);
    private static final String FULL_BAR_ASCII = Strings.repeat("#", 30);
    private static final String EMPTY_BAR_ASCII = Strings.repeat("-", 30);

    private static final TreeMap<Double, String> BARS_UNICODE = new TreeMap<Double, String>()
    {{
        this.put(1.0,       "\u2589"); // full, actually using 7/8th for bad font impls of fullblock
        this.put(7.0 / 8.0, "\u2589"); // 7/8ths left block
        this.put(3.0 / 4.0, "\u258A"); // 3/4th block
        this.put(5.0 / 8.0, "\u258B"); // 5/8th
        this.put(3.0 / 8.0, "\u258D"); // three eighths, skips 1/2 due to font inconsistencies
        this.put(1.0 / 4.0, "\u258E"); // 1/4th
        this.put(1.0 / 8.0, "\u258F"); // 1/8th
    }};

    private static final TreeMap<Double, String> BARS_ASCII = new TreeMap<Double, String>()
    {{
        this.put(1.00, "O");
        this.put(0.75, "o");
        this.put(0.30, ".");
    }};

    private static TreeMap<Double, String> barmap(boolean unicode)
    {
        return unicode ? BARS_UNICODE : BARS_ASCII;
    }

    public static String progress(double percentComplete, int width, boolean unicode)
    {
        assert percentComplete >= 0 && percentComplete <= 1;
        int cols = (int) (percentComplete * width);
        return (unicode ? FULL_BAR_UNICODE : FULL_BAR_ASCII).substring(width - cols) +
               (unicode ? EMPTY_BAR_UNICODE : EMPTY_BAR_ASCII ).substring(cols);
    }

    public static String stripANSI(String string)
    {
        return ANSI_COLORS.stream().reduce(string, (a, b) -> a.replace(b, ""));
    }

    public static int countANSI(String string)
    {
        return string.length() - stripANSI(string).length();
    }

    public static String wrapQuiet(String toWrap, boolean color)
    {
        if (Strings.isNullOrEmpty(toWrap))
        {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        if (color) sb.append(WHITE);
        sb.append("(");
        sb.append(toWrap);
        sb.append(")");
        if (color) sb.append(RESET);
        return sb.toString();
    }

    public static class TermHistogram
    {
        public long max;
        public long min;
        public double sum;
        int maxCountLength = 5;
        int maxOffsetLength = 5;
        Map<? extends Number, Long> histogram;
        LongFunction<String> offsetName;
        LongFunction<String> countName;
        String title;

        public TermHistogram(Map<? extends Number, Long> histogram,
                String title,
                LongFunction<String> offsetName,
                LongFunction<String> countName)
        {
            this.offsetName = offsetName;
            this.countName = countName;
            this.histogram = histogram;
            this.title = title;
            maxOffsetLength = title.length();
            histogram.entrySet().stream().forEach(e ->
            {
                max = Math.max(max, e.getValue());
                min = Math.min(min, e.getValue());
                sum += e.getValue();
                // find max width, but remove ansi sequences first
                maxCountLength = Math.max(maxCountLength, stripANSI(countName.apply(e.getValue())).length());
                maxOffsetLength = Math.max(maxOffsetLength, stripANSI(offsetName.apply(e.getKey().longValue())).length());
            });
        }

        public TermHistogram(TombstoneHistogram histogram,
                String title,
                LongFunction<String> offsetName,
                LongFunction<String> countName)
        {
            this(new TreeMap<Number, Long>()
            {
                {
                    histogram.forEach((point, value) -> {
                        this.put(point, (long) value);
                    });
                }
            }, title, offsetName, countName);
        }

        public TermHistogram(EstimatedHistogram histogram,
                String title,
                LongFunction<String> offsetName,
                LongFunction<String> countName)
        {
            this(new TreeMap<Number, Long>()
            {
                {
                    long[] counts = histogram.getBuckets(false);
                    long[] offsets = histogram.getBucketOffsets();
                    for (int i = 0; i < counts.length; i++)
                    {
                        long e = counts[i];
                        if (e > 0)
                        {
                            put(offsets[i], e);
                        }
                    }
                }
            }, title, offsetName, countName);
        }

        public String bar(long count, int length, String color, boolean unicode)
        {
            if (color == null) color = "";
            StringBuilder sb = new StringBuilder(color);
            long barVal = count;
            int intWidth = (int) (barVal * 1.0 / max * length);
            double remainderWidth = (barVal * 1.0 / max * length) - intWidth;
            sb.append(Strings.repeat(barmap(unicode).get(1.0), intWidth));

            if (barmap(unicode).floorKey(remainderWidth) != null)
                sb.append(barmap(unicode).get(barmap(unicode).floorKey(remainderWidth)));

            if(!Strings.isNullOrEmpty(color))
                sb.append(RESET);

            return sb.toString();
        }

        public void printHistogram(PrintStream out, boolean color, boolean unicode)
        {
            // String.format includes ansi sequences in the count, so need to modify the lengths
            int offsetTitleLength = color ? maxOffsetLength + BLUE.length() : maxOffsetLength;
            out.printf("   %-" + offsetTitleLength + "s %s %-" + maxCountLength + "s  %s  %sHistogram%s %n",
                       color ? BLUE + title : title,
                       color ? CYAN + "|" + BLUE : "|",
                       "Count",
                       wrapQuiet("%", color),
                       color ? BLUE : "",
                       color ? RESET : "");
            histogram.entrySet().stream().forEach(e ->
            {
                String offset = offsetName.apply(e.getKey().longValue());
                long count = e.getValue();
                String histo = bar(count, 30, color? WHITE : null, unicode);
                int mol = color ? maxOffsetLength + countANSI(offset) : maxOffsetLength;
                int mcl = color ? maxCountLength + countANSI(countName.apply(count)) : maxCountLength;
                out.printf("   %-" + mol + "s %s %" + mcl + "s %s %s%n",
                           offset,
                           color ? CYAN + "|" + RESET : "|",
                           countName.apply(count),
                           wrapQuiet(String.format("%3s", (int) (100 * ((double) count / sum))), color),
                           histo);
            });
            EstimatedHistogram eh = new EstimatedHistogram(165);
            for (Entry<? extends Number, Long> e : histogram.entrySet())
            {
                eh.add(e.getKey().longValue(), e.getValue());
            }
            String[] percentiles = new String[]{"50th", "75th", "95th", "98th", "99th", "Min", "Max"};
            long[] data = new long[]
            {
                eh.percentile(.5),
                eh.percentile(.75),
                eh.percentile(.95),
                eh.percentile(.98),
                eh.percentile(.99),
                eh.min(),
                eh.max(),
            };
            out.println((color ? BLUE : "") + "   Percentiles" + (color ? RESET : ""));

            for (int i = 0; i < percentiles.length; i++)
            {
                out.println(format("   %s%-10s%s%s",
                                   (color ? BLUE : ""),
                                   percentiles[i],
                                   (color ? RESET : ""),
                                   offsetName.apply(data[i])));
            }
        }
    }
    private Util()
    {
    }

    /**
     * This is used by standalone tools to force static initialization of DatabaseDescriptor, and fail if configuration
     * is bad.
     */
    public static void initDatabaseDescriptor()
    {
        try
        {
            DatabaseDescriptor.toolInitialization();
        }
        catch (Throwable e)
        {
            boolean logStackTrace = !(e instanceof ConfigurationException) || ((ConfigurationException) e).logStackTrace;
            System.out.println("Exception (" + e.getClass().getName() + ") encountered during startup: " + e.getMessage());

            if (logStackTrace)
            {
                e.printStackTrace();
                System.exit(3);
            }
            else
            {
                System.err.println(e.getMessage());
                System.exit(3);
            }
        }
    }

    public static <T> Stream<T> iterToStream(Iterator<T> iter)
    {
        Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }

    /**
     * Construct table schema from info stored in SSTable's Stats.db
     *
     * @param desc SSTable's descriptor
     * @return Restored CFMetaData
     * @throws IOException when Stats.db cannot be read
     */
    public static TableMetadata metadataFromSSTable(Descriptor desc) throws IOException
    {
        if (!desc.version.isCompatible())
            throw new IOException("Unsupported SSTable version " + desc.getFormat().name() + "/" + desc.version);

        StatsComponent statsComponent = StatsComponent.load(desc, MetadataType.STATS, MetadataType.HEADER);
        SerializationHeader.Component header = statsComponent.serializationHeader();

        IPartitioner partitioner = FBUtilities.newPartitioner(desc);

        TableMetadata.Builder builder = TableMetadata.builder("keyspace", "table").partitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addStaticColumn(ident, entry.getValue());
                });
        header.getRegularColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addRegularColumn(ident, entry.getValue());
                });
        builder.addPartitionKeyColumn("PartitionKey", header.getKeyType());
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
        }
        if (SecondaryIndexManager.isIndexColumnFamily(desc.cfname))
        {
            String index = SecondaryIndexManager.getIndexName(desc.cfname);
            // Just set the Kind of index to CUSTOM, which is an irrelevant parameter that doesn't make any effect on the result
            IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata(index, IndexMetadata.Kind.CUSTOM, null);
            Indexes indexes = Indexes.of(indexMetadata);
            builder.indexes(indexes);
            builder.kind(TableMetadata.Kind.INDEX);
        }
        return builder.build();
    }
}