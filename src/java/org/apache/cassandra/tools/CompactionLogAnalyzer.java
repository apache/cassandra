/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.io.ByteStreams;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


/**
 * Analyzes a collection of CSV logs from the unified compaction strategy. Run with
 *
 *   tools/bin/analyzecompactionlog <path-to-directory-with-csvs>
 *
 * It will process the CSVs are create a compaction_report.html file in the target directory. The file is similar to our
 * performance reports.
 */
public class CompactionLogAnalyzer
{

    private static final Options options = new Options();
    private static CommandLine cmd;

    public static final String OPTION_LIMIT = "l";
    public static final String OPTION_RESOLUTION = "r";

    static
    {
        DatabaseDescriptor.toolInitialization();

        Option optLimit = new Option(OPTION_LIMIT, true, "If specified, will only read this number of events " +
                                                         "from the first file, and up to that time from the others.");
        optLimit.setArgs(1);
        options.addOption(optLimit);

        Option optResolution = new Option(OPTION_RESOLUTION, true, "The resolution of the produced" +
                                                                   "report in milliseconds, 100 by default.");
        optResolution.setArgs(1);
        options.addOption(optResolution);
    }

    /**
     * A data point represents both an input data point as well as aggregated data for a level or total.
     */
    static class DataPoint
    {
        String shardId;
        long timestamp;
        int bucket;
        // number of sstables
        int sstables;
        // max number of overlapping sstables in bucket
        int overlap;
        // total size of the sstables
        long size;
        // number of running compactions
        int compactionsInProgress;
        // number of compactions to do
        int compactionsPending;
        // bytes read per second
        long readBytesPerSecond;
        // bytes written per second
        long writeBytesPerSecond;
        // total bytes to compact
        long totalBytes;
        // remaining bytes to compact
        long remainingReadBytes;
        // value of scaling parameter W
        int scalingParameter;

        /**
         * Called to aggregate data in response to a new data point for a bucket.
         * Unless the process is just starting, the new data point will be replacing the older state of the bucket,
         * thus this will add the new data but also remove the older values.
         */
        private void updateTotals(DataPoint toAdd, DataPoint toRemove)
        {
            timestamp = toAdd.timestamp;
            compactionsInProgress += toAdd.compactionsInProgress - toRemove.compactionsInProgress;
            compactionsPending += toAdd.compactionsPending - toRemove.compactionsPending;
            sstables += toAdd.sstables - toRemove.sstables;
            size += toAdd.size - toRemove.size;
            readBytesPerSecond += toAdd.readBytesPerSecond - toRemove.readBytesPerSecond;
            writeBytesPerSecond += toAdd.writeBytesPerSecond - toRemove.writeBytesPerSecond;
            totalBytes += toAdd.totalBytes - toRemove.totalBytes;
            remainingReadBytes += toAdd.remainingReadBytes - toRemove.remainingReadBytes;
            scalingParameter = toAdd.scalingParameter;
            overlap = toAdd.overlap;
        }
    }


    static final Pattern CSVNamePattern = Pattern.compile("compaction-(\\w+)-([^-]*)-([^-]*)(-([^.]*))?\\.csv");
    private static final String fullDateFormatter = "yyyy-MM-dd' 'HH:mm:ss.SSS";

    static int reportResolutionInMs;

    // Indexes of the relevant columns in the source CSV, set by initializeIndexes below.
    static int timestampIndex = -1;
    static int eventIndex;
    static int bucketIndex;
    static int sstablesIndex;
    static int overlapIndex;
    static int compactingSstablesIndex;
    static int sizeIndex;
    static int compactionsIndex;
    static int readPerSecIndex;
    static int writePerSecIndex;
    static int sizesIndex;
    static int Windex;

    private static void initializeIndexes(String header)
    {
        if (timestampIndex < 0)
            synchronized (CompactionLogAnalyzer.class) {
                if (timestampIndex < 0)
                {
                    Map<String, Integer> indexMap = new HashMap<>();
                    String[] headers = header.split(",");
                    for (int i = 0; i < headers.length; ++i)
                        indexMap.put(headers[i], i);

                    timestampIndex = indexMap.get("Timestamp");
                    eventIndex = indexMap.get("Event");
                    bucketIndex = indexMap.getOrDefault("Level", indexMap.get("Bucket"));
                    sstablesIndex = indexMap.get("Tot. SSTables");
                    overlapIndex = indexMap.get("Overlap");
                    compactingSstablesIndex = indexMap.get("Comp. SSTables");
                    sizeIndex = indexMap.getOrDefault("Size (bytes)", -1);
                    sizeIndex = indexMap.get("Tot. size (bytes)");
                    compactionsIndex = indexMap.get("Compactions");
                    readPerSecIndex = indexMap.get("Read (bytes/sec)");
                    writePerSecIndex = indexMap.get("Write (bytes/sec)");
                    sizesIndex = indexMap.getOrDefault("Tot/Read/Written", -1);
                    sizesIndex = indexMap.get("Tot. comp. size/Read/Written (bytes)");
                    Windex = indexMap.get("W");
                }
            }
    }

    static DataPoint parse(String shardId, String dataLine) throws ParseException
    {
        String[] data = dataLine.split(",");

        DataPoint dp = new DataPoint();
        dp.shardId = shardId;
        dp.timestamp = getTimestamp(data[timestampIndex]);
        dp.bucket = Integer.parseInt(data[bucketIndex]);
        dp.sstables = Integer.parseInt(data[sstablesIndex]);
        dp.size = parseHumanReadableSize(data[sizeIndex]);
        final String[] compactions = data[compactionsIndex].split("/");
        dp.compactionsInProgress = Integer.parseInt(compactions[1]);
        dp.compactionsPending = Integer.parseInt(compactions[0]);
        dp.readBytesPerSecond = parseHumanReadableRate(data[readPerSecIndex]);
        dp.writeBytesPerSecond = parseHumanReadableRate(data[writePerSecIndex]);
        String[] sizes = data[sizesIndex].split("/");
        dp.totalBytes = parseHumanReadableSize(sizes[0]);
        dp.remainingReadBytes = dp.totalBytes - parseHumanReadableSize(sizes[1]);
        dp.scalingParameter = UnifiedCompactionStrategy.parseScalingParameter(data[Windex]);
        if (overlapIndex >= 0)
        {
            dp.overlap = Integer.parseInt(data[overlapIndex]);
            // Note: This overlap does not include the sstables that are currently compacting. Having such a measure
            // could be valuable, but it needs processing that the strategy does not do (to improve efficiency the
            // overlap sets construction only uses non-compacting sstables).
        }
        else
        {
            // The number of non-compacting sstables in a bucket is the proxy the strategy used for overlapping sstables.
            int compactingSSTables = Integer.parseInt(data[compactingSstablesIndex].split("/")[1]);
            dp.overlap = dp.sstables - compactingSSTables;
        }
        return dp;
    }

    private static long getTimestamp(String datum) throws ParseException
    {
        Date date = new SimpleDateFormat(fullDateFormatter).parse(datum);
        return date.getTime();
    }

    private static long parseHumanReadableSize(String datum)
    {
        return FBUtilities.parseHumanReadableBytes(datum);
    }

    private static long parseHumanReadableRate(String datum)
    {
        return (long) FBUtilities.parseHumanReadable(datum, null, "B/s");
    }

    public static void generateGraph(File htmlFile, JSONObject stats)
    {
        try (PrintWriter out = new PrintWriter(htmlFile))
        {
            String statsBlock = "/* stats start */\nstats = " + stats.toJSONString() + ";\n/* stats end */\n";
            String html = getGraphHTML().replaceFirst("/\\* stats start \\*/\n\n/\\* stats end \\*/\n", statsBlock);
            out.write(html);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Couldn't write stats html.");
        }
    }

    private static String getGraphHTML()
    {
        try (InputStream graphHTMLRes = CompactionLogAnalyzer.class.getClassLoader().getResourceAsStream("org/apache/cassandra/graph/graph.html"))
        {
            return new String(ByteStreams.toByteArray(graphHTMLRes));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception
    {
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (org.apache.commons.cli.ParseException e1)
        {
            System.err.println(e1.getMessage());
            printUsage();
            System.exit(1);
        }

        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one log csv path.");
            printUsage();
            System.exit(1);
        }

        File logPath = new File(cmd.getArgs()[0]);
        File[] files = logPath.listFiles(f -> CSVNamePattern.matcher(f.getName()).matches());
        Arrays.sort(files);

        reportResolutionInMs = Integer.parseInt(cmd.getOptionValue(OPTION_RESOLUTION, "100"));

        final String limitOption = cmd.getOptionValue(OPTION_LIMIT);
        Integer lineCountLimit = limitOption == null ? null : Integer.parseInt(limitOption);

        List<DataPoint> dataPoints = readDataPoints(files, lineCountLimit);
        dataPoints.sort((a, b) -> Long.compare(a.timestamp, b.timestamp));

        JSONArray marr = processData(dataPoints);
        JSONObject main = new JSONObject();
        main.put("title", "Compaction report");
        main.put("stats", marr);

        generateGraph(new File(logPath.getPath() + File.separator + "compaction_report.html"), main);

        System.exit(0);
    }

    @VisibleForTesting
    static List<DataPoint> readDataPoints(File[] files, @Nullable Integer lineCountLimit) throws IOException, ParseException
    {
        List<DataPoint> dataPoints;

        if (lineCountLimit != null)
        {
            long timestampLimit = Long.MAX_VALUE;
            dataPoints = new ArrayList<>();

            for (File file : files)
                timestampLimit = readDataPoints(dataPoints, lineCountLimit, timestampLimit, file);
        }
        else
        {
            // Reading the files can take a long time. Do it in parallel.
            dataPoints = Arrays.stream(files)
                               .parallel()
                               .flatMap(file ->
                                    {
                                        List<DataPoint> pts = new ArrayList<>();
                                        try
                                        {
                                            readDataPoints(pts, Integer.MAX_VALUE, Long.MAX_VALUE, file);
                                            return pts.stream();
                                        }
                                        catch (Exception e)
                                        {
                                            throw Throwables.propagate(e);
                                        }
                                    })
                               .collect(Collectors.toList());
        }

        return dataPoints;
    }

    private static long readDataPoints(List<DataPoint> dataPoints, int lineCountLimit, long timestampLimit, File file) throws IOException, ParseException
    {
        Matcher m = CSVNamePattern.matcher(file.getName());
        if (!m.matches())
            throw new AssertionError();

        String shardId = m.group(5);
        if (shardId == null)
            shardId = "none";
        try (BufferedReader rdr = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8))
        {
            String header = rdr.readLine();
            initializeIndexes(header);
            DataPoint curr = null;

            int lineCount = 0;

            while (rdr.ready())
            {
                if (++lineCount > lineCountLimit && curr != null)
                {
                    timestampLimit = curr.timestamp;
                    break;
                }

                String line = rdr.readLine();
                if (line.isEmpty())
                    continue;

                try
                {
                    curr = parse(shardId, line);
                }
                catch (NumberFormatException | ParseException | ArrayIndexOutOfBoundsException e)
                {
                    System.out.format("%s parsing line %s, skipping.\n", e.getMessage(), line);
                    continue;
                }
                if (curr.timestamp > timestampLimit)
                    break;
                dataPoints.add(curr);
            }
            System.out.format("%d data points processed for shard %s.\n", lineCount, shardId);
        }
        return timestampLimit;
    }

    @VisibleForTesting
    static JSONArray processData(List<DataPoint> dataPoints)
    {
        int levels = dataPoints.stream().mapToInt(dp -> dp.bucket).max().getAsInt() + 1;

        // Prepare the JSON objects representing the data in the report
        JSONArray marr = new JSONArray();

        JSONArray[] intervalsPerLevel = new JSONArray[levels + 1];
        Table<String, Integer, DataPoint> progressMap = HashBasedTable.create();
        DataPoint totals = new DataPoint();
        DataPoint[] perLevel = new DataPoint[levels + 1];
        perLevel[levels] = totals;
        DataPoint zero = new DataPoint();
        totals.shardId = "Total";
        totals.bucket = levels;

        JSONArray metricsHeader = makeMetricsHeader();
        for (int i = 0; i < levels; ++i)
        {
            perLevel[i] = new DataPoint();
            perLevel[i].shardId = "Level " + i;
            perLevel[i].bucket = i;
        }


        for (int i = 0; i <= levels; ++i)
        {
            intervalsPerLevel[i] = new JSONArray();

            JSONObject stats = new JSONObject();
            stats.put("revision", perLevel[i].shardId);
            stats.put("test", "Compaction");
            stats.put("metrics", metricsHeader);
            stats.put("intervals", intervalsPerLevel[i]);
            marr.add(stats);
        }

        System.out.println("Totals");
        System.out.format("%25s %8s %9s %15s %15s %15s %15s\n", "Timestamp", "SSTables", "Run/Pendg", "Read tput", "Write tput", "TotalCompBytes", "RemCompBytes");

        // Process the data points to compile aggregate state and report it with the specified resolution.
        long startTimestamp = -1;
        int count = 0;
        for (DataPoint dp : dataPoints)
        {
            // Data points replace previous data for the given bucket. This map is used to find what is replaced.
            DataPoint prev = progressMap.get(dp.shardId, dp.bucket);
            if (prev == null)
                prev = zero;

            if (startTimestamp == -1)
                startTimestamp = dp.timestamp;
            else if (dp.timestamp >= totals.timestamp + reportResolutionInMs)
            {
                report(intervalsPerLevel, progressMap, perLevel, startTimestamp);
                ++count;
            }

            totals.updateTotals(dp, prev);
            perLevel[dp.bucket].updateTotals(dp, prev);
            progressMap.put(dp.shardId, dp.bucket, dp);
        }
        report(intervalsPerLevel, progressMap, perLevel, startTimestamp);
        ++count;

        System.out.format("Wrote %d datapoints, spanning %.1f seconds\n", count, (totals.timestamp - startTimestamp) / 1000.0);
        return marr;
    }

    private static void report(JSONArray[] intervalsPerLevel,
                               Table<String, Integer, DataPoint> progressMap,
                               DataPoint[] perLevel,
                               long startTimestamp)
    {
        // Collect a histogram of the number of sstables per bucket.
        int levels = perLevel.length - 1;

        int maxOverlap = -1;
        for (DataPoint bucket : progressMap.values())
        {
            maxOverlap = Math.max(maxOverlap, bucket.overlap);
        }
        perLevel[levels].overlap = maxOverlap;

        print(perLevel[levels]);    // print out the totals on the console
        for (int i = 0; i <= levels; ++i)
            addMetrics(perLevel[i], intervalsPerLevel[i], startTimestamp);
    }

    private static JSONArray makeMetricsHeader()
    {
        JSONArray metrics = new JSONArray();
        metrics.add("SSTables");
        metrics.add("Size MB");
        metrics.add("Running compactions");
        metrics.add("Pending compactions");
        metrics.add("Read throughput MB/s");
        metrics.add("Write throughput MB/s");
        metrics.add("Read throughput per thread MB/s");
        metrics.add("Write throughput per thread MB/s");
        metrics.add("Total GB to compact");
        metrics.add("Remaining GB to compact");
        metrics.add("Max overlapping SSTables");
        metrics.add("Scaling parameter W");

        metrics.add("time");
        return metrics;
    }

    private static void addMetrics(DataPoint totals, JSONArray intervals, long startTimestamp)
    {
        if (totals.timestamp < startTimestamp)
            return; // nothing to add yet

        JSONArray metrics = new JSONArray();
        metrics.add(totals.sstables);
        metrics.add(Math.scalb(totals.size, -20));
        metrics.add(totals.compactionsInProgress);
        metrics.add(totals.compactionsPending);
        metrics.add(Math.scalb(totals.readBytesPerSecond, -20));
        metrics.add(Math.scalb(totals.writeBytesPerSecond, -20));
        if (totals.compactionsInProgress > 0)
        {
            long readThroughput = totals.readBytesPerSecond / totals.compactionsInProgress;
            long writeThroughput = totals.writeBytesPerSecond / totals.compactionsInProgress;
            metrics.add(Math.scalb(readThroughput, -20));
            metrics.add(Math.scalb(writeThroughput, -20));
        }
        else
        {
            metrics.add(null);
            metrics.add(null);
        }
        metrics.add(Math.scalb(totals.totalBytes, -30));
        metrics.add(Math.scalb(totals.remainingReadBytes, -30));

        metrics.add(totals.overlap);

        metrics.add(totals.scalingParameter);

        metrics.add((totals.timestamp - startTimestamp) / 1000.0);
        intervals.add(metrics);
    }

    static void print(DataPoint dp)
    {
        System.out.format("%25s %8s %3d/%5d %13s/s %13s/s %15s %15s\n",
                          new SimpleDateFormat(fullDateFormatter).format(new Date(dp.timestamp)),
                          dp.sstables,
                          dp.compactionsInProgress,
                          dp.compactionsPending,
                          FBUtilities.prettyPrintMemory(dp.readBytesPerSecond),
                          FBUtilities.prettyPrintMemory(dp.writeBytesPerSecond),
                          FBUtilities.prettyPrintMemory(dp.totalBytes),
                          FBUtilities.prettyPrintMemory(dp.remainingReadBytes));
    }

    private static void printUsage()
    {
        String usage = String.format("analyzecompactionlog <options> <log csvs path>%n");
        String header = "Perform an analysis of the UCS compaction log.\n\n" +
                        "The input is a directory that contains the per-shard CSV files generated using the " +
                        "'logAll: true' flag by the unified compaction strategy.\n" +
                        "Constructs a compaction_report.html in the target directory with summarized metrics.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}
