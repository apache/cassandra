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

package org.apache.cassandra.repair.consistent;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.repair.RepairSessionResult;
import org.apache.cassandra.repair.SyncStat;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;

public class SyncStatSummary
{

    private static class Session
    {
        final InetSocketAddress src;
        final InetSocketAddress dst;

        int files = 0;
        long bytes = 0;
        Set<Range<Token>> ranges = new HashSet<>();

        Session(InetSocketAddress src, InetSocketAddress dst)
        {
            this.src = src;
            this.dst = dst;
        }

        void consumeSummary(StreamSummary summary)
        {
            files += summary.files;
            bytes += summary.totalSize;
        }

        void consumeSummaries(Collection<StreamSummary> summaries, Collection<Range<Token>> ranges)
        {
            summaries.forEach(this::consumeSummary);
            this.ranges.addAll(ranges);
        }

        public String toString()
        {
            return String.format("%s -> %s: %s ranges, %s sstables, %s bytes", src, dst, ranges.size(), files, FBUtilities.prettyPrintMemory(bytes));
        }
    }

    private static class Table
    {
        final String keyspace;

        final String table;

        int files = -1;
        long bytes = -1;
        Collection<Range<Token>> ranges = new HashSet<>();
        boolean totalsCalculated = false;

        final Map<Pair<InetSocketAddress, InetSocketAddress>, Session> sessions = new HashMap<>();

        Table(String keyspace, String table)
        {
            this.keyspace = keyspace;
            this.table = table;
        }

        Session getOrCreate(InetSocketAddress from, InetSocketAddress to)
        {
            Pair<InetSocketAddress, InetSocketAddress> k = Pair.create(from, to);
            if (!sessions.containsKey(k))
            {
                sessions.put(k, new Session(from, to));
            }
            return sessions.get(k);
        }

        void consumeStat(SyncStat stat)
        {
            for (SessionSummary summary: stat.summaries)
            {
                getOrCreate(summary.coordinator, summary.peer).consumeSummaries(summary.sendingSummaries, stat.differences);
                getOrCreate(summary.peer, summary.coordinator).consumeSummaries(summary.receivingSummaries, stat.differences);
            }
        }

        void consumeStats(List<SyncStat> stats)
        {
            filter(stats, s -> s.summaries != null).forEach(this::consumeStat);
        }

        void calculateTotals()
        {
            files = 0;
            bytes = 0;
            ranges = new HashSet<>();
            for (Session session: sessions.values())
            {
                files += session.files;
                bytes += session.bytes;
                ranges.addAll(session.ranges);
            }
            totalsCalculated = true;
        }

        boolean isCounter()
        {
            TableMetadata tmd = Schema.instance.getTableMetadata(keyspace, table);
            return tmd != null && tmd.isCounter();
        }

        public String toString()
        {
            if (!totalsCalculated)
            {
                calculateTotals();
            }
            StringBuilder output = new StringBuilder();

            output.append(String.format("%s.%s - %s ranges, %s sstables, %s bytes\n", keyspace, table, ranges.size(), files, FBUtilities.prettyPrintMemory(bytes)));
            if (ranges.size() > 0)
            {
                output.append("    Mismatching ranges: ");
                int i = 0;
                Iterator<Range<Token>> rangeIterator = ranges.iterator();
                while (rangeIterator.hasNext() && i < 30)
                {
                    Range<Token> r = rangeIterator.next();
                    output.append('(').append(r.left).append(',').append(r.right).append("],");
                    i++;
                }
                if (i == 30)
                    output.append("...");
                output.append(System.lineSeparator());
            }
            for (Session session: sessions.values())
            {
                output.append("    ").append(session.toString()).append(System.lineSeparator());
            }
            return output.toString();
        }
    }

    private final Map<Pair<String, String>, Table> summaries = new HashMap<>();
    private final boolean isEstimate;

    private int files = -1;
    private long bytes = -1;
    private Set<Range<Token>> ranges = new HashSet<>();
    private boolean totalsCalculated = false;

    public SyncStatSummary(boolean isEstimate)
    {
        this.isEstimate = isEstimate;
    }

    public void consumeRepairResult(RepairResult result)
    {
        Pair<String, String> cf = Pair.create(result.desc.keyspace, result.desc.columnFamily);
        if (!summaries.containsKey(cf))
        {
            summaries.put(cf, new Table(cf.left, cf.right));
        }
        summaries.get(cf).consumeStats(result.stats);
    }

    public void consumeSessionResults(Optional<List<RepairSessionResult>> results)
    {
        if (results.isPresent())
        {
            filter(results.get(), Objects::nonNull).forEach(r -> filter(r.repairJobResults, Objects::nonNull).forEach(this::consumeRepairResult));
        }
    }

    public boolean isEmpty()
    {
        calculateTotals();
        return files == 0 && bytes == 0 && ranges.isEmpty();
    }

    private void calculateTotals()
    {
        files = 0;
        bytes = 0;
        ranges = new HashSet<>();
        summaries.values().forEach(Table::calculateTotals);
        for (Table table: summaries.values())
        {
            if (table.isCounter())
            {
                continue;
            }
            table.calculateTotals();
            files += table.files;
            bytes += table.bytes;
            ranges.addAll(table.ranges);
        }
        totalsCalculated = true;
    }

    public String toString()
    {
        List<Pair<String, String>> tables = Lists.newArrayList(summaries.keySet());
        tables.sort((o1, o2) ->
            {
                int ks = o1.left.compareTo(o2.left);
                return ks != 0 ? ks : o1.right.compareTo(o2.right);
            });

        calculateTotals();

        StringBuilder output = new StringBuilder();

        if (isEstimate)
        {
            output.append(String.format("Total estimated streaming: %s ranges, %s sstables, %s bytes\n", ranges.size(), files, FBUtilities.prettyPrintMemory(bytes)));
        }
        else
        {
            output.append(String.format("Total streaming: %s ranges, %s sstables, %s bytes\n", ranges.size(), files, FBUtilities.prettyPrintMemory(bytes)));
        }

        for (Pair<String, String> tableName: tables)
        {
            Table table = summaries.get(tableName);
            output.append(table.toString()).append('\n');
        }

        return output.toString();
    }
}
