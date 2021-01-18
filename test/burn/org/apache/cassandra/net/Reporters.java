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

package org.apache.cassandra.net;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

class Reporters
{
    final Collection<InetAddressAndPort> endpoints;
    final Connection[] connections;
    final List<Reporter> reporters;
    final long start = nanoTime();

    Reporters(Collection<InetAddressAndPort> endpoints, Connection[] connections)
    {
        this.endpoints = endpoints;
        this.connections = connections;
        this.reporters = ImmutableList.of(
            outboundReporter (true, "Outbound Throughput", OutboundConnection::sentBytes, Reporters::prettyPrintMemory),
            inboundReporter  (true, "Inbound Throughput", InboundCounters::processedBytes, Reporters::prettyPrintMemory),

            outboundReporter (false, "Outbound Pending Bytes",       OutboundConnection::pendingBytes,       Reporters::prettyPrintMemory),
            reporter         (false, "Inbound Pending Bytes",        c -> c.inbound.usingCapacity(), Reporters::prettyPrintMemory),

            outboundReporter (true,  "Outbound Expirations",         OutboundConnection::expiredCount,       Long::toString),
            inboundReporter  (true,  "Inbound Expirations",          InboundCounters::expiredCount,          Long::toString),

            outboundReporter (true,  "Outbound Errors",              OutboundConnection::errorCount,         Long::toString),
            inboundReporter  (true,  "Inbound Errors",               InboundCounters::errorCount,            Long::toString),

            outboundReporter (true,  "Outbound Connection Attempts", OutboundConnection::connectionAttempts, Long::toString)
        );
    }

    void update()
    {
        for (Reporter reporter : reporters)
            reporter.update();
    }

    void print()
    {
        System.out.println("==" + prettyPrintElapsed(nanoTime() - start) + "==\n");

        for (Reporter reporter : reporters)
        {
            reporter.print();
        }
    }

    private Reporter outboundReporter(boolean accumulates, String name, ToLongFunction<OutboundConnection> get, LongFunction<String> printer)
    {
        return new Reporter(accumulates, name, (conn) -> get.applyAsLong(conn.outbound), printer);
    }

    private Reporter inboundReporter(boolean accumulates, String name, ToLongFunction<InboundCounters> get, LongFunction<String> printer)
    {
        return new Reporter(accumulates, name, (conn) -> get.applyAsLong(conn.inboundCounters()), printer);
    }

    private Reporter reporter(boolean accumulates, String name, ToLongFunction<Connection> get, LongFunction<String> printer)
    {
        return new Reporter(accumulates, name, get, printer);
    }

    class Reporter
    {
        boolean accumulates;
        final String name;
        final ToLongFunction<Connection> get;
        final LongFunction<String> print;
        final long[][] previousValue;
        final long[] columnTotals = new long[1 + endpoints.size() * 3];
        final Table table;

        Reporter(boolean accumulates, String name, ToLongFunction<Connection> get, LongFunction<String> print)
        {
            this.accumulates = accumulates;
            this.name = name;
            this.get = get;
            this.print = print;

            previousValue = accumulates ? new long[endpoints.size()][endpoints.size() * 3] : null;

            String[] rowNames = new String[endpoints.size() + 1];
            for (int row = 0 ; row < endpoints.size() ; ++row)
            {
                rowNames[row] = Integer.toString(1 + row);
            }
            rowNames[rowNames.length - 1] = "Total";

            String[] columnNames = new String[endpoints.size() * 3 + 1];
            for (int column = 0 ; column < endpoints.size() * 3 ; column += 3)
            {
                String endpoint = Integer.toString(1 + column / 3);
                columnNames[    column] = endpoint + ".Urgent";
                columnNames[1 + column] = endpoint + ".Small";
                columnNames[2 + column] = endpoint + ".Large";
            }
            columnNames[columnNames.length - 1] = "Total";

            table = new Table(rowNames, columnNames, "Recipient");
        }

        public void update()
        {
            Arrays.fill(columnTotals, 0);
            int row = 0, connection = 0;
            for (InetAddressAndPort recipient : endpoints)
            {
                int column = 0;
                long rowTotal = 0;
                for (InetAddressAndPort sender : endpoints)
                {
                    for (ConnectionType type : ConnectionType.MESSAGING_TYPES)
                    {
                        assert recipient.equals(connections[connection].recipient);
                        assert sender.equals(connections[connection].sender);
                        assert type == connections[connection].outbound.type();

                        long cur = get.applyAsLong(connections[connection]);
                        long value;
                        if (accumulates)
                        {
                            long prev = previousValue[row][column];
                            previousValue[row][column] = cur;
                            value = cur - prev;
                        }
                        else
                        {
                            value = cur;
                        }
                        table.set(row, column, print.apply(value));
                        columnTotals[column] += value;
                        rowTotal += value;
                        ++column;
                        ++connection;
                    }
                }
                columnTotals[column] += rowTotal;
                table.set(row, column, print.apply(rowTotal));
                table.displayRow(row, rowTotal > 0);
                ++row;
            }

            boolean displayTotalRow = false;
            for (int column = 0 ; column < columnTotals.length ; ++column)
            {
                table.set(endpoints.size(), column, print.apply(columnTotals[column]));
                table.displayColumn(column, columnTotals[column] > 0);
                displayTotalRow |= columnTotals[column] > 0;
            }
            table.displayRow(endpoints.size(), displayTotalRow);
        }

        public void print()
        {
            table.print("===" + name + "===");
        }
    }

    private static class Table
    {
        final String[][] print;
        final int[] width;
        final BitSet rowMask = new BitSet();
        final BitSet columnMask = new BitSet();

        public Table(String[] rowNames, String[] columnNames, String rowNameHeader)
        {
            print = new String[rowNames.length + 1][columnNames.length + 1];
            width = new int[columnNames.length + 1];
            print[0][0] = rowNameHeader;
            for (int i = 0 ; i < columnNames.length ; ++i)
                print[0][1 + i] = columnNames[i];
            for (int i = 0 ; i < rowNames.length ; ++i)
                print[1 + i][0] = rowNames[i];
        }

        void set(int row, int column, String value)
        {
            print[row + 1][column + 1] = value;
        }

        void displayRow(int row, boolean display)
        {
            rowMask.set(row, display);
        }

        void displayColumn(int column, boolean display)
        {
            columnMask.set(column, display);
        }

        void print(String heading)
        {
            if (rowMask.isEmpty() && columnMask.isEmpty())
                return;

            System.out.println(heading + '\n');

            Arrays.fill(width, 0);
            for (int row = 0 ; row < print.length ; ++row)
            {
                for (int column = 0 ; column < width.length ; ++column)
                {
                    width[column] = Math.max(width[column], print[row][column].length());
                }
            }

            for (int row = 0 ; row < print.length ; ++row)
            {
//                if (row > 0 && !rowMask.get(row - 1))
//                    continue;

                StringBuilder builder = new StringBuilder();
                for (int column = 0 ; column < width.length ; ++column)
                {
//                    if (column > 0 && !columnMask.get(column - 1))
//                        continue;

                    String s = print[row][column];
                    int pad = width[column] - s.length();
                    for (int i = 0 ; i < pad ; ++i)
                        builder.append(' ');
                    builder.append(s);
                    builder.append("  ");
                }
                System.out.println(builder.toString());
            }
            System.out.println();
        }
    }

    private static final class OneTimeUnit
    {
        final TimeUnit unit;
        final String symbol;
        final long nanos;

        private OneTimeUnit(TimeUnit unit, String symbol)
        {
            this.unit = unit;
            this.symbol = symbol;
            this.nanos = unit.toNanos(1L);
        }
    }

    private static final List<OneTimeUnit> prettyPrintElapsed = ImmutableList.of(
        new OneTimeUnit(TimeUnit.DAYS, "d"),
        new OneTimeUnit(TimeUnit.HOURS, "h"),
        new OneTimeUnit(TimeUnit.MINUTES, "m"),
        new OneTimeUnit(TimeUnit.SECONDS, "s"),
        new OneTimeUnit(TimeUnit.MILLISECONDS, "ms"),
        new OneTimeUnit(TimeUnit.MICROSECONDS, "us"),
        new OneTimeUnit(TimeUnit.NANOSECONDS, "ns")
    );

    private static String prettyPrintElapsed(long nanos)
    {
        if (nanos == 0)
            return "0ns";

        int count = 0;
        StringBuilder builder = new StringBuilder();
        for (OneTimeUnit unit : prettyPrintElapsed)
        {
            if (count == 2)
                break;

            if (nanos >= unit.nanos)
            {
                if (count > 0)
                    builder.append(' ');
                long inUnit = unit.unit.convert(nanos, TimeUnit.NANOSECONDS);
                nanos -= unit.unit.toNanos(inUnit);
                builder.append(inUnit);
                builder.append(unit.symbol);
                ++count;
            } else if (count > 0)
                ++count;
        }

        return builder.toString();
    }

    static String prettyPrintMemory(long size)
    {
        if (size >= 1000 * 1000 * 1000)
            return String.format("%.0fG", size / (double) (1 << 30));
        if (size >= 1000 * 1000)
            return String.format("%.0fM", size / (double) (1 << 20));
        return String.format("%.0fK", size / (double) (1 << 10));
    }
}

