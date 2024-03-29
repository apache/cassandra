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

package org.apache.cassandra.index.accord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.FullKeyRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.RandomSource;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ReadSizeAbortException;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;
import org.apache.cassandra.utils.ObjectSizes;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * This test validates the system_accord.commands.route index to make sure it returns the right values... the way the
 * test is strucutred allows pluggability but runs with fixed configs while running in CI...
 *
 * If you are interested in testing different cases, the following tunables exist:
 *
 * <table>
 *     <li>size: how many read/writes should be done</li>
 *     <li>pattern: how ranges are layed out.  By default we test NO_OVERLAP so there are no conflicts</li>
 * </table>
 */
public class AccordIndexStressTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(AccordIndexStressTest.class);

    static
    {
        // The plan is to migrate away from SAI, so rather than hacking around timeout issues; just disable for now
        CassandraRelevantProperties.SAI_TEST_DISABLE_TIMEOUT.setBoolean(true);
    }
    private static final boolean VALIDATE = true;
    private static final boolean INCLUDE_FLUSH = true;
    private static final long SLOW_NS = TimeUnit.MILLISECONDS.toNanos(25);
    private static final Node.Id NODE = new Node.Id(42);
    private final Routable.Domain domain = Routable.Domain.Range;
    private final Int2ObjectHashMap<Map<TableId, Long2ObjectHashMap<List<TxnId>>>> storeToTableToRoutingKeysToTxns = new Int2ObjectHashMap<>();
    private final Int2ObjectHashMap<Map<TableId, Map<TokenRange, List<TxnId>>>> storeToTableToRangesToTxns = new Int2ObjectHashMap<>();
    private final RoutesSearcher searcher = new RoutesSearcher();

    enum Size
    {tiny, small, medium, large, benchmark}

    private final Size size = Size.small;

    private enum Read
    {INDEX, CQL}

    private Read read = Read.INDEX;

    private enum Pattern { RANDOM, NO_OVERLAP }
    private final Pattern pattern = Pattern.NO_OVERLAP;

    @Test
    public void test()
    {
        var tables = IntStream.range(0, 10)
                              .mapToObj(i -> TableId.fromUUID(new UUID(0, i)))
                              .collect(Collectors.toList());
        int numWrites, numReads;
        switch (size)
        {
            case tiny:
                numWrites = 100;
                numReads = 10;
                break;
            case small:
                numWrites = 100_000;
                numReads = 1_000;
                break;
            case medium:
                numWrites = 1_000_000;
                numReads = 1_000;
                break;
            case large:
                numWrites = 10_000_000;
                numReads = 1_000;
                break;
            case benchmark:
                numWrites = 1_000_000;
                numReads = numWrites * 10;
                break;
            default:
                throw new AssertionError("Unknown size: " + size);
        }
        var minToken = 0;
        var maxToken = (1 << 8) * numWrites;
        int numStores = 10;
        qt().withSeed(-1464527987857660885L).withExamples(1).check(rs -> {
            timed("write(" + numWrites + ")", () -> writeRecords(rs, numStores, tables, minToken, maxToken, numWrites));
            if (INCLUDE_FLUSH)
                timed("flush(writes=" + numWrites + ")", () -> FBUtilities.waitOnFutures(Keyspace.open("system_accord").flush(ColumnFamilyStore.FlushReason.UNIT_TESTS)));
            var warmupReads = Math.max(1, (int) (numReads * .2));
            timed("warmup read(" + warmupReads + ")", () -> readRecords(rs, warmupReads));
            timed("read(" + numReads + ")", () -> readRecords(rs, numReads));
        });
    }

    private static void timed(String name, Runnable fn)
    {
        logger.warn("Task {} starting...", name);
        long startNs = nanoTime();
        try
        {
            fn.run();
        }
        catch (Throwable t)
        {
            logger.warn("Task {} failed after {}ms", name, TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNs));
            throw t;
        }
        logger.warn("Task {} completed after {}ms", name, TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNs));
    }

    private static class RangeWrapper
    {
        final TokenRange[] ranges;
        final IntervalTree<RoutingKey, TxnId, Interval<RoutingKey, TxnId>> tree;

        RangeWrapper(TokenRange[] ranges, IntervalTree<RoutingKey, TxnId, Interval<RoutingKey, TxnId>> tree)
        {
            this.ranges = ranges;
            this.tree = tree;
        }
    }

    private Int2ObjectHashMap<Map<TableId, long[]>> store2Table2Tokens;
    private Int2ObjectHashMap<Map<TableId, RangeWrapper>> store2Table2Ranges;

    private void readRecords(RandomSource rs, int numRecords)
    {
        logger.warn("The bookkeeping is {} bytes", ObjectSizes.measureDeep(storeToTableToRoutingKeysToTxns));
        // sort the tokens
        if (domain == Routable.Domain.Key && store2Table2Tokens == null)
        {
            store2Table2Tokens = new Int2ObjectHashMap<>();
            timed("Model building: key", () -> storeToTableToRoutingKeysToTxns.forEachInt((storeId, actual) -> {
                Map<TableId, long[]> map = new HashMap<>();
                for (var e : actual.entrySet())
                {
                    var keys = e.getValue().keySet();
                    long[] tokens = new long[keys.size()];
                    var it = keys.iterator();
                    for (int i = 0; it.hasNext(); i++)
                        tokens[i] = it.nextLong();
                    Arrays.sort(tokens);
                    map.put(e.getKey(), tokens);
                }
                store2Table2Tokens.put(storeId, map);
            }));
            store2Table2Ranges = null;
        }
        else if (domain == Routable.Domain.Range && store2Table2Ranges == null)
        {
            store2Table2Ranges = new Int2ObjectHashMap<>();
            timed("Model building: range", () -> storeToTableToRangesToTxns.forEachInt((storeId, actual) -> {
                Map<TableId, RangeWrapper> map = new HashMap<>();
                for (var e : actual.entrySet())
                {
                    TableId tableId = e.getKey();
                    Map<TokenRange, List<TxnId>> range2Txns = e.getValue();
                    var keys = range2Txns.keySet();
                    TokenRange[] ranges = new TokenRange[keys.size()];
                    var it = keys.iterator();
                    var builder = new IntervalTree.Builder<RoutingKey, TxnId, Interval<RoutingKey, TxnId>>();
                    for (int i = 0; it.hasNext(); i++)
                    {
                        TokenRange r = ranges[i] = it.next();
                        List<TxnId> txns = range2Txns.get(r);
                        txns.forEach(txnId -> builder.add(new Interval<>(r.start(), r.end(), txnId)));
                    }
                    Arrays.sort(ranges, Range::compare);
                    map.put(tableId, new RangeWrapper(ranges, builder.build()));
                }
                store2Table2Ranges.put(storeId, map);
            }));
            store2Table2Tokens = null;
        }
        long[] samples = new long[numRecords];
        int[] counts = new int[numRecords];
        int size = 0;
        int numReadSizeAborts = 0;
        try
        {
            for (int i = 0; i < numRecords; i++)
            {
                int store;
                TableId table;
                Set<TxnId> expected = new HashSet<>();
                TokenKey start, end;
                switch (domain)
                {
                    case Key:
                    {
                        store = rs.pick(storeToTableToRoutingKeysToTxns.keySet());
                        var actual = this.storeToTableToRoutingKeysToTxns.get(store);
                        var tableToTokens = store2Table2Tokens.get(store);

                        table = rs.pick(actual.keySet());
                        var tokens = tableToTokens.get(table);

                        var offset = rs.nextInt(0, tokens.length);
                        var endOffset = offset == tokens.length - 1 ? tokens.length - 1 : offset + rs.nextInt(1, Math.min(3, tokens.length - offset));
                        IntStream.range(offset + 1, endOffset + 1).mapToLong(o -> tokens[o]).forEach(token -> expected.addAll(actual.get(table).get(token)));

                        start = new TokenKey(table, new Murmur3Partitioner.LongToken(tokens[offset]));
                        end = new TokenKey(table, new Murmur3Partitioner.LongToken(tokens[endOffset]));
                    }
                    break;
                    case Range:
                    {
                        store = rs.pick(storeToTableToRangesToTxns.keySet());
                        var tableToRangesToTxns = storeToTableToRangesToTxns.get(store);
                        var tableToRanges = store2Table2Ranges.get(store);

                        table = rs.pick(tableToRangesToTxns.keySet());
                        var wrapper = tableToRanges.get(table);
                        var ranges = wrapper.ranges;
                        var tree = wrapper.tree;
                        var range = rs.pick(ranges);
                        var a = tokenValue(range.start());
                        var b = tokenValue(range.end());
                        start = new TokenKey(table, new Murmur3Partitioner.LongToken(a + 1));
                        end = new TokenKey(table, new Murmur3Partitioner.LongToken(b - 1));
                        expected.addAll(tree.search(start, end));
                        assert !expected.isEmpty();
                    }
                    break;
                    default:
                        throw new IllegalArgumentException("Unknown domain: " + domain);
                }

                var startNs = nanoTime();
                Set<TxnId> actual = read(store, start, end);
                var durationNs = nanoTime() - startNs;
                samples[size] = durationNs;
                counts[size++] = actual.size();
                if (slow(durationNs))
                    logger.warn("Slow search: i={}, store={}, [{}, {}), results={}", i, store, start, end, actual.size());
                if (VALIDATE)
                {
                    try
                    {
                        Assertions.assertThat(actual).describedAs("[%s, %s)", start, end).isEqualTo(expected);
                    }
                    catch (Throwable t)
                    {
                        logSamples(samples, counts, size);
                        throw t;
                    }
                }
            }
        }
        finally
        {
            logger.info("Number of aborts due to size: {}", numReadSizeAborts);
            logSamples(samples, counts, size);
        }
    }

    private static long tokenValue(RoutingKey start)
    {
        return ((AccordRoutingKey.TokenKey) start).token().getLongValue();
    }

    private static boolean slow(long durationNs)
    {
        return durationNs >= SLOW_NS;
    }

    private Set<TxnId> read(int store, AccordRoutingKey start, AccordRoutingKey end)
    {
        switch (read)
        {
            case INDEX:
                return readIndex(store, start, end);
            case CQL:
                return readCQL(store, start, end);
            default:
                throw new AssertionError("Unknown read type: " + read);
        }
    }

    private Set<TxnId> readIndex(int store, AccordRoutingKey start, AccordRoutingKey end)
    {
        return searcher.intersects(store, start, end);
    }

    private Set<TxnId> readCQL(int store, AccordRoutingKey start, AccordRoutingKey end)
    {
        Set<TxnId> actual = new HashSet<>();
        try
        {
            UntypedResultSet results = execute("SELECT txn_id FROM system_accord.commands WHERE store_id = ? AND route > ? AND route <= ?", store, OrderedRouteSerializer.serializeRoutingKey(start), OrderedRouteSerializer.serializeRoutingKey(end));
            for (var row : results)
                actual.add(AccordKeyspace.deserializeTxnId(row));
        }
        catch (ReadSizeAbortException e)
        {
            // don't count it...
            logger.warn("Abort query to [{}, {}) do to size", start, end);
            return null;
        }
        return actual;
    }

    private void logSamples(long[] samples, int[] counts, int size)
    {
        if (size == 0)
        {
            logger.warn("No logs sampled");
            return;
        }
        Arrays.sort(samples, 0, size);
        Arrays.sort(counts, 0, size);
        StringBuilder sb = new StringBuilder();
        sb.append("Samples: ").append(size);
        sb.append("\nLatenciy:");
        sb.append("\n  Min (micro): ").append(TimeUnit.NANOSECONDS.toMicros(samples[0]));
        sb.append("\n  Max (micro): ").append(TimeUnit.NANOSECONDS.toMicros(samples[size - 1]));
        sb.append("\n  Median (micro): ").append(TimeUnit.NANOSECONDS.toMicros(samples[size / 2]));
        sb.append("\n  Avg (micro): ").append(TimeUnit.NANOSECONDS.toMicros((long) LongStream.of(samples).limit(size).average().getAsDouble()));
        sb.append("\nCounts:");
        sb.append("\n  Min: ").append(counts[0]);
        sb.append("\n  Max: ").append(counts[size - 1]);
        sb.append("\n  Median: ").append(counts[size / 2]);
        sb.append("\n  Avg: ").append((int) IntStream.of(counts).limit(size).average().getAsDouble());
        logger.info(sb.toString());
    }

    private void writeRecords(RandomSource rs,
                              int numStores,
                              List<TableId> tables,
                              int minToken, int maxToken,
                              int numRecords)
    {
        var cql = "INSERT INTO system_accord.commands (store_id, domain, txn_id, status, route, durability) VALUES (?, ?, ?, ?, ?, ?)";
        for (int i = 0; i < numRecords; i++)
        {
            int store = rs.nextInt(0, numStores);
            TxnId txnId = new TxnId(0, 1000 + i, Txn.Kind.Write, domain, NODE);
            int domain = txnId.domain().ordinal();
            int status = SaveStatus.PreCommitted.ordinal();
            ByteBuffer routeBB;
            try
            {
                Route<?> route = createRoute(rs, numRecords, i, rs.nextInt(1, 20), tables, minToken, maxToken);
                for (var u : route)
                {
                    switch (u.domain())
                    {
                        case Key:
                        {
                            AccordRoutingKey key = (AccordRoutingKey) u;
                            var table = key.table();
                            var token = key.token().getLongValue();
                            storeToTableToRoutingKeysToTxns.computeIfAbsent(store, ignore -> new HashMap<>())
                                                           .computeIfAbsent(table, ignore -> new Long2ObjectHashMap<>())
                                                           .computeIfAbsent(token, ignore -> new ArrayList<>())
                                                           .add(txnId);
                        }
                        break;
                        case Range:
                        {
                            TokenRange range = (TokenRange) u;
                            var table = range.table();
                            storeToTableToRangesToTxns.computeIfAbsent(store, ignore -> new HashMap<>())
                                                      .computeIfAbsent(table, ignore -> new HashMap<>())
                                                      .computeIfAbsent(range, ignore -> new ArrayList<>())
                                                      .add(txnId);
                        }
                        break;
                        default:
                            throw new AssertionError("Unexpected domain: " + u.domain());
                    }
                }
                routeBB = AccordKeyspace.serializeRoute(route);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            int durability = Status.Durability.NotDurable.ordinal();

            execute(cql, store, domain, AccordKeyspace.serializeTimestamp(txnId), status, routeBB, durability);
        }
    }

    private Route<?> createRoute(RandomSource rs, int numRecords, int index, int numKeys, List<TableId> tables, int minToken, int maxToken)
    {
        switch (domain)
        {
            case Key:
            {
                TreeSet<AccordRoutingKey> keys = new TreeSet<>();
                while (keys.size() < numKeys)
                {
                    var table = rs.pick(tables);
                    var token = new Murmur3Partitioner.LongToken(rs.nextInt(minToken, maxToken));
                    keys.add(new TokenKey(table, token));
                }
                return new FullKeyRoute(keys.first(), true, keys.toArray(RoutingKey[]::new));
            }
            case Range:
            {
                TreeSet<TokenRange> ranges = new TreeSet<>(Range::compareTo);
                RoutingKey routingKey = null;
                var domain = maxToken - minToken + 1;
                var delta = domain / numRecords;
                var sub_delta = delta / numKeys;
                while (ranges.size() < numKeys)
                {
                    var table = rs.pick(tables);
                    int a, b;
                    switch (pattern)
                    {
                        case RANDOM:
                        {
                            a = rs.nextInt(minToken, maxToken);
                            b = rs.nextInt(minToken, maxToken);
                            while (a == b)
                                b = rs.nextInt(minToken, maxToken);
                            if (a > b)
                            {
                                var tmp = a;
                                a = b;
                                b = tmp;
                            }
                        }
                        break;
                        case NO_OVERLAP:
                        {
                            a = delta * index + (sub_delta * ranges.size());
                            b = a + sub_delta;
                        }
                        break;
                        default:
                            throw new IllegalArgumentException("Unknown pattern: " + pattern);
                    }
                    ranges.add(new TokenRange(new TokenKey(table, new Murmur3Partitioner.LongToken(a)), new TokenKey(table, new Murmur3Partitioner.LongToken(b))));
                    if (routingKey == null)
                    {
                        routingKey = new TokenKey(table, new Murmur3Partitioner.LongToken(b));
                    }
                }
                return Ranges.ofSorted(ranges.toArray(Range[]::new)).toRoute(routingKey);
            }
            default:
                throw new IllegalArgumentException("Unknown domain: " + domain);
        }
    }
}
