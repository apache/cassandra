package org.apache.cassandra.test.microbench;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.PendingRangeMaps;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 50, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3,jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class PendingRangesBench
{
    PendingRangeMaps pendingRangeMaps;
    int maxToken = 256 * 100;

    Multimap<Range<Token>, InetAddress> oldPendingRanges;

    private Range<Token> genRange(String left, String right)
    {
        return new Range<Token>(new RandomPartitioner.BigIntegerToken(left), new RandomPartitioner.BigIntegerToken(right));
    }

    @Setup
    public void setUp() throws UnknownHostException
    {
        pendingRangeMaps = new PendingRangeMaps();
        oldPendingRanges = HashMultimap.create();

        InetAddress[] addresses = {InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.2")};

        for (int i = 0; i < maxToken; i++)
        {
            for (int j = 0; j < ThreadLocalRandom.current().nextInt(2); j ++)
            {
                Range<Token> range = genRange(Integer.toString(i * 10 + 5), Integer.toString(i * 10 + 15));
                pendingRangeMaps.addPendingRange(range, addresses[j]);
                oldPendingRanges.put(range, addresses[j]);
            }
        }

        // add the wrap around range
        for (int j = 0; j < ThreadLocalRandom.current().nextInt(2); j ++)
        {
            Range<Token> range = genRange(Integer.toString(maxToken * 10 + 5), Integer.toString(5));
            pendingRangeMaps.addPendingRange(range, addresses[j]);
            oldPendingRanges.put(range, addresses[j]);
        }
    }

    @Benchmark
    public void searchToken(final Blackhole bh)
    {
        int randomToken = ThreadLocalRandom.current().nextInt(maxToken * 10 + 5);
        Token searchToken = new RandomPartitioner.BigIntegerToken(Integer.toString(randomToken));
        bh.consume(pendingRangeMaps.pendingEndpointsFor(searchToken));
    }

    @Benchmark
    public void searchTokenForOldPendingRanges(final Blackhole bh)
    {
        int randomToken = ThreadLocalRandom.current().nextInt(maxToken * 10 + 5);
        Token searchToken = new RandomPartitioner.BigIntegerToken(Integer.toString(randomToken));
        Set<InetAddress> endpoints = new HashSet<>();
        for (Map.Entry<Range<Token>, Collection<InetAddress>> entry : oldPendingRanges.asMap().entrySet())
        {
            if (entry.getKey().contains(searchToken))
                endpoints.addAll(entry.getValue());
        }
        bh.consume(endpoints);
    }

}
