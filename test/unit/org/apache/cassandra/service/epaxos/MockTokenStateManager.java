package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.util.Set;
import java.util.UUID;


public class MockTokenStateManager extends TokenStateManager
{
    public static final Token TOKEN0 = AbstractEpaxosTest.token(0);
    public static final Token TOKEN100 = AbstractEpaxosTest.token(100);

    private final Set<Range<Token>> ranges = Sets.newHashSet(new Range<>(TOKEN0, TOKEN0));

    public MockTokenStateManager(Scope scope)
    {
        super(scope);
        start();
    }

    public MockTokenStateManager(String keyspace, String table, Scope scope)
    {
        super(keyspace, table, scope);
        start();
    }

    public void setTokens(Token t1, Token t2, Token... tokens)
    {
        this.ranges.clear();
        ranges.add(new Range<>(t1, t2));

        Token last = t2;
        for (Token t: tokens)
        {
            ranges.add(new Range<>(last, t));
            last = t;
        }
    }

    @Override
    protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
    {
        return Sets.newHashSet(ranges);
    }

    public int epochIncrementThreshold = 100;

    @Override
    public int getEpochIncrementThreshold(UUID cfId)
    {
        return epochIncrementThreshold;
    }
}
